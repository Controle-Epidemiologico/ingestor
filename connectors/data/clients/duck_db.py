from datetime import datetime
from typing import Optional, Any

import duckdb
import pandas as pd

from configs.logger import get_logger
from configs.settings import DuckDB as DuckDBConfig, MinIO as MinIOConfig
from schemas.database import TableName, TableSchema, ViewSchema

logger = get_logger(__name__)


class DuckDBClient:
    """
    Cliente para operações de staging e processamento de dados usando DuckDB.
    """

    def __init__(
            self,
            database_path: Optional[str] = None,
            minio_config: Optional[MinIOConfig] = None,
            read_only: bool = False,
    ):
        """Inicializa o cliente DuckDB."""
        self.database_path = database_path or DuckDBConfig().path
        self.minio_config = minio_config or MinIOConfig()
        self.read_only = read_only

        self.conn = duckdb.connect(self.database_path, read_only=self.read_only)
        self._configure_s3()

        logger.info(
            f"Cliente DuckDB inicializado: {self.database_path} (read_only={self.read_only})"
        )

    def _configure_s3(self):
        """Configura acesso ao MinIO/S3 para leitura direta de arquivos."""
        try:
            self.conn.execute(
                f"""
                SET s3_endpoint='{self.minio_config.endpoint}';
                SET s3_access_key_id='{self.minio_config.access_key}';
                SET s3_secret_access_key='{self.minio_config.secret_key}';
                SET s3_region='auto';
                SET s3_use_ssl={str(self.minio_config.secure).lower()};
                SET s3_url_style='path';
            """
            )
            logger.info("Configuração S3/MinIO aplicada")
        except Exception as e:
            logger.error(f"Erro ao configurar acesso S3: {str(e)}")
            raise

    def initialize_schema(
            self,
            tables: list[TableSchema],
            views: list[ViewSchema],
    ) -> None:
        """Inicializa o esquema do banco criando tabelas e views."""
        if self.read_only:
            logger.warning("Banco somente leitura. Não é possível inicializar esquema.")
            return

        try:
            for table in tables:
                self.conn.execute(table.get_create_table_sql())

            for view in views:
                self.conn.execute(view.get_create_view_sql())

            logger.info(
                f"Esquema inicializado: {len(tables)} tabelas, {len(views)} views"
            )
        except Exception as e:
            logger.error(f"Erro ao inicializar esquema: {str(e)}")
            raise

    def get_table_columns(
            self,
            table_name: str | TableName,
    ) -> list[str]:
        """Retorna a lista de colunas de uma tabela."""
        table = table_name.value if hasattr(table_name, "value") else table_name

        try:
            columns_df = self.conn.execute(f"PRAGMA table_info({table})").fetchdf()
            return columns_df["name"].tolist()
        except Exception as e:
            logger.error(f"Erro ao obter colunas da tabela {table}: {str(e)}")
            raise

    def _get_primary_key(
            self,
            table_name: str | TableName,
    ) -> Optional[str]:
        """Retorna o nome da coluna de chave primária se existir."""
        table = table_name.value if hasattr(table_name, "value") else table_name

        try:
            result = self.conn.execute(f"PRAGMA table_info({table})").fetchdf()
            pk_col = result[result["pk"] > 0]
            return pk_col["name"].iloc[0] if len(pk_col) > 0 else None
        except Exception as e:
            logger.error(f"Erro ao obter chave primária de {table}: {str(e)}")
            return None

    def insert_dataframe(
            self,
            table_name: str | TableName,
            avoid_duplicates: bool = True,
    ) -> int:
        """Insere DataFrame em uma tabela, garantindo compatibilidade de esquema.

        Args:
            table_name: Nome da tabela de destino
            df: DataFrame a ser inserido
            avoid_duplicates: Se True, evita inserção de registros duplicados

        Returns:
            Número de registros inseridos
        """
        if self.read_only:
            logger.warning("Banco somente leitura. Não é possível inserir dados.")
            return 0

        table = table_name.value if hasattr(table_name, "value") else table_name

        try:
            temp_table = f"temp_insert_{datetime.now().strftime('%Y%m%d%H%M%S')}"
            self.conn.execute(
                f"CREATE TEMPORARY TABLE {temp_table} AS SELECT * FROM df"
            )

            count_before = self.conn.execute(
                f"SELECT COUNT(*) FROM {table}"
            ).fetchone()[0]

            # TODO: Refatorar essa barbaridade
            if avoid_duplicates and (primary_key := self._get_primary_key(table_name)):
                self.conn.execute(
                    f"""
                    INSERT INTO {table} ({', '.join(self.get_table_columns(table))})
                    SELECT {', '.join(self.get_table_columns(table))} FROM {temp_table} t
                    LEFT JOIN {table} e ON t.{primary_key} = e.{primary_key}
                    WHERE e.{primary_key} IS NULL
                """
                )
            else:
                self.conn.execute(
                    f"""
                    INSERT INTO {table} ({', '.join(self.get_table_columns(table))})
                    SELECT {', '.join(self.get_table_columns(table))} FROM {temp_table}
                """
                )

            self.conn.execute(f"DROP TABLE {temp_table}")

            count_after = self.conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[
                0
            ]
            records_added = count_after - count_before

            logger.info(f"Inseridos {records_added} registros na tabela {table}")
            return records_added
        except Exception as e:
            logger.error(f"Erro ao inserir DataFrame em {table}: {str(e)}")
            raise

    def query_to_dataframe(
            self,
            query_or_table: str | TableName,
            filters: dict[str, Any] = None,
    ) -> pd.DataFrame:
        """Recupera dados via query SQL ou tabela com filtros.

        Args:
            query_or_table: Query SQL completa ou nome da tabela
            filters: Filtros a serem aplicados (no caso de ser tabela)

        Returns:
            DataFrame com resultados
        """
        try:
            if hasattr(query_or_table, "value"):
                query_or_table = query_or_table.value

            is_table = " " not in query_or_table and "(" not in query_or_table

            if is_table:
                query = f"SELECT * FROM {query_or_table}"

                if filters:
                    conditions = []
                    for field, value in filters.items():
                        if isinstance(value, str):
                            # Escapa aspas simples em strings
                            value = value.replace("'", "''")
                            conditions.append(f"{field} = '{value}'")
                        else:
                            conditions.append(f"{field} = {value}")

                    if conditions:
                        query += " WHERE " + " AND ".join(conditions)
            else:
                query = query_or_table

            return self.execute_query(query)

        except Exception as e:
            logger.error(f"Erro ao recuperar dados: {str(e)}")
            raise

    def execute_query(self, query: str) -> pd.DataFrame:
        """Executa uma consulta SQL e retorna DataFrame."""
        try:
            result = self.conn.execute(query).fetchdf()
            logger.debug(f"Consulta executada: {query[:100]}...")
            return result
        except Exception as e:
            logger.error(f"Erro ao executar consulta: {str(e)}")
            raise

    def read_external_parquet(
            self,
            uris: list[str],
            target_table: Optional[TableName] = None,
    ) -> pd.DataFrame:
        """Lê dados de múltiplos arquivos Parquet via URIs S3/MinIO.

        Args:
            uris: lista de URIs S3 para arquivos Parquet
            target_table: Tabela alvo (opcional) para padronização de colunas

        Returns:
            DataFrame com dados lidos e padronizados se necessário
        """
        if not uris:
            logger.warning("Nenhuma URI fornecida para leitura")
            return pd.DataFrame()

        try:
            uris_str = ", ".join([f"'{uri}'" for uri in uris])
            df = self.conn.execute(
                f"SELECT * FROM read_parquet([{uris_str}])"
            ).fetchdf()

            logger.info(f"Lidos {len(df)} registros de {len(uris)} arquivos Parquet")

            if target_table is not None:
                target_columns = self.get_table_columns(target_table)

                # Aqui delegamos a transformação para o DataTransformer
                # Isso será importado diretamente pelo DataOrchestrator
                # para evitar dependência circular
                return (
                    df[target_columns]
                    if set(target_columns).issubset(set(df.columns))
                    else df
                )

            return df
        except Exception as e:
            logger.error(f"Erro ao ler arquivos Parquet: {str(e)}")
            raise

    def create_temporary_table(
            self,
            table_name: str,
            df: pd.DataFrame,
    ) -> None:
        """Cria uma tabela temporária a partir de um DataFrame."""
        try:
            self.conn.execute(
                f"CREATE OR REPLACE TEMPORARY TABLE {table_name} AS SELECT * FROM df"
            )
            logger.info(f"Tabela temporária criada: {table_name} ({len(df)} registros)")
        except Exception as e:
            logger.error(f"Erro ao criar tabela temporária: {str(e)}")
            raise

    def execute_script(self, script: str) -> None:
        """Executa múltiplas instruções SQL sem retorno."""
        try:
            self.conn.execute(script)
            logger.debug(f"Script SQL executado: {script[:100]}...")
        except Exception as e:
            logger.error(f"Erro ao executar script SQL: {str(e)}")
            raise

    def export_to_dataframe(
            self,
            query_or_table: str | TableName,
    ) -> pd.DataFrame:
        """Exporta dados para DataFrame."""
        return self.query_to_dataframe(query_or_table)

    def close(self):
        """Fecha a conexão com o banco de dados."""
        if self.conn:
            self.conn.close()
            logger.info("Conexão com DuckDB fechada")
