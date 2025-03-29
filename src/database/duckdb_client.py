import os
from typing import Optional
import pandas as pd
import duckdb

from src.utils.logging import get_logger
from src.schemas.duckdb_schema import TableSchema, ViewSchema, AVIAN_CASES_SCHEMA, SURVEILLANCE_SCHEMA, CASES_BY_REGION_VIEW, SURVEILLANCE_SUMMARY_VIEW

logger = get_logger(__name__)

class DuckDBClient:
    """Cliente para interação com DuckDB para análise de dados."""

    def __init__(
        self,
        database_path: Optional[str] = None,
        minio_endpoint: Optional[str] = None,
        minio_access_key: Optional[str] = None,
        minio_secret_key: Optional[str] = None,
        read_only: bool = False
    ):
        """
        Inicializa o cliente DuckDB.

        Args:
            database_path: Caminho para o arquivo de banco de dados DuckDB
                           Se None, usa um banco em memória
            minio_endpoint: Endpoint do MinIO (ex: "minio:9000")
            minio_access_key: Chave de acesso MinIO
            minio_secret_key: Chave secreta MinIO
            read_only: Se True, abre o banco em modo somente leitura
        """
        self.database_path = database_path or ":memory:"
        self.read_only = read_only

        self.minio_endpoint = minio_endpoint or os.getenv("MINIO_ENDPOINT", "minio:9000")
        self.minio_access_key = minio_access_key or os.getenv("MINIO_ACCESS_KEY", "minioadmin")
        self.minio_secret_key = minio_secret_key or os.getenv("MINIO_SECRET_KEY", "minioadmin")

        self.conn = duckdb.connect(self.database_path, read_only=self.read_only)
        self._configure_s3()

        logger.info(f"Cliente DuckDB inicializado com banco: {self.database_path}")

    def _configure_s3(self):
        """Configura acesso ao MinIO/S3."""
        try:
            self.conn.execute(f"""
                SET s3_endpoint='{self.minio_endpoint}';
                SET s3_access_key_id='{self.minio_access_key}';
                SET s3_secret_access_key='{self.minio_secret_key}';
                SET s3_use_ssl=false;
            """)
            logger.info(f"Conexão S3 configurada para endpoint: {self.minio_endpoint}")
        except Exception as e:
            logger.error(f"Erro ao configurar conexão S3: {str(e)}")
            raise

    def create_table(self, schema: TableSchema) -> None:
        """
        Cria uma tabela no DuckDB.

        Args:
            schema: Esquema da tabela a ser criada
        """
        try:
            sql = schema.get_create_table_sql()
            self.conn.execute(sql)
            logger.info(f"Tabela criada: {schema.name}")
        except Exception as e:
            logger.error(f"Erro ao criar tabela {schema.name}: {str(e)}")
            raise

    def create_view(self, schema: ViewSchema) -> None:
        """
        Cria uma view no DuckDB.

        Args:
            schema: Esquema da view a ser criada
        """
        try:
            sql = schema.get_create_view_sql()
            self.conn.execute(sql)
            logger.info(f"View criada: {schema.name}")
        except Exception as e:
            logger.error(f"Erro ao criar view {schema.name}: {str(e)}")
            raise

    def initialize_schema(self) -> None:
        """Inicializa o esquema com tabelas e views pré-definidas."""
        self.create_table(AVIAN_CASES_SCHEMA)
        self.create_table(SURVEILLANCE_SCHEMA)
        # As views dependem das tabelas, então criamos depois de ter certeza que as tabelas existem
        self.create_view(CASES_BY_REGION_VIEW)
        self.create_view(SURVEILLANCE_SUMMARY_VIEW)
        logger.info("Esquema inicializado com tabelas e views padrão")

    def query_parquet_from_minio(self, bucket: str, prefix: str) -> pd.DataFrame:
        """
        Consulta arquivos Parquet diretamente do MinIO.

        Args:
            bucket: Nome do bucket
            prefix: Prefixo para filtrar arquivos

        Returns:
            DataFrame com os resultados
        """
        try:
            # Construir caminho S3
            s3_path = f"s3://{bucket}/{prefix}/*.parquet"

            # Executar consulta
            result = self.conn.execute(f"SELECT * FROM read_parquet('{s3_path}')").fetchdf()
            logger.info(f"Consulta executada com sucesso: {s3_path}")
            return result
        except Exception as e:
            logger.error(f"Erro ao consultar Parquet do MinIO: {str(e)}")
            raise

    def execute_query(self, query: str) -> pd.DataFrame:
        """
        Executa uma consulta SQL.

        Args:
            query: Consulta SQL

        Returns:
            DataFrame com os resultados
        """
        try:
            result = self.conn.execute(query).fetchdf()
            return result
        except Exception as e:
            logger.error(f"Erro ao executar consulta: {str(e)}")
            raise

    def load_avian_cases_from_minio(self, force_reload: bool = False) -> int:
        """
        Carrega dados de casos de H5N1 em aves do MinIO para tabela DuckDB.

        Args:
            force_reload: Se True, limpa a tabela antes de carregar

        Returns:
            Número de registros importados
        """
        try:
            if force_reload:
                self.conn.execute(f"DELETE FROM {AVIAN_CASES_SCHEMA.name}")

            count_before = self.conn.execute(
                f"SELECT COUNT(*) FROM {AVIAN_CASES_SCHEMA.name}"
            ).fetchone()[0]

            s3_path = "s3://raw/avian_cases/*.parquet"

            self.conn.execute(f"""
                INSERT INTO {AVIAN_CASES_SCHEMA.name}
                SELECT
                    uuid() as id,  -- Gera UUID para cada registro
                    source_id,
                    source,
                    collected_at,
                    location_info->>'location' as location,
                    CAST(location_info->>'latitude' AS DOUBLE) as latitude,
                    CAST(location_info->>'longitude' AS DOUBLE) as longitude,
                    case_data->>'bird_species' as bird_species,
                    case_data->>'bird_type' as bird_type,
                    case_data->>'h5_subtype' as h5_subtype,
                    case_data->>'detection_date' as detection_date,
                    case_data->>'test_method' as test_method,
                    case_data,
                    metadata
                FROM read_parquet('{s3_path}')
            """)

            # Contar registros depois
            count_after = self.conn.execute(
                f"SELECT COUNT(*) FROM {AVIAN_CASES_SCHEMA.name}"
            ).fetchone()[0]

            records_imported = count_after - count_before
            logger.info(f"Importados {records_imported} registros de casos em aves")
            return records_imported

        except Exception as e:
            logger.error(f"Erro ao importar dados de casos: {str(e)}")
            raise

    def load_surveillance_from_minio(self, force_reload: bool = False) -> int:
        """
        Carrega dados de vigilância do MinIO para tabela DuckDB.

        Args:
            force_reload: Se True, limpa a tabela antes de carregar

        Returns:
            Número de registros importados
        """
        try:
            if force_reload:
                self.conn.execute(f"DELETE FROM {SURVEILLANCE_SCHEMA.name}")

            count_before = self.conn.execute(
                f"SELECT COUNT(*) FROM {SURVEILLANCE_SCHEMA.name}"
            ).fetchone()[0]

            s3_path = "s3://raw/surveillance/*.parquet"

            self.conn.execute(f"""
                INSERT INTO {SURVEILLANCE_SCHEMA.name}
                SELECT
                    uuid() as id,
                    source_id,
                    source,
                    collected_at,
                    location_info->>'location' as location,
                    CAST(location_info->>'latitude' AS DOUBLE) as latitude,
                    CAST(location_info->>'longitude' AS DOUBLE) as longitude,
                    surveillance_data->>'sample_type' as sample_type,
                    surveillance_data->>'result' as result,
                    surveillance_data->>'bird_species' as bird_species,
                    surveillance_data->>'bird_type' as bird_type,
                    surveillance_data,
                    metadata
                FROM read_parquet('{s3_path}')
            """)

            count_after = self.conn.execute(
                f"SELECT COUNT(*) FROM {SURVEILLANCE_SCHEMA.name}"
            ).fetchone()[0]

            records_imported = count_after - count_before
            logger.info(f"Importados {records_imported} registros de vigilância")
            return records_imported

        except Exception as e:
            logger.error(f"Erro ao importar dados de vigilância: {str(e)}")
            raise

    def close(self):
        """Fecha a conexão com o banco de dados."""
        if self.conn:
            self.conn.close()
            logger.info("Conexão com DuckDB fechada")