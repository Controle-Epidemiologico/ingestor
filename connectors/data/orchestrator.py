import uuid
from datetime import datetime
from typing import Any, Callable, Optional, Type

import pandas as pd
from pydantic import BaseModel

from configs.logger import get_logger
from connectors.data.clients.duck_db import DuckDBClient
from connectors.data.clients.minio import MinioClient
from connectors.data.transformer import DataTransformer
from schemas.database import TableName

logger = get_logger(__name__)


class DataOrchestrator:
    """
    Orquestra fluxos de dados entre componentes do sistema.
    """

    def __init__(
        self,
        db_client: Optional[DuckDBClient] = None,
        storage_client: Optional[MinioClient] = None,
    ):
        """Inicializa o orquestrador com clientes de banco e armazenamento."""
        self.db = db_client or DuckDBClient()
        self.storage = storage_client or MinioClient()
        self.transformer = DataTransformer()
        logger.info("DataOrchestrator inicializado")

    def ingest_models_to_database(
        self,
        models: list[BaseModel],
        table_name: str | TableName,
    ) -> int:
        """
        Ingere modelos Pydantic diretamente no banco de dados.

        Args:
            models: lista de modelos Pydantic
            table_name: Nome da tabela destino

        Returns:
            Número de registros inseridos
        """
        if not models:
            logger.warning(f"Nenhum modelo para ingerir na tabela {table_name}")
            return 0

        df = DataTransformer.model_to_dataframe(models)

        num_inserted = self.db.insert_dataframe(table_name, df)
        logger.info(f"Inseridos {num_inserted} registros na tabela {table_name}")

        return num_inserted

    def load_storage_to_database(
        self,
        bucket: str,
        prefix: str,
        table_name: str | TableName,
    ) -> int:
        """
        Carrega dados do armazenamento para o banco de dados.

        Args:
            bucket: Nome do bucket
            prefix: Prefixo dos objetos a serem carregados
            table_name: Nome da tabela destino

        Returns:
            Número de registros carregados
        """
        parquet_uris = self.storage.get_parquet_uris(bucket, prefix)

        if not parquet_uris:
            logger.warning(f"Nenhum arquivo Parquet encontrado em {bucket}/{prefix}")
            return 0

        df = self.db.read_external_parquet(parquet_uris, table_name)

        if not df.empty:
            num_inserted = self.db.insert_dataframe(table_name, df)
            logger.info(
                f"Carregados {num_inserted} registros do storage para {table_name}"
            )
            return num_inserted

        return 0

    def export_database_to_storage(
        self,
        table_name: str | TableName,
        bucket: str,
        path_prefix: str,
        partition_cols: list[str] = None,
    ) -> str:
        """
        Exporta dados do banco para o armazenamento em formato Parquet.

        Args:
            table_name: Nome da tabela origem
            bucket: Nome do bucket destino
            path_prefix: Prefixo do caminho no storage
            partition_cols: Colunas para particionamento (opcional)

        Returns:
            URI do arquivo exportado
        """
        df = self.db.query_to_dataframe(table_name)

        if df.empty:
            logger.warning(f"Tabela {table_name} vazia, nada para exportar")
            return ""

        uri = self.storage.upload_parquet(bucket, path_prefix, df, partition_cols)
        logger.info(f"Exportados {len(df)} registros para {uri}")

        return uri

    def transform_and_load(
        self,
        source_uris: list[str],
        target_table: str | TableName,
        transformations: list[Callable[[pd.DataFrame], pd.DataFrame]] = None,
    ) -> int:
        """
        Aplica transformações em dados do storage e carrega no banco.

        Args:
            source_uris: URIs dos arquivos fonte no storage
            target_table: Tabela destino no banco
            transformations: lista de funções de transformação

        Returns:
            Número de registros processados
        """
        if not source_uris:
            logger.warning("Nenhuma URI de origem fornecida")
            return 0

        df = self.db.read_external_parquet(source_uris)

        if df.empty:
            logger.warning("Nenhum dado carregado das URIs fornecidas")
            return 0

        if transformations:
            for transform_fn in transformations:
                df = transform_fn(df)
                logger.debug(f"Transformação aplicada: {transform_fn.__name__}")

        num_inserted = self.db.insert_dataframe(target_table, df)
        logger.info(
            f"Processados e inseridos {num_inserted} registros na tabela {target_table}"
        )

        return num_inserted

    def archive_and_process(
        self,
        data_source: list[BaseModel] | pd.DataFrame,
        process_name: str,
        target_table: str | TableName,
        archive_bucket: str,
        archive_prefix: str,
    ) -> dict[str, Any]:
        """
        Arquiva dados no storage e processa no banco numa única operação.

        Args:
            data_source: lista de modelos ou DataFrame
            process_name: Nome do processo (para identificação)
            target_table: Tabela destino no banco
            archive_bucket: Bucket para arquivamento
            archive_prefix: Prefixo do caminho no storage

        Returns:
            Dicionário com resultados da operação
        """
        results = {
            "process_id": str(uuid.uuid4()),
            "timestamp": datetime.now().isoformat(),
            "process_name": process_name,
            "archived_uri": "",
            "records_processed": 0,
        }

        if (
            isinstance(data_source, list)
            and data_source
            and isinstance(data_source[0], BaseModel)
        ):
            df = DataTransformer.model_to_dataframe(data_source)
        elif isinstance(data_source, pd.DataFrame):
            df = data_source
        else:
            logger.error(f"Tipo de dados não suportado: {type(data_source)}")
            return results

        archive_path = (
            f"{archive_prefix}/{process_name}/{datetime.now().strftime('%Y/%m/%d')}"
        )
        results["archived_uri"] = self.storage.upload_parquet(
            archive_bucket, archive_path, df
        )

        results["records_processed"] = self.db.insert_dataframe(target_table, df)

        logger.info(
            f"Processo {process_name} concluído: {results['records_processed']} registros "
            f"arquivados em {results['archived_uri']} e processados em {target_table}"
        )

        return results

    def get_models_from_database(
        self,
        query_or_table: str | TableName,
        model_class: Type[BaseModel],
        filters: dict[str, Any] = None,
    ) -> list[BaseModel]:
        """
        Recupera dados do banco e converte para modelos Pydantic.

        Args:
            query_or_table: Query SQL ou nome da tabela
            model_class: Classe do modelo Pydantic
            filters: Filtros para a consulta (opcional)

        Returns:
            lista de modelos Pydantic
        """
        df = self.db.query_to_dataframe(query_or_table, filters)

        if df.empty:
            logger.warning(
                f"Nenhum dado encontrado para converter em {model_class.__name__}"
            )
            return []

        models = DataTransformer.dataframe_to_models(df, model_class)
        logger.info(f"Convertidos {len(models)} registros para {model_class.__name__}")

        return models

    def close(self):
        """Fecha todas as conexões."""
        self.db.close()
        logger.info("DataOrchestrator encerrado")
