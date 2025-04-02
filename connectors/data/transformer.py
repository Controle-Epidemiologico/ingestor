from datetime import datetime
from typing import Any, Type, TypeVar, Hashable

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pydantic import BaseModel

from configs.logger import get_logger

logger = get_logger(__name__)

T = TypeVar("T", bound=BaseModel)


class DataTransformer:
    """
    Responsável por transformações de dados entre diferentes formatos:
    - Modelos Pydantic <-> DataFrames
    - DataFrames <-> Parquet
    - Validações e padronizações de esquema
    """

    @staticmethod
    def model_to_dataframe(models: list[BaseModel]) -> pd.DataFrame:
        """Converte lista de modelos Pydantic para DataFrame."""
        if not models:
            logger.warning("Lista de modelos vazia, retornando DataFrame vazio")
            return pd.DataFrame()

        flattened_data = [model.model_dump() for model in models]
        df = pd.DataFrame(flattened_data)
        logger.info(f"Convertidos {len(models)} modelos para DataFrame")
        return df

    @staticmethod
    def dataframe_to_models(df: pd.DataFrame, model_class: Type[T], ) -> list[T]:
        """Converte DataFrame para lista de modelos Pydantic."""
        if df.empty:
            logger.warning(
                f"DataFrame vazio, retornando lista vazia de {model_class.__name__}"
            )
            return []

        models = []
        errors = []

        for _, row in df.iterrows():
            try:
                model_data = row.to_dict()
                # Remover valores NaN que causam problemas na validação
                model_data = {k: v for k, v in model_data.items() if pd.notna(v)}
                model = model_class.model_validate(model_data)
                models.append(model)
            except Exception as e:
                errors.append(str(e))
                logger.error(
                    f"Erro ao converter linha para modelo {model_class.__name__}: {str(e)}"
                )

        if errors:
            logger.warning(f"Ocorreram {len(errors)} erros durante a conversão")

        logger.info(
            f"Convertidas {len(models)} linhas para modelos {model_class.__name__}"
        )
        return models

    @staticmethod
    def standardize_columns(
            df: pd.DataFrame, target_columns: list[str], fill_missing: bool = True,
    ) -> pd.DataFrame:
        """
        Padroniza colunas de um DataFrame conforme lista alvo.

        Args:
            df: DataFrame original
            target_columns: Lista de colunas desejadas
            fill_missing: Se True, preenche colunas faltantes com None
        """
        result_df = df.copy()

        for col in target_columns:
            if col not in result_df.columns and fill_missing:
                result_df[col] = None

        result_df = result_df[target_columns]
        logger.debug(
            f"DataFrame padronizado: {len(result_df)} linhas, {len(target_columns)} colunas"
        )
        return result_df

    @staticmethod
    def extract_partition_metadata(df: pd.DataFrame) -> dict[str, Any]:
        """
        Extrai metadados para particionamento a partir de um DataFrame.
        """
        metadata = {}

        now = datetime.now()
        metadata["year"] = now.year
        metadata["month"] = now.month
        metadata["day"] = now.day

        # Particionamento por origem dos dados (se disponível)
        if "source" in df.columns and not df["source"].isna().all():
            metadata["source"] = df["source"].iloc[0]

        # Particionamento por região (se disponível)
        if "region" in df.columns and not df["region"].isna().all():
            metadata["region"] = df["region"].iloc[0]

        logger.debug(f"Metadados extraídos: {metadata}")
        return metadata

    @staticmethod
    def apply_schema_validation(
            df: pd.DataFrame, schema: dict[str, str],
    ) -> pd.DataFrame:
        """
        Aplica validação e conversão de tipos conforme esquema definido.
        """
        result_df = df.copy()

        # Converter tipos conforme esquema
        for col, dtype in schema.items():
            if col in result_df.columns:
                try:
                    result_df[col] = result_df[col].astype(dtype)
                except Exception as e:
                    logger.warning(
                        f"Erro ao converter coluna {col} para {dtype}: {str(e)}"
                    )

        logger.info(f"Validação de esquema aplicada: {len(schema)} colunas processadas")
        return result_df

    @staticmethod
    def dataframe_to_parquet_bytes(df: pd.DataFrame) -> bytes:
        """Converte DataFrame para bytes em formato Parquet."""
        if df.empty:
            logger.warning("DataFrame vazio, retornando Parquet vazio")
            empty_table = pa.Table.from_pandas(df)
            buffer = pa.BufferOutputStream()
            pq.write_table(empty_table, buffer)
            return buffer.getvalue().to_pybytes()

        table = pa.Table.from_pandas(df)
        buffer = pa.BufferOutputStream()
        pq.write_table(table, buffer)
        parquet_bytes = buffer.getvalue().to_pybytes()

        logger.debug(f"DataFrame convertido para Parquet: {len(parquet_bytes)} bytes")
        return parquet_bytes

    @staticmethod
    def parquet_bytes_to_dataframe(parquet_bytes: bytes) -> pd.DataFrame:
        """Converte bytes em formato Parquet para DataFrame."""
        if not parquet_bytes:
            logger.warning("Dados Parquet vazios, retornando DataFrame vazio")
            return pd.DataFrame()

        buffer = pa.py_buffer(parquet_bytes)
        table = pq.read_table(buffer)
        df = table.to_pandas()

        logger.debug(f"Parquet convertido para DataFrame: {len(df)} linhas")
        return df

    @staticmethod
    def infer_schema_from_dataframe(df: pd.DataFrame) -> dict[Hashable, str]:
        """Infere um esquema de tipos a partir de um DataFrame."""
        schema = {}
        for col, dtype in df.dtypes.items():
            schema[col] = str(dtype)

        logger.debug(f"Esquema inferido: {len(schema)} colunas")
        return schema
