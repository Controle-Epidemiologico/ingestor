"""
Módulo de configurações de ambiente para a pipeline.
"""

from typing import Optional

from pydantic_settings import BaseSettings, SettingsConfigDict


class EnvConfig(BaseSettings):
    """
    Modelo de configuração de ambiente.
    Lê diretamente do arquivo `.env`, e mapeia-os para seus respectivos objetos.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_nested_delimiter="__",
        extra="ignore",
    )


class MinIO(
    BaseSettings,
    env_prefix="MINIO_",
):
    """
    Configurações do MinIO.
    """

    endpoint: Optional[str] = "localhost:9000"
    """Endereço do MinIO."""
    access_key: Optional[str] = "minioadmin"
    """Chave de acesso do MinIO."""
    secret_key: Optional[str] = "minioadmin"
    """Chave secreta do MinIO."""
    secure: bool = False
    """Se o MinIO está executando em HTTPS ou HTTP."""


class DuckDB(
    BaseSettings,
    env_prefix="DUCKDB_",
):
    """
    Configurações do DuckDB.
    """

    path: Optional[str] = "/app/data/h5n1.duckdb"
    """Caminho para o arquivo DuckDB."""


class Airflow(
    BaseSettings,
    env_prefix="AIRFLOW__",
):
    """
    Configurações do Airflow.
    """

    core__executor: Optional[str] = "LocalExecutor"
    """Executor do Airflow."""
    database__sql_alchemy_conn: Optional[str] = "sqlite:////opt/airflow/airflow.db"
    """String de conexão SQLAlchemy para o Airflow."""
    core__load_examples: bool = False
    """Carregar exemplos no Airflow."""


class Logging(
    BaseSettings,
    env_prefix="LOG_",
):
    """
    Configurações de logging.
    """

    level: Optional[str] = "DEBUG"
    """Nível de logging."""
