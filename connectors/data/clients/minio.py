import io
import uuid
from datetime import datetime
from typing import Optional, ClassVar

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from minio import Minio
from minio.error import S3Error

from configs.logger import get_logger
from configs.settings import MinIO as MinIOConfig

logger = get_logger(__name__)


class MinioClient:
    """ """

    _instances: ClassVar[dict[str, "MinioClient"]] = {}

    def __init__(
            self,
            config_or_client: MinIOConfig | Minio | None = None,
    ):
        """Inicializa o cliente MinIO."""
        if isinstance(config_or_client, Minio):
            self.client = config_or_client
            logger.info("Cliente MinIO inicializado a partir de instância existente")
        else:
            config = (
                config_or_client
                if isinstance(config_or_client, MinIOConfig)
                else MinIOConfig()
            )
            self.client = Minio(
                endpoint=config.endpoint,
                access_key=config.access_key,
                secret_key=config.secret_key,
                secure=config.secure,
            )
            logger.info(f"Cliente MinIO inicializado para endpoint: {config.endpoint}")

    @classmethod
    def get_instance(
            cls,
            config: Optional[MinIOConfig] = None,
    ) -> "MinioClient":
        """Obtém uma instância compartilhada do cliente."""
        config = config or MinIOConfig()
        cache_key = config.endpoint

        if cache_key not in cls._instances:
            cls._instances[cache_key] = cls(config)

        return cls._instances[cache_key]

    def ensure_bucket_exists(
            self,
            bucket_name: str,
    ) -> None:
        """Garante que o bucket exista, criando se necessário."""
        try:
            if not self.client.bucket_exists(bucket_name):
                self.client.make_bucket(bucket_name)
                logger.info(f"Bucket criado: {bucket_name}")
            else:
                logger.debug(f"Bucket já existe: {bucket_name}")
        except S3Error as e:
            logger.error(f"Erro ao verificar/criar bucket {bucket_name}: {str(e)}")
            raise

    def list_objects(
            self,
            bucket_name: str,
            prefix: str = "",
            recursive: bool = True,
    ) -> list[str]:
        """Lista objetos em um bucket com filtro por prefixo."""
        try:
            objects = self.client.list_objects(
                bucket_name, prefix=prefix, recursive=recursive
            )
            object_names = [obj.object_name for obj in objects]
            logger.debug(
                f"Listados {len(object_names)} objetos em {bucket_name}/{prefix}"
            )
            return object_names
        except S3Error as e:
            logger.error(f"Erro ao listar objetos em {bucket_name}/{prefix}: {str(e)}")
            raise

    def upload_object(
            self,
            bucket_name: str,
            object_path: str,
            data,
            length: Optional[int] = None,
            content_type: str = "application/octet-stream",
    ) -> str:
        """Faz upload direto de dados para o MinIO e retorna a URI.

        Args:
            bucket_name: Nome do bucket
            object_path: Caminho completo do objeto (incluindo prefixos)
            data: Dados a serem carregados (bytes, stream, etc)
            length: Tamanho dos dados em bytes
            content_type: Tipo MIME do conteúdo

        Returns:
            URI S3 completa do objeto carregado
        """
        try:
            if length is None and hasattr(data, "getbuffer"):
                length = data.getbuffer().nbytes

            self.ensure_bucket_exists(bucket_name)

            self.client.put_object(
                bucket_name=bucket_name,
                object_name=object_path,
                data=data,
                length=length,
                content_type=content_type,
            )

            uri = f"s3://{bucket_name}/{object_path}"
            logger.info(f"Upload concluído: {uri}")
            return uri
        except S3Error as e:
            logger.error(f"Erro no upload para {bucket_name}/{object_path}: {str(e)}")
            raise

    def download_object(
            self,
            bucket_name: str,
            object_path: str,
    ) -> bytes:
        """Baixa um objeto como bytes."""
        try:
            response = self.client.get_object(bucket_name, object_path)
            data = response.read()
            response.close()
            response.release_conn()
            logger.info(f"Download concluído: {bucket_name}/{object_path}")
            return data
        except S3Error as e:
            logger.error(f"Erro ao baixar {bucket_name}/{object_path}: {str(e)}")
            raise

    def delete_object(
            self,
            bucket_name: str,
            object_path: str,
    ) -> None:
        """Remove um objeto do MinIO."""
        try:
            self.client.remove_object(bucket_name, object_path)
            logger.info(f"Objeto removido: {bucket_name}/{object_path}")
        except S3Error as e:
            logger.error(
                f"Erro ao remover objeto {bucket_name}/{object_path}: {str(e)}"
            )
            raise

    def upload_parquet(
            self,
            bucket: str,
            path: str,
            dataframe: pd.DataFrame,
            partition_cols: list[str] = None,
    ) -> str:
        """Converte DataFrame para Parquet e faz upload.

        Args:
            bucket: Nome do bucket
            path: Caminho base do objeto
            dataframe: DataFrame a ser armazenado
            partition_cols: Colunas para particionamento (opcional)

        Returns:
            URI completa do objeto carregado
        """
        try:
            self.ensure_bucket_exists(bucket)

            timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

            if partition_cols:
                final_path = self._create_partitioned_path(
                    path, dataframe, partition_cols, timestamp
                )
            else:
                final_path = f"{path.rstrip('/')}/{timestamp}.parquet"

            table = pa.Table.from_pandas(dataframe)
            buffer = io.BytesIO()
            pq.write_table(table, buffer)
            buffer.seek(0)

            return self.upload_object(
                bucket_name=bucket,
                object_path=final_path,
                data=buffer,
                length=buffer.getbuffer().nbytes,
                content_type="application/x-parquet",
            )
        except Exception as e:
            logger.error(f"Erro ao converter/fazer upload de DataFrame: {str(e)}")
            raise

    def _create_partitioned_path(
            self,
            base_path: str,
            df: pd.DataFrame,
            partition_cols: list[str],
            timestamp: str,
    ) -> str:
        """Cria caminho particionado baseado em colunas do DataFrame."""
        base_path = base_path.rstrip("/")

        partition_path = ""
        for col in partition_cols:
            if col in df.columns:
                # Pegar primeiro valor não-nulo da coluna para particionamento
                value = (
                    df[col].dropna().iloc[0] if not df[col].isna().all() else "unknown"
                )
                partition_path += f"/{col}={value}"

        uuid_str = str(uuid.uuid4())
        return f"{base_path}{partition_path}/{timestamp}_{uuid_str[:8]}.parquet"

    def download_parquet(
            self,
            bucket: str,
            path: str,
    ) -> pd.DataFrame:
        """Baixa um arquivo Parquet e converte para DataFrame."""
        try:
            data = self.download_object(bucket, path)
            buffer = io.BytesIO(data)
            table = pq.read_table(buffer)
            df = table.to_pandas()
            logger.info(f"Parquet convertido para DataFrame: {len(df)} linhas")
            return df
        except Exception as e:
            logger.error(f"Erro ao baixar/converter Parquet: {str(e)}")
            raise

    def list_parquet_files(
            self,
            bucket: str,
            prefix: str,
    ) -> list[str]:
        """Lista caminhos de arquivos Parquet em um bucket/prefixo."""
        try:
            all_files = self.list_objects(bucket, prefix)
            parquet_files = [f for f in all_files if f.endswith(".parquet")]
            logger.info(
                f"Encontrados {len(parquet_files)} arquivos Parquet em {bucket}/{prefix}"
            )
            return parquet_files
        except Exception as e:
            logger.error(f"Erro ao listar arquivos Parquet: {str(e)}")
            raise

    def get_object_uris(
            self,
            bucket: str,
            prefix: str = "",
    ) -> list[str]:
        """Retorna URIs S3 completas para objetos.

        Args:
            bucket: Nome do bucket
            prefix: Prefixo para filtrar objetos (opcional)

        Returns:
            Lista de URIs S3 no formato s3://bucket/path
        """
        try:
            object_paths = self.list_objects(bucket, prefix)
            uris = [f"s3://{bucket}/{path}" for path in object_paths]
            return uris
        except Exception as e:
            logger.error(f"Erro ao gerar URIs para {bucket}/{prefix}: {str(e)}")
            raise

    def get_parquet_uris(
            self,
            bucket: str,
            prefix: str = "",
    ) -> list[str]:
        """Retorna URIs S3 apenas para arquivos Parquet.

        Args:
            bucket: Nome do bucket
            prefix: Prefixo para filtrar arquivos (opcional)

        Returns:
            Lista de URIs S3 para arquivos Parquet
        """
        try:
            parquet_paths = self.list_parquet_files(bucket, prefix)
            return [f"s3://{bucket}/{path}" for path in parquet_paths]
        except Exception as e:
            logger.error(f"Erro ao gerar URIs Parquet para {bucket}/{prefix}: {str(e)}")
            raise

    def object_exists(
            self,
            bucket: str,
            path: str,
    ) -> bool:
        """Verifica se um objeto existe no bucket."""
        try:
            self.client.stat_object(bucket, path)
            return True
        except S3Error:
            return False
