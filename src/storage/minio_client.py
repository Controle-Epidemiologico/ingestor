import io
import uuid
from datetime import datetime
from typing import Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from minio import Minio
from minio.error import S3Error

from src.schemas.h5n1 import RawAvianCase, RawSurveillanceRecord
from src.schemas.storage import BucketName, StorageKey, DataCategory
from src.utils.logging import get_logger

logger = get_logger(__name__)


class MinioClient:
    """Cliente para interação com o MinIO storage."""

    def __init__(
            self,
            endpoint: str,
            access_key: str,
            secret_key: str,
            secure: bool = False
    ):
        """
        Inicializa o cliente MinIO.

        Args:
            endpoint: Endpoint do MinIO (ex: "minio:9000")
            access_key: Chave de acesso
            secret_key: Chave secreta
            secure: Se True, usa HTTPS
        """
        self.client = Minio(
            endpoint=endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure
        )
        logger.info(f"Cliente MinIO inicializado para endpoint: {endpoint}")

    def ensure_bucket_exists(self, bucket_name: str) -> None:
        """
        Garante que o bucket exista, criando-o se necessário.

        Args:
            bucket_name: Nome do bucket
        """
        try:
            if not self.client.bucket_exists(bucket_name):
                self.client.make_bucket(bucket_name)
                logger.info(f"Bucket criado: {bucket_name}")
            else:
                logger.debug(f"Bucket já existe: {bucket_name}")
        except S3Error as e:
            logger.error(f"Erro ao verificar/criar bucket {bucket_name}: {str(e)}")
            raise

    def initialize_buckets(self) -> None:
        """Inicializa os buckets padrão (raw, processed)."""
        for bucket in BucketName:
            self.ensure_bucket_exists(bucket.value)
        logger.info("Buckets padrão inicializados")

    def _generate_key(self, data_type: DataCategory, source: str, timestamp: datetime, uuid_str: str) -> str:
        """
        Gera uma chave hierárquica para o objeto no MinIO.

        Args:
            data_type: Categoria de dados (ex: DataCategory.AVIAN_CASES)
            source: Fonte dos dados
            timestamp: Data/hora da coleta
            uuid_str: UUID único para o arquivo

        Returns:
            Caminho completo para o objeto no MinIO
        """
        year = timestamp.strftime("%Y")
        month = timestamp.strftime("%m")
        day = timestamp.strftime("%d")
        file_timestamp = timestamp.strftime("%Y%m%d%H%M%S")

        return f"{data_type.value}/{source}/year={year}/month={month}/day={day}/data_{file_timestamp}_{uuid_str}.parquet"

    def upload_avian_case(self, data: RawAvianCase | list[RawAvianCase]) -> str:
        """
        Faz upload de dados de casos de H5N1 em aves para o bucket raw.

        Args:
            data: Objeto ou lista de objetos RawAvianCase

        Returns:
            Chave do objeto no MinIO
        """
        if isinstance(data, RawAvianCase):
            data = [data]

        df = pd.DataFrame([item.model_dump() for item in data])

        uuid_str = str(uuid.uuid4())
        timestamp = datetime.now()
        key = self._generate_key(DataCategory.AVIAN_CASES, data[0].source, timestamp, uuid_str)

        table = pa.Table.from_pandas(df)
        buffer = io.BytesIO()
        pq.write_table(table, buffer)
        buffer.seek(0)

        try:
            self.client.put_object(
                bucket_name=BucketName.RAW.value,
                object_name=key,
                data=buffer,
                length=buffer.getbuffer().nbytes,
                content_type="application/octet-stream"
            )
            logger.info(f"Upload concluído: {key}")
            return key
        except S3Error as e:
            logger.error(f"Erro no upload para {key}: {str(e)}")
            raise

    def upload_surveillance_data(self, data: RawSurveillanceRecord | list[RawSurveillanceRecord]) -> str:
        """
        Faz upload de dados de vigilância para o bucket raw.

        Args:
            data: Objeto ou lista de objetos RawSurveillanceRecord

        Returns:
            Chave do objeto no MinIO
        """
        if isinstance(data, RawSurveillanceRecord):
            data = [data]

        df = pd.DataFrame([item.model_dump() for item in data])

        uuid_str = str(uuid.uuid4())
        timestamp = datetime.now()
        key = self._generate_key(DataCategory.SURVEILLANCE, data[0].source, timestamp, uuid_str)

        table = pa.Table.from_pandas(df)
        buffer = io.BytesIO()
        pq.write_table(table, buffer)
        buffer.seek(0)

        try:
            self.client.put_object(
                bucket_name=BucketName.RAW.value,
                object_name=key,
                data=buffer,
                length=buffer.getbuffer().nbytes,
                content_type="application/octet-stream"
            )
            logger.info(f"Upload concluído: {key}")
            return key
        except S3Error as e:
            logger.error(f"Erro no upload para {key}: {str(e)}")
            raise

    def download_object(self, bucket_name: str, object_name: str) -> pd.DataFrame:
        """
        Baixa um objeto Parquet e retorna como DataFrame.

        Args:
            bucket_name: Nome do bucket
            object_name: Nome/caminho do objeto

        Returns:
            DataFrame com os dados
        """
        try:
            response = self.client.get_object(bucket_name, object_name)
            buffer = io.BytesIO(response.read())
            table = pq.read_table(buffer)
            df = table.to_pandas()
            response.close()
            response.release_conn()
            logger.info(f"Download concluído: {bucket_name}/{object_name}")
            return df
        except S3Error as e:
            logger.error(f"Erro ao baixar {bucket_name}/{object_name}: {str(e)}")
            raise

    def list_objects(self, bucket_name: str, prefix: str = "", recursive: bool = True) -> list[str]:
        """
        Lista objetos em um bucket com filtro por prefixo.

        Args:
            bucket_name: Nome do bucket
            prefix: Prefixo para filtrar objetos
            recursive: Se deve listar recursivamente

        Returns:
            Lista de chaves de objetos
        """
        try:
            objects = self.client.list_objects(
                bucket_name, prefix=prefix, recursive=recursive
            )
            return [obj.object_name for obj in objects]
        except S3Error as e:
            logger.error(f"Erro ao listar objetos em {bucket_name}: {str(e)}")
            raise

    def upload_large_file(self, bucket_name: str, object_name: str, file_path: str) -> None:
        """
        Faz upload de arquivos grandes usando multipart upload.

        Args:
            bucket_name: Nome do bucket
            object_name: Nome do objeto
            file_path: Caminho para o arquivo local
        """
        try:
            self.client.fput_object(
                bucket_name=bucket_name,
                object_name=object_name,
                file_path=file_path
            )
            logger.info(f"Upload de arquivo grande concluído: {bucket_name}/{object_name}")
        except S3Error as e:
            logger.error(f"Erro no upload para {bucket_name}/{object_name}: {str(e)}")
            raise

    def upload_dataframe(self, bucket: BucketName, category: DataCategory, source: str,
                         df: pd.DataFrame, timestamp: Optional[datetime] = None) -> StorageKey:
        """Upload de DataFrame com geração automática de chave organizada."""
        timestamp = timestamp or datetime.now()
        uuid_str = str(uuid.uuid4())

        year = timestamp.strftime("%Y")
        month = timestamp.strftime("%m")
        day = timestamp.strftime("%d")
        file_timestamp = timestamp.strftime("%Y%m%d%H%M%S")
        filename = f"data_{file_timestamp}_{uuid_str}.parquet"

        storage_key = StorageKey(
            bucket=bucket,
            category=category,
            source=source,
            year=year,
            month=month,
            day=day,
            filename=filename
        )

        table = pa.Table.from_pandas(df)
        buffer = io.BytesIO()
        pq.write_table(table, buffer)
        buffer.seek(0)

        try:
            self.client.put_object(
                bucket_name=bucket.value,
                object_name=storage_key.full_path,
                data=buffer,
                length=buffer.getbuffer().nbytes,
                content_type="application/octet-stream"
            )
            logger.info(f"Upload concluído: {bucket.value}/{storage_key.full_path}")
            return storage_key
        except S3Error as e:
            logger.error(f"Erro no upload para {bucket.value}/{storage_key.full_path}: {str(e)}")
            raise
