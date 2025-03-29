from dataclasses import dataclass
from enum import Enum
from typing import Dict, List, Optional, Union


class BucketName(str, Enum):
    """Nomes dos buckets no MinIO."""
    RAW = "raw"
    PROCESSED = "processed"


class DataCategory(str, Enum):
    """Categorias de dados."""
    AVIAN_CASES = "avian_cases"
    SURVEILLANCE = "surveillance"


@dataclass
class BucketStats:
    """Estatísticas de um bucket."""
    object_count: int
    size_bytes: int
    last_modified: Optional[str] = None


@dataclass
class StorageKey:
    """Modelo para representar a chave de um objeto no storage."""
    bucket: BucketName
    category: DataCategory
    source: str
    year: str
    month: str
    day: str
    filename: str

    @property
    def full_path(self) -> str:
        """Retorna o caminho completo."""
        return f"{self.category}/{self.source}/year={self.year}/month={self.month}/day={self.day}/{self.filename}"

    @classmethod
    def from_path(cls, bucket: BucketName, path: str) -> "StorageKey":
        """Cria um objeto StorageKey a partir de um caminho."""
        parts = path.split("/")
        if len(parts) < 6:
            raise ValueError(f"Caminho inválido: {path}")

        category_str = parts[0]
        source = parts[1]
        year = parts[2].split("=")[1]
        month = parts[3].split("=")[1]
        day = parts[4].split("=")[1]
        filename = parts[5]

        try:
            category = DataCategory(category_str)
        except ValueError:
            raise ValueError(f"Categoria inválida: {category_str}")

        return cls(
            bucket=bucket,
            category=category,
            source=source,
            year=year,
            month=month,
            day=day,
            filename=filename
        )