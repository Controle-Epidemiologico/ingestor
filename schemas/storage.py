from dataclasses import dataclass
from enum import StrEnum
from typing import Optional


class BucketName(StrEnum):
    """Nome do bucket."""

    RAW = "raw"


class DataCategory(StrEnum):
    """Categorias de dados."""

    AVIAN_CASES = "avian_cases"


@dataclass
class BucketStats:
    """Estatísticas de um bucket."""

    object_count: int
    size_bytes: int
    last_modified: Optional[str] = None


@dataclass
class StorageKey:
    """Chave de armazenamento para o MinIO."""

    bucket: BucketName
    category: DataCategory
    source: str
    year: str
    month: str
    day: str
    filename: str

    def __post_init__(self):
        if not self.year.isdigit() or len(self.year) != 4:
            raise ValueError(f"Formato de ano inválido: {self.year}")

        if not self.month.isdigit() or not (1 <= int(self.month) <= 12):
            raise ValueError(f"Formato de mês inválido: {self.month}")

        if not self.day.isdigit() or not (1 <= int(self.day) <= 31):
            raise ValueError(f"Formato de dia inválido: {self.day}")

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
            filename=filename,
        )
