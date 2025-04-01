from datetime import datetime
from enum import StrEnum
from typing import Optional

from pydantic import BaseModel, Field, field_validator

from schemas.storage import BucketName, DataCategory, StorageKey


class BirdType(StrEnum):
    """Classificação de tipos de aves."""

    WILD = "wild"
    DOMESTIC = "domestic"
    MIGRATORY = "migratory"
    UNKNOWN = "unknown"


class ConfidenceLevel(StrEnum):
    """Nível de confiabilidade da informação."""

    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"
    UNVERIFIED = "unverified"


class Location(BaseModel):
    """Localização geográfica do caso."""

    region: Optional[str] = Field(None, description="Estado/Província")
    city: Optional[str] = Field(None, description="Cidade/Município")
    latitude: Optional[float] = Field(None, description="Latitude em graus")
    longitude: Optional[float] = Field(None, description="Longitude em graus")

    @field_validator("latitude")
    def validate_latitude(cls, v):
        if v is not None and (v < -90 or v > 90):
            raise ValueError("Latitude deve estar entre -90 e 90")
        return v

    @field_validator("longitude")
    def validate_longitude(cls, v):
        if v is not None and (v < -180 or v > 180):
            raise ValueError("Longitude deve estar entre -180 e 180")
        return v


class BirdInfo(BaseModel):
    """Informações da ave afetada."""

    species: str = Field(..., description="Espécie da ave")
    type: BirdType = Field(default=BirdType.UNKNOWN, description="Classificação da ave")
    population_size: Optional[int] = Field(
        None, description="Tamanho da população afetada"
    )


class Metadata(BaseModel):
    """Metadados sobre o registro."""

    source: str = Field(..., description="Fonte da informação")
    confidence: ConfidenceLevel = Field(
        default=ConfidenceLevel.UNVERIFIED, description="Nível de confiabilidade"
    )
    collected_at: datetime = Field(
        default_factory=datetime.now, description="Data de coleta do registro"
    )
    is_verified: bool = Field(
        default=False, description="Se o caso foi verificado oficialmente"
    )


class AvianFluCase(BaseModel):
    """Caso de gripe aviária em aves."""

    case_id: str = Field(..., description="Identificador único do caso")
    detection_date: datetime = Field(..., description="Data de detecção do caso")
    bird: BirdInfo = Field(..., description="Informações sobre a ave")
    location: Location = Field(..., description="Localização do caso")
    metadata: Metadata = Field(..., description="Metadados do registro")

    def to_storage_key(self, filename: str) -> StorageKey:
        """Gera uma chave de armazenamento para o caso."""
        detection_date = self.detection_date
        return StorageKey(
            bucket=BucketName.RAW,
            category=DataCategory.AVIAN_CASES,
            source=self.metadata.source.lower().replace(" ", "_"),
            year=str(detection_date.year),
            month=f"{detection_date.month:02d}",
            day=f"{detection_date.day:02d}",
            filename=filename,
        )
