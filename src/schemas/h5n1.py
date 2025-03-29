from datetime import datetime
from enum import Enum
from typing import Any, Optional
from pydantic import BaseModel, Field, field_validator, model_validator

class SourceFormat(str, Enum):
    """Formatos suportados para fontes de dados."""
    JSON = "json"
    CSV = "csv"
    PDF = "pdf"
    HTML = "html"
    EXCEL = "excel"
    API = "api"
    UNSTRUCTURED = "unstructured"

class LocationType(str, Enum):
    """Tipos de localização."""
    POINT = "point"
    REGION = "region"

class LocationInfo(BaseModel):
    """Modelo para informações de localização."""
    location: str = Field(..., description="Nome da localização")
    location_type: LocationType = Field(default=LocationType.POINT, description="Tipo de localização")
    latitude: Optional[float] = Field(None, description="Latitude")
    longitude: Optional[float] = Field(None, description="Longitude")
    admin_level_1: Optional[str] = Field(None, description="Estado/Província")
    admin_level_2: Optional[str] = Field(None, description="Município/Cidade")

    @field_validator('latitude')
    def validate_latitude(cls, v):
        if v is not None and (v < -90 or v > 90):
            raise ValueError('Latitude deve estar entre -90 e 90')
        return v

    @field_validator('longitude')
    def validate_longitude(cls, v):
        if v is not None and (v < -180 or v > 180):
            raise ValueError('Longitude deve estar entre -180 e 180')
        return v

class BirdType(str, Enum):
    """Classificação de tipos de aves."""
    WILD = "wild"
    DOMESTIC_POULTRY = "poultry"
    DOMESTIC_PET = "pet"
    MIGRATORY = "migratory"
    UNKNOWN = "unknown"

class CaseData(BaseModel):
    """Dados de um caso individual de H5N1."""
    bird_species: str = Field(..., description="Espécie de ave")
    bird_type: BirdType = Field(default=BirdType.UNKNOWN, description="Classificação da ave")
    h5_subtype: Optional[str] = Field(None, description="Subtipo específico de H5 (ex: H5N1, H5N8)")
    detection_date: datetime = Field(..., description="Data de detecção/diagnóstico")
    test_method: Optional[str] = Field(None, description="Método de teste (PCR, sequenciamento, etc.)")
    additional_data: dict[str, Any] = Field(default_factory=dict, description="Dados adicionais")

    @model_validator(mode='after')
    def validate_subtype(self):
        if self.h5_subtype and not self.h5_subtype.startswith('H5'):
            raise ValueError('O subtipo deve iniciar com H5')
        return self

class SurveillanceData(BaseModel):
    """Dados essenciais de vigilância."""
    sample_type: str = Field(..., description="Tipo de amostra")
    result: str = Field(..., description="Resultado")
    collection_date: datetime = Field(..., description="Data de coleta")
    test_method: str = Field(..., description="Método de teste")
    bird_species: Optional[str] = Field(None, description="Espécie de ave amostrada")
    bird_type: Optional[BirdType] = Field(None, description="Classificação da ave")
    additional_data: dict[str, Any] = Field(default_factory=dict, description="Dados adicionais")

class RawFileAttachment(BaseModel):
    """Anexo de arquivo bruto."""
    content_type: str = Field(..., description="Tipo de conteúdo (MIME)")
    filename: str = Field(..., description="Nome do arquivo original")
    file_size: int = Field(..., description="Tamanho em bytes")
    file_hash: str = Field(..., description="Hash SHA-256 do conteúdo")
    storage_key: str = Field(..., description="Chave no MinIO")
    extracted: bool = Field(default=False, description="Se dados foram extraídos")

class Metadata(BaseModel):
    """Metadados essenciais."""
    source_format: SourceFormat = Field(..., description="Formato da fonte original")
    source_url: Optional[str] = Field(None, description="URL da fonte")
    collection_method: str = Field(..., description="Método de coleta")
    collected_by: str = Field(..., description="Responsável pela coleta")
    original_files: list[RawFileAttachment] = Field(default_factory=list, description="Arquivos originais")
    data_reliability: Optional[float] = Field(None, description="Confiabilidade dos dados (0-1)")

class RawAvianCase(BaseModel):
    """Esquema para validação de casos individuais de H5N1 em aves."""
    source_id: str = Field(..., description="ID original da fonte")
    source: str = Field(..., description="Fonte dos dados")
    collected_at: datetime = Field(..., description="Data de coleta dos dados")
    location_info: LocationInfo = Field(..., description="Informações de localização")
    case_data: CaseData = Field(..., description="Dados do caso individual")
    metadata: Metadata = Field(..., description="Metadados")

    class Config:
        schema_extra = {
            "example": {
                "source_id": "WAHIS-789456",
                "source": "OIE-WAHIS",
                "collected_at": "2023-05-15T14:30:00Z",
                "location_info": {
                    "location": "Campinas, Brazil",
                    "location_type": "point",
                    "latitude": -22.9064,
                    "longitude": -47.0616,
                    "admin_level_1": "São Paulo",
                    "admin_level_2": "Campinas"
                },
                "case_data": {
                    "bird_species": "Gallus gallus domesticus",
                    "bird_type": "poultry",
                    "detection_date": "2023-05-10T00:00:00Z",
                    "h5_subtype": "H5N1",
                    "test_method": "RT-PCR"
                },
                "metadata": {
                    "source_format": "pdf",
                    "source_url": "https://wahis.woah.org/report/123456",
                    "collection_method": "web_scraping",
                    "collected_by": "collector_bot"
                }
            }
        }

class RawSurveillanceRecord(BaseModel):
    """Esquema para validação de dados brutos de vigilância."""
    source_id: str = Field(..., description="ID original da fonte")
    source: str = Field(..., description="Fonte dos dados")
    collected_at: datetime = Field(..., description="Data de coleta")
    location_info: LocationInfo = Field(..., description="Informações de localização")
    surveillance_data: SurveillanceData = Field(..., description="Dados da vigilância")
    metadata: Metadata = Field(..., description="Metadados")

    class Config:
        schema_extra = {
            "example": {
                "source_id": "SURVEILLANCE-789",
                "source": "NATIONAL_LAB",
                "collected_at": "2023-06-01T09:15:00Z",
                "location_info": {
                    "location": "Ubatuba, Brazil",
                    "latitude": -23.4336,
                    "longitude": -45.0838,
                    "admin_level_1": "São Paulo",
                    "admin_level_2": "Ubatuba"
                },
                "surveillance_data": {
                    "sample_type": "Cloacal swab",
                    "result": "Negative",
                    "collection_date": "2023-05-28T00:00:00Z",
                    "test_method": "RT-PCR",
                    "bird_species": "Ardea alba",
                    "bird_type": "wild"
                },
                "metadata": {
                    "source_format": "excel",
                    "collection_method": "field_collection",
                    "collected_by": "field_team_42"
                }
            }
        }