from enum import Enum
from typing import Optional, List, Dict, Any
from dataclasses import dataclass


class TableName(str, Enum):
    """Nomes das tabelas/visualizações no DuckDB."""
    RAW_AVIAN_CASES = "raw_avian_cases"
    RAW_SURVEILLANCE = "raw_surveillance"
    CASES_BY_REGION = "cases_by_region"
    SURVEILLANCE_SUMMARY = "surveillance_summary"


@dataclass
class TableSchema:
    """Esquema para tabelas do DuckDB."""
    name: str
    columns: Dict[str, str]
    primary_key: Optional[str] = None

    def get_create_table_sql(self) -> str:
        """Gera SQL para criar a tabela."""
        columns_sql = []
        for col_name, col_type in self.columns.items():
            column_def = f"{col_name} {col_type}"
            if self.primary_key and col_name == self.primary_key:
                column_def += " PRIMARY KEY"
            columns_sql.append(column_def)

        return f"CREATE TABLE IF NOT EXISTS {self.name} (\n" + \
               ",\n".join(f"  {col}" for col in columns_sql) + \
               "\n);"


@dataclass
class ViewSchema:
    """Esquema para views no DuckDB."""
    name: str
    query: str

    def get_create_view_sql(self) -> str:
        """Gera SQL para criar a view."""
        return f"CREATE OR REPLACE VIEW {self.name} AS\n{self.query};"


AVIAN_CASES_SCHEMA = TableSchema(
    name=TableName.RAW_AVIAN_CASES,
    columns={
        "id": "VARCHAR",
        "source_id": "VARCHAR",
        "source": "VARCHAR",
        "collected_at": "TIMESTAMP",
        "location": "VARCHAR",
        "latitude": "DOUBLE",
        "longitude": "DOUBLE",
        "bird_species": "VARCHAR",
        "bird_type": "VARCHAR",
        "h5_subtype": "VARCHAR",
        "detection_date": "TIMESTAMP",
        "test_method": "VARCHAR",
        "case_data": "JSON",
        "metadata": "JSON"
    },
    primary_key="id"
)

SURVEILLANCE_SCHEMA = TableSchema(
    name=TableName.RAW_SURVEILLANCE,
    columns={
        "id": "VARCHAR",
        "source_id": "VARCHAR",
        "source": "VARCHAR",
        "collected_at": "TIMESTAMP",
        "location": "VARCHAR",
        "latitude": "DOUBLE",
        "longitude": "DOUBLE",
        "sample_type": "VARCHAR",
        "result": "VARCHAR",
        "bird_species": "VARCHAR",
        "bird_type": "VARCHAR",
        "surveillance_data": "JSON",
        "metadata": "JSON"
    },
    primary_key="id"
)

CASES_BY_REGION_VIEW = ViewSchema(
    name=TableName.CASES_BY_REGION,
    query="""
        SELECT
            location,
            COUNT(*) as case_count,
            bird_species,
            bird_type,
            h5_subtype,
            MIN(detection_date) as first_detection,
            MAX(detection_date) as last_detection
        FROM raw_avian_cases
        GROUP BY location, bird_species, bird_type, h5_subtype
        ORDER BY case_count DESC
    """
)

SURVEILLANCE_SUMMARY_VIEW = ViewSchema(
    name=TableName.SURVEILLANCE_SUMMARY,
    query="""
        SELECT
            location,
            result,
            bird_species,
            bird_type,
            COUNT(*) as sample_count,
            MIN(collected_at) as first_sample,
            MAX(collected_at) as last_sample
        FROM raw_surveillance
        GROUP BY location, result, bird_species, bird_type
        ORDER BY location, result
    """
)