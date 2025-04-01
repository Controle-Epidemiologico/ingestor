from dataclasses import dataclass
from enum import Enum
from typing import Optional


class TableName(str, Enum):
    """Nomes das tabelas/visualizações no DuckDB."""

    AVIAN_CASES = "avian_cases"
    SURVEILLANCE = "surveillance"
    CASES_BY_REGION = "cases_by_region"
    CASES_BY_BIRD_TYPE = "cases_by_bird_type"
    SURVEILLANCE_SUMMARY = "surveillance_summary"


@dataclass
class TableSchema:
    """Esquema para tabelas do DuckDB."""

    name: str
    columns: dict[str, str]
    primary_key: Optional[str] = None

    def get_create_table_sql(self) -> str:
        """Gera SQL para criar a tabela."""
        columns_sql = []
        for col_name, col_type in self.columns.items():
            column_def = f"{col_name} {col_type}"
            if self.primary_key and col_name == self.primary_key:
                column_def += " PRIMARY KEY"
            columns_sql.append(column_def)

        return (
            f"CREATE TABLE IF NOT EXISTS {self.name} (\n"
            + ",\n".join(f"  {col}" for col in columns_sql)
            + "\n);"
        )


@dataclass
class ViewSchema:
    """Esquema para views no DuckDB."""

    name: str
    query: str

    def get_create_view_sql(self) -> str:
        """Gera SQL para criar a view."""
        view_name = self.name.value if hasattr(self.name, "value") else self.name
        return f"CREATE OR REPLACE VIEW {view_name} AS\n{self.query};"

AVIAN_CASES_SCHEMA = TableSchema(
    name=TableName.AVIAN_CASES,
    columns={
        "case_id": "VARCHAR",
        "detection_date": "TIMESTAMP",
        "region": "VARCHAR",
        "city": "VARCHAR",
        "latitude": "DOUBLE",
        "longitude": "DOUBLE",
        "bird_species": "VARCHAR",
        "bird_type": "VARCHAR",  # Enum: wild, domestic, migratory, unknown
        "population_size": "INTEGER",
        "source": "VARCHAR",
        "confidence_level": "VARCHAR",  # Enum: high, medium, low, unverified
        "collected_at": "TIMESTAMP",
        "is_verified": "BOOLEAN",
        "full_data": "JSON",
    },
    primary_key="case_id",
)

SURVEILLANCE_SCHEMA = TableSchema(
    name=TableName.SURVEILLANCE,
    columns={
        "id": "VARCHAR",
        "collection_date": "TIMESTAMP",
        "region": "VARCHAR",
        "city": "VARCHAR",
        "latitude": "DOUBLE",
        "longitude": "DOUBLE",
        "bird_species": "VARCHAR",
        "bird_type": "VARCHAR",
        "sample_type": "VARCHAR",
        "result": "VARCHAR",
        "source": "VARCHAR",
        "confidence_level": "VARCHAR",
        "is_verified": "BOOLEAN",
        "full_data": "JSON",
    },
    primary_key="id",
)

CASES_BY_REGION_VIEW = ViewSchema(
    name=TableName.CASES_BY_REGION,
    query="""
        SELECT
            region,
            city,
            COUNT(*) as case_count,
            bird_species,
            bird_type,
            MIN(detection_date) as first_detection,
            MAX(detection_date) as last_detection,
            AVG(CASE WHEN confidence_level = 'high' THEN 3
                     WHEN confidence_level = 'medium' THEN 2
                     WHEN confidence_level = 'low' THEN 1
                     ELSE 0 END) as avg_confidence
        FROM avian_cases
        GROUP BY region, city, bird_species, bird_type
        ORDER BY case_count DESC
    """,
)

CASES_BY_BIRD_TYPE_VIEW = ViewSchema(
    name=TableName.CASES_BY_BIRD_TYPE,
    query="""
        SELECT
            bird_type,
            bird_species,
            COUNT(*) as case_count,
            COUNT(DISTINCT region) as affected_regions,
            MIN(detection_date) as first_detection,
            MAX(detection_date) as last_detection
        FROM avian_cases
        GROUP BY bird_type, bird_species
        ORDER BY case_count DESC
    """,
)

SURVEILLANCE_SUMMARY_VIEW = ViewSchema(
    name=TableName.SURVEILLANCE_SUMMARY,
    query="""
        SELECT
            region,
            city,
            result,
            bird_species,
            bird_type,
            COUNT(*) as sample_count,
            MIN(collection_date) as first_sample,
            MAX(collection_date) as last_sample,
            COUNT(DISTINCT sample_type) as sample_type_count
        FROM surveillance
        GROUP BY region, city, result, bird_species, bird_type
        ORDER BY region, result
    """,
)
