import uuid
import random
from datetime import datetime, timedelta

from schemas.models.h5n1 import (
    AvianFluCase, Location, BirdInfo, Metadata,
    BirdType, ConfidenceLevel
)
from connectors.storage.minio import MinioClient
from connectors.storage.duckdb import DuckDBClient
from schemas.database import TableName
from configs.logger import get_logger, configure_logging
from configs.settings import MinIO, DuckDB, Airflow, Logging

# Configurar logging para exibir mais detalhes
configure_logging(log_level="DEBUG")
logger = get_logger("test_workflow")

def gerar_casos_teste(quantidade=10):
    """Gera casos fictícios de gripe aviária para teste."""
    regioes = ["Norte", "Nordeste", "Centro-Oeste", "Sudeste", "Sul"]
    cidades = {
        "Norte": ["Manaus", "Belém", "Macapá"],
        "Nordeste": ["Recife", "Salvador", "Fortaleza"],
        "Centro-Oeste": ["Brasília", "Goiânia", "Cuiabá"],
        "Sudeste": ["São Paulo", "Rio de Janeiro", "Belo Horizonte"],
        "Sul": ["Curitiba", "Florianópolis", "Porto Alegre"]
    }

    especies_aves = [
        "Gallus gallus domesticus", "Meleagris gallopavo", "Anas platyrhynchos",
        "Cygnus olor", "Buteo jamaicensis"
    ]

    tipos_aves = list(BirdType)
    niveis_confianca = list(ConfidenceLevel)
    fontes = ["IBAMA", "Ministério da Agricultura", "Vigilância Sanitária"]

    casos = []
    agora = datetime.now()

    for _ in range(quantidade):
        regiao = random.choice(regioes)
        cidade = random.choice(cidades[regiao])

        # Coordenadas fictícias
        lat = random.uniform(-33.0, 5.0)
        lon = random.uniform(-73.0, -34.0)

        caso = AvianFluCase(
            case_id=str(uuid.uuid4()),
            detection_date=agora - timedelta(days=random.randint(1, 60)),
            location=Location(
                region=regiao,
                city=cidade,
                latitude=lat,
                longitude=lon
            ),
            bird=BirdInfo(
                species=random.choice(especies_aves),
                type=random.choice(tipos_aves),
                population_size=random.randint(1, 100) if random.random() > 0.5 else None
            ),
            metadata=Metadata(
                source=random.choice(fontes),
                confidence=random.choice(niveis_confianca),
                collected_at=agora - timedelta(hours=random.randint(1, 24)),
                is_verified=random.random() > 0.3
            )
        )
        casos.append(caso)

    return casos

# Função principal de teste
def testar_pipeline():
    print("=== TESTE COMPLETO DO PIPELINE DE DADOS ===")

    # 1. Gerar dados de teste
    print("\n[1] Gerando casos de teste...")
    casos_teste = gerar_casos_teste(15)
    print(f"✓ Gerados {len(casos_teste)} casos fictícios")

    # 2. Configurar e inicializar MinIO
    print("\n[2] Inicializando MinIO...")
    minio_client = MinioClient(MinIO(endpoint="localhost:9000"))
    minio_client.initialize_buckets()
    print("✓ MinIO inicializado")

    # 3. Armazenar casos no MinIO
    print("\n[3] Armazenando casos no MinIO...")
    storage_key = minio_client.upload_avian_flu_case(casos_teste)
    print(f"✓ Casos armazenados em: {storage_key.bucket.value}/{storage_key.full_path}")

    # 4. Configurar DuckDB
    print("\n[4] Inicializando DuckDB...")
    duckdb_client = DuckDBClient(database_path=":memory:")  # Banco em memória para teste
    duckdb_client.initialize_schema()
    print("✓ Esquema DuckDB inicializado")

    # 5. Testar leitura direta do MinIO
    print("\n[5] Testando leitura direta do MinIO com DuckDB...")
    df_direto = duckdb_client.execute_query(
        f"SELECT * FROM read_parquet('s3://{storage_key.bucket.value}/{storage_key.full_path}')"
    )
    print(f"✓ Leitura direta: {len(df_direto)} registros")
    print("\nAmostra de dados lidos diretamente:")
    print(df_direto[["case_id", "region", "city", "bird_species"]].head(3))

    # 6. Sincronizar dados para tabela local
    print("\n[6] Sincronizando dados do MinIO para tabela local...")
    registros_adicionados = duckdb_client.synchronize_from_minio(
        storage_key.bucket.value,
        f"{storage_key.category}/",
        TableName.AVIAN_CASES
    )
    print(f"✓ Sincronizados {registros_adicionados} registros")

    # 7. Testar views analíticas
    print("\n[7] Consultando views analíticas...")

    # Casos por região
    regioes_df = duckdb_client.get_view_data(TableName.CASES_BY_REGION)
    print("\nCasos por região:")
    print(regioes_df[["region", "case_count", "first_detection", "last_detection"]].head(3))

    # Casos por tipo de ave
    tipos_df = duckdb_client.get_view_data(TableName.CASES_BY_BIRD_TYPE)
    print("\nCasos por tipo de ave:")
    print(tipos_df[["bird_type", "bird_species", "case_count", "affected_regions"]].head(3))

    # 8. Demonstrar pré-processamento para ML
    print("\n[8] Demonstrando pré-processamento para ML...")

    # Agregação temporal
    print("\nAgregação temporal (casos por semana):")
    agregado_semanal = duckdb_client.execute_query("""
        SELECT 
            DATE_TRUNC('week', detection_date) as semana,
            COUNT(*) as total_casos,
            COUNT(DISTINCT region) as total_regioes
        FROM avian_cases
        GROUP BY 1
        ORDER BY 1
    """)
    print(agregado_semanal)

    # Transformação para ML
    print("\nPré-processamento de dados para ML:")
    dados_ml = duckdb_client.execute_query("""
        -- Normalização e codificação de features
        SELECT 
            case_id,
            -- Normalizar coordenadas
            (latitude + 33.0) / 38.0 as lat_norm,
            (longitude + 73.0) / 39.0 as long_norm,
            -- One-hot encoding para região
            CASE WHEN region = 'Norte' THEN 1 ELSE 0 END as region_norte,
            CASE WHEN region = 'Nordeste' THEN 1 ELSE 0 END as region_nordeste,
            CASE WHEN region = 'Centro-Oeste' THEN 1 ELSE 0 END as region_centro,
            CASE WHEN region = 'Sudeste' THEN 1 ELSE 0 END as region_sudeste,
            CASE WHEN region = 'Sul' THEN 1 ELSE 0 END as region_sul,
            -- Codificação numérica para tipo de ave
            CASE 
                WHEN bird_type = 'domestic' THEN 0
                WHEN bird_type = 'wild' THEN 1
                WHEN bird_type = 'migratory' THEN 2
                ELSE 3
            END as bird_type_code,
            -- Target variable
            is_verified as target
        FROM avian_cases
    """)
    print(dados_ml.head(3))

    # Exportação downstream (simulada)
    print("\nExportação para processamento downstream:")
    print("✓ Preparando payload para API de processamento")

    # Exporte apenas aves domésticas como exemplo
    domestic_birds = duckdb_client.execute_query("""
        SELECT 
            case_id, detection_date, region, city, 
            bird_species, confidence_level, is_verified
        FROM avian_cases
        WHERE bird_type = 'domestic'
    """)
    print(f"✓ {len(domestic_birds)} casos de aves domésticas prontos para envio")

    print("\n=== TESTE CONCLUÍDO COM SUCESSO ===")
    duckdb_client.close()

if __name__ == "__main__":
    testar_pipeline()