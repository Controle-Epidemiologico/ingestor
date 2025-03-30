#!/bin/bash
set -e

PROJETO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="${PROJETO_DIR}/docker/docker-compose.yml"
AIRFLOW_DIR="${PROJETO_DIR}/airflow"
LOGS_DIR="${AIRFLOW_DIR}/logs"
DAGS_DIR="${AIRFLOW_DIR}/dags"
PLUGINS_DIR="${AIRFLOW_DIR}/plugins"

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[AVISO]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERRO]${NC} $1"
}

check_docker() {
    if ! command -v docker &> /dev/null || ! command -v docker-compose &> /dev/null; then
        log_error "Docker e/ou Docker Compose não estão instalados. Por favor, instale-os primeiro."
        exit 1
    fi
}

log_info "=== Inicializando ambiente Airflow ==="
check_docker

log_info "Criando diretórios necessários..."
mkdir -p "${DAGS_DIR}" "${LOGS_DIR}" "${PLUGINS_DIR}"

log_info "Ajustando permissões dos diretórios..."
chmod -R 777 "${DAGS_DIR}" "${LOGS_DIR}" "${PLUGINS_DIR}"

log_info "Iniciando banco de dados PostgreSQL..."
docker-compose -f "${COMPOSE_FILE}" up -d postgres
log_info "Aguardando PostgreSQL inicializar (10s)..."
sleep 10

log_info "Verificando conexão com PostgreSQL..."
if ! docker-compose -f "${COMPOSE_FILE}" exec postgres pg_isready -U airflow; then
    log_warning "PostgreSQL ainda não está pronto. Aguardando mais 5 segundos..."
    sleep 5

    if ! docker-compose -f "${COMPOSE_FILE}" exec postgres pg_isready -U airflow; then
        log_error "Falha ao conectar ao PostgreSQL. Verifique os logs para mais informações."
        docker-compose -f "${COMPOSE_FILE}" logs postgres
        exit 1
    fi
fi

log_info "Inicializando banco de dados do Airflow..."
docker-compose -f "${COMPOSE_FILE}" run --rm \
    -e AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres/airflow \
    airflow-webserver \
    airflow db init

log_info "Criando usuário admin do Airflow..."
docker-compose -f "${COMPOSE_FILE}" run --rm \
    -e AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres/airflow \
    airflow-webserver \
    airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com

log_info "Iniciando todos os serviços..."
docker-compose -f "${COMPOSE_FILE}" up -d

log_info "Verificando status dos serviços..."
docker-compose -f "${COMPOSE_FILE}" ps

log_info "==== Inicialização concluída! ===="
log_info "Airflow UI: http://localhost:8080"
log_info "MinIO Console: http://localhost:9001"
log_info "Username: admin / Password: admin"

export PROJETO_DIR