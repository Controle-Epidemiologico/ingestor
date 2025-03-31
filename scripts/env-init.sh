#!/bin/bash
set -e

PROJETO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="${PROJETO_DIR}/infrastructure/docker-compose.yml"
AIRFLOW_DIR="${PROJETO_DIR}/infrastructure/airflow"
LOGS_DIR="${AIRFLOW_DIR}/logs"
DAGS_DIR="${AIRFLOW_DIR}/dags"
PLUGINS_DIR="${AIRFLOW_DIR}/plugins"

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

MINIO_ROOT_USER="${MINIO_ROOT_USER:-minioadmin}"
MINIO_ROOT_PASSWORD="${MINIO_ROOT_PASSWORD:-minioadmin}"
POSTGRES_USER="${POSTGRES_USER:-airflow}"
POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-airflow}"
AIRFLOW_ADMIN_USER="${AIRFLOW_ADMIN_USER:-admin}"
AIRFLOW_ADMIN_PASSWORD="${AIRFLOW_ADMIN_PASSWORD:-admin}"

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
    if ! command -v docker &> /dev/null; then
        log_error "Docker não está instalado. Por favor, instale-o primeiro."
        exit 1
    fi
    
    if ! command -v docker compose &> /dev/null; then
        log_warning "Docker Compose V2 não encontrado, tentando o V1..."
        if ! command -v docker-compose &> /dev/null; then
            log_error "Docker Compose não está instalado. Por favor, instale-o primeiro."
            exit 1
        fi
        DOCKER_COMPOSE="docker-compose"
    else
        DOCKER_COMPOSE="docker compose"
    fi
}

check_environment() {
    log_info "Verificando ambiente..."
    
    if [ ! -f "${COMPOSE_FILE}" ]; then
        log_error "Arquivo docker-compose.yml não encontrado em ${COMPOSE_FILE}"
        exit 1
    fi
}

create_directories() {
    log_info "Criando diretórios necessários..."
    mkdir -p "${DAGS_DIR}" "${LOGS_DIR}" "${PLUGINS_DIR}"

    log_info "Ajustando permissões dos diretórios..."
    chmod -R 777 "${DAGS_DIR}" "${LOGS_DIR}" "${PLUGINS_DIR}"
}

start_postgres() {
    log_info "Iniciando banco de dados PostgreSQL..."
    ${DOCKER_COMPOSE} -f "${COMPOSE_FILE}" up -d postgres
    
    log_info "Aguardando PostgreSQL inicializar (10s)..."
    sleep 10

    log_info "Verificando conexão com PostgreSQL..."
    if ! ${DOCKER_COMPOSE} -f "${COMPOSE_FILE}" exec -T postgres pg_isready -U "${POSTGRES_USER}"; then
        log_warning "PostgreSQL ainda não está pronto. Aguardando mais 5 segundos..."
        sleep 5

        if ! ${DOCKER_COMPOSE} -f "${COMPOSE_FILE}" exec -T postgres pg_isready -U "${POSTGRES_USER}"; then
            log_error "Falha ao conectar ao PostgreSQL. Verifique os logs para mais informações."
            ${DOCKER_COMPOSE} -f "${COMPOSE_FILE}" logs postgres
            exit 1
        fi
    fi
}

initialize_airflow() {
    log_info "Inicializando banco de dados do Airflow..."
    ${DOCKER_COMPOSE} -f "${COMPOSE_FILE}" run --rm \
        -e AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://"${POSTGRES_USER}":"${POSTGRES_PASSWORD}"@postgres/airflow \
        airflow-webserver \
        airflow db init

    log_info "Criando usuário admin do Airflow..."
    ${DOCKER_COMPOSE} -f "${COMPOSE_FILE}" run --rm \
        -e AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://"${POSTGRES_USER}":"${POSTGRES_PASSWORD}"@postgres/airflow \
        airflow-webserver \
        airflow users create \
        --username "${AIRFLOW_ADMIN_USER}" \
        --password "${AIRFLOW_ADMIN_PASSWORD}" \
        --firstname Admin \
        --lastname User \
        --role Admin \
        --email admin@example.com
}

initialize_minio() {
    log_info "Iniciando MinIO..."
    ${DOCKER_COMPOSE} -f "${COMPOSE_FILE}" up -d minio

    log_info "Aguardando MinIO inicializar (10s)..."
    sleep 10

    # Obter o nome da rede Docker que o compose criou
    # shellcheck disable=SC2046
    NETWORK_NAME=$(docker inspect -f '{{range $key, $value := .NetworkSettings.Networks}}{{$key}}{{end}}' $(${DOCKER_COMPOSE} -f "${COMPOSE_FILE}" ps -q minio))

    log_info "Configurando buckets do MinIO na rede ${NETWORK_NAME}..."
    docker run --rm --network="${NETWORK_NAME}" \
        --entrypoint /bin/sh \
        minio/mc:latest \
        -c "mc alias set myminio http://minio:9000 ${MINIO_ROOT_USER} ${MINIO_ROOT_PASSWORD} && \
            mc mb --ignore-existing myminio/raw && \
            echo 'Buckets criados com sucesso!'"

    # shellcheck disable=SC2181
    if [ $? -ne 0 ]; then
        log_error "Falha ao configurar o MinIO. Verifique os logs para mais informações."
        ${DOCKER_COMPOSE} -f "${COMPOSE_FILE}" logs minio
        exit 1
    fi
}

start_all_services() {
    log_info "Iniciando todos os serviços..."
    ${DOCKER_COMPOSE} -f "${COMPOSE_FILE}" up -d
}

check_services_status() {
    log_info "Verificando status dos serviços..."
    ${DOCKER_COMPOSE} -f "${COMPOSE_FILE}" ps
}

# Função principal
main() {
    log_info "=== Inicializando ambiente Controle Epidemiológico ==="
    check_docker
    check_environment
    create_directories
    
    start_postgres
    initialize_airflow
    initialize_minio
    
    start_all_services
    check_services_status
    
    log_info "==== Inicialização concluída! ===="
    log_info "Airflow UI: http://localhost:8080"
    log_info "MinIO Console: http://localhost:9001"
    log_info "Usuário Airflow: ${AIRFLOW_ADMIN_USER} / Senha: ${AIRFLOW_ADMIN_PASSWORD}"
    log_info "Usuário MinIO: ${MINIO_ROOT_USER} / Senha: ${MINIO_ROOT_PASSWORD}"
}

main "$@"
