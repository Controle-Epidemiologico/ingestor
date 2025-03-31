FROM python:3.12-slim AS builder

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    clang \
    rustc \
    cargo \
    cmake \
    git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /tmp
RUN git clone --depth 1 https://github.com/abseil/abseil-cpp.git \
    && cd abseil-cpp \
    && cmake -DCMAKE_POSITION_INDEPENDENT_CODE=ON -DCMAKE_BUILD_TYPE=Release . \
    && make -j$(nproc) \
    && make install \
    && cd .. \
    && rm -rf abseil-cpp

RUN pip install --no-cache-dir -U pip setuptools wheel \
    && pip install --no-cache-dir pdm

WORKDIR /app

COPY pyproject.toml pdm.lock* ./
RUN mkdir -p __pypackages__

RUN pdm install --prod --no-self

COPY ./ ./

FROM python:3.12-slim AS prod

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app:/app/pkgs

RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq-dev \
    curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

RUN groupadd -r appuser && useradd -r -g appuser appuser

WORKDIR /app

COPY --from=builder /app/__pypackages__/3.12/lib pkgs/
COPY --from=builder /app/./ ./

RUN chown -R appuser:appuser /app

USER appuser

HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

CMD ["python", "main"]
