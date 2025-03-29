FROM python:3.12-slim as builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    clang \
    rustc \
    cargo \
    cmake \
    git \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /tmp
RUN git clone https://github.com/abseil/abseil-cpp.git \
    && cd abseil-cpp \
    && cmake -DCMAKE_POSITION_INDEPENDENT_CODE=ON . \
    && make -j$(nproc) \
    && make install \
    && cd .. \
    && rm -rf abseil-cpp

RUN pip install -U pip setuptools wheel
RUN pip install --no-cache-dir pdm

WORKDIR /app

COPY pyproject.toml pdm.lock* ./
RUN mkdir -p __pypackages__

RUN pdm install --prod --no-self

COPY src/ src/

FROM python:3.12-slim as prod

RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

ENV PYTHONPATH=/app:/app/pkgs
WORKDIR /app

COPY --from=builder /app/__pypackages__/3.12/lib pkgs/
COPY --from=builder /app/src/ src/

CMD ["python", "-m", "src.main"]