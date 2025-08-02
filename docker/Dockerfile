# syntax=docker/dockerfile:1
FROM lukemathwalker/cargo-chef:latest-rust-1.86-slim AS chef
WORKDIR /app

RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    unixodbc-dev \
    perl \
    make \
    gcc \
    && rm -rf /var/lib/apt/lists/*

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/app/target \
    cargo chef cook --release --recipe-path recipe.json --features odbc -p aqueducts-executor

COPY . .

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/app/target \
    cargo build --release --features odbc -p aqueducts-executor && \
    cp target/release/aqueducts-executor /tmp/aqueducts-executor

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y \
    ca-certificates \
    unixodbc \
    odbc-postgresql \
    curl \
    && rm -rf /var/lib/apt/lists/*

RUN useradd --create-home --shell /bin/bash aqueducts

COPY --from=builder /tmp/aqueducts-executor /usr/local/bin/aqueducts-executor

COPY docker/odbcinst.ini /etc/odbcinst.ini
COPY docker/odbc.ini /etc/odbc.ini

RUN chmod +x /usr/local/bin/aqueducts-executor

USER aqueducts
WORKDIR /home/aqueducts

EXPOSE 3031

HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
  CMD curl -f http://localhost:3031/api/health || exit 1

CMD ["aqueducts-executor", "--host", "0.0.0.0", "--port", "3031"]
