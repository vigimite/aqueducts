FROM rust:1.86-slim AS builder

RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    unixodbc-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY Cargo.toml Cargo.lock ./
COPY aqueducts/ ./aqueducts/
COPY aqueducts-cli/ ./aqueducts-cli/
COPY aqueducts-executor/ ./aqueducts-executor/

RUN cargo build --release --features odbc -p aqueducts-executor

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y \
    ca-certificates \
    unixodbc \
    odbc-postgresql \
    curl \
    && rm -rf /var/lib/apt/lists/*

RUN useradd --create-home --shell /bin/bash aqueducts

COPY --from=builder /app/target/release/aqueducts-executor /usr/local/bin/aqueducts-executor

# Copy ODBC configuration files
COPY docker/odbcinst.ini /etc/odbcinst.ini
COPY docker/odbc.ini /etc/odbc.ini

RUN chmod +x /usr/local/bin/aqueducts-executor

USER aqueducts
WORKDIR /home/aqueducts

EXPOSE 3031

HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
  CMD curl -f http://localhost:3031/api/health || exit 1

CMD ["aqueducts-executor", "--host", "0.0.0.0", "--port", "3031"]
