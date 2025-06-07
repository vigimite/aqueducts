# Execution

Aqueducts supports both local and remote pipeline execution. Use the CLI for local execution or submit jobs to remote executors running in your infrastructure.

## Local Execution

Execute pipelines directly on your local machine using the Aqueducts CLI.

### Basic Usage

```bash
# Run a pipeline
aqueducts run --file pipeline.yml

# Run with parameters
aqueducts run --file pipeline.yml --param key1=value1 --param key2=value2

# Multiple parameters
aqueducts run --file pipeline.yml --params key1=value1 --params key2=value2
```

### Supported Formats

The CLI supports multiple pipeline definition formats:

=== "YAML"

    ```bash
    aqueducts run --file pipeline.yml
    ```

=== "JSON"

    ```bash
    aqueducts run --file pipeline.json
    ```

=== "TOML"

    ```bash
    aqueducts run --file pipeline.toml
    ```

!!! note "Format Support"
    JSON and TOML support require appropriate feature flags during CLI installation.

### Local Execution Benefits

- **Direct access** to local files and databases
- **No network overhead** for file-based operations
- **Immediate feedback** and debugging
- **Full control** over execution environment

## Remote Execution

Execute pipelines on remote executors deployed within your infrastructure, closer to your data sources.

### Architecture Overview

Remote execution follows this flow:

1. **CLI Client** submits pipeline to **Remote Executor**
2. **Remote Executor** processes data from **Data Sources** 
3. **Remote Executor** writes results to **Destinations**
4. **Remote Executor** sends status updates back to **CLI Client**

### Setting Up Remote Execution

1. **Deploy an Executor**:

    ```bash
    # Start executor with API key
    aqueducts-executor --api-key your_secret_key --max-memory 4
    ```

2. **Submit Pipeline from CLI**:

    ```bash
    # Execute remotely
    aqueducts run --file pipeline.yml \
      --executor executor-host:3031 \
      --api-key your_secret_key \
      --param environment=prod
    ```

### Remote Execution Benefits

- **Minimize network transfer** by processing data close to sources
- **Scale processing power** independently of client machines
- **Secure execution** within your infrastructure boundaries
- **Centralized resource management** with memory limits

### Managing Remote Executions

Monitor and control remote pipeline executions:

```bash
# Check executor status
curl http://executor-host:3031/api/health

# Cancel a running pipeline
aqueducts cancel --executor executor-host:3031 \
  --api-key your_secret_key \
  --execution-id abc-123
```

---

## Aqueducts Executor

The Aqueducts Executor is a deployable application for running pipelines within your infrastructure.

### Key Features

- **Remote Execution**: Run data pipelines securely within your infrastructure
- **Memory Management**: Configure maximum memory usage with DataFusion's memory pool
- **Real-time Feedback**: WebSocket communication for live progress updates
- **Cloud Storage Support**: Native S3, GCS, and Azure Blob Storage integration
- **Database Connectivity**: ODBC support for various database systems
- **Exclusive Execution**: Single-pipeline execution for optimal resource utilization

### Docker Deployment (Recommended)

The Docker image includes ODBC support with PostgreSQL drivers pre-installed:

```bash
# Pull from GitHub Container Registry
docker pull ghcr.io/vigimite/aqueducts/aqueducts-executor:latest

# Run with command line arguments
docker run -d \
  --name aqueducts-executor \
  -p 3031:3031 \
  ghcr.io/vigimite/aqueducts/aqueducts-executor:latest \
  --api-key your_secret_key --max-memory 4
```

### Environment Variables

Configure the executor using environment variables:

```bash
docker run -d \
  --name aqueducts-executor \
  -p 3031:3031 \
  -e AQUEDUCTS_API_KEY=your_secret_key \
  -e AQUEDUCTS_HOST=0.0.0.0 \
  -e AQUEDUCTS_PORT=3031 \
  -e AQUEDUCTS_MAX_MEMORY=4 \
  -e AQUEDUCTS_LOG_LEVEL=info \
  ghcr.io/vigimite/aqueducts/aqueducts-executor:latest
```

### Configuration Options

| Option          | Description                                         | Default        | Environment Variable    |
|-----------------|-----------------------------------------------------|----------------|-------------------------|
| `--api-key`     | API key for authentication                          | -              | `AQUEDUCTS_API_KEY`     |
| `--host`        | Host address to bind to                             | 0.0.0.0        | `AQUEDUCTS_HOST`        |
| `--port`        | Port to listen on                                   | 8080           | `AQUEDUCTS_PORT`        |
| `--max-memory`  | Maximum memory usage in GB (0 for unlimited)        | 0              | `AQUEDUCTS_MAX_MEMORY`  |
| `--server-url`  | URL of Aqueducts server for registration (optional) | -              | `AQUEDUCTS_SERVER_URL`  |
| `--executor-id` | Unique identifier for this executor                 | auto-generated | `AQUEDUCTS_EXECUTOR_ID` |
| `--log-level`   | Logging level (info, debug, trace)                  | info           | `AQUEDUCTS_LOG_LEVEL`   |

### Docker Compose Setup

For local development and testing:

```bash
# Start database only (default)
docker-compose up

# Start database + executor
docker-compose --profile executor up

# Build and start from source
docker-compose --profile executor up --build
```

The executor will be available at:
- **API**: `http://localhost:3031`
- **Health check**: `http://localhost:3031/api/health`
- **WebSocket**: `ws://localhost:3031/ws/connect`

### Manual Installation

Install using Cargo for custom deployments:

```bash
# Standard installation with cloud storage features
cargo install aqueducts-executor

# Installation with ODBC support
cargo install aqueducts-executor --features odbc
```

### API Endpoints

| Endpoint       | Method | Auth | Description                                        |
|----------------|--------|------|----------------------------------------------------|
| `/api/health`  | GET    | No   | Basic health check                                 |
| `/ws/connect`  | GET    | Yes  | WebSocket endpoint for bidirectional communication |

## ODBC Configuration

For database connectivity, ODBC support requires additional system dependencies.

### System Requirements

=== "Ubuntu/Debian"

    ```bash
    # Install UnixODBC and drivers
    sudo apt-get update
    sudo apt-get install unixodbc-dev

    # Database-specific drivers
    sudo apt-get install odbc-postgresql  # PostgreSQL
    sudo apt-get install libmyodbc        # MySQL
    ```

=== "Fedora/RHEL/CentOS"

    ```bash
    # Install UnixODBC and drivers
    sudo dnf install unixODBC-devel

    # Database-specific drivers
    sudo dnf install postgresql-odbc      # PostgreSQL
    sudo dnf install mysql-connector-odbc # MySQL
    ```

=== "macOS"

    ```bash
    # Install via Homebrew
    brew install unixodbc

    # Database drivers
    brew install psqlodbc              # PostgreSQL
    brew install mysql-connector-c++   # MySQL
    ```

### Testing ODBC Connections

Verify your ODBC setup before using in pipelines:

```bash
# Test connection with isql
isql -v YOUR_DSN YOUR_USERNAME YOUR_PASSWORD
```

## Troubleshooting

### Common Issues

**Local Execution:**
- **Pipeline validation errors**: Check YAML syntax and schema compliance
- **Missing features**: Ensure CLI was compiled with required feature flags
- **File not found**: Verify file paths and permissions

**Remote Execution:**
- **Connection timeouts**: Check network connectivity and firewall rules
- **Authentication failures**: Verify API key configuration
- **Executor busy**: Only one pipeline runs at a time per executor
- **Memory errors**: Increase `--max-memory` or optimize pipeline queries

**ODBC Issues:**
- **Driver not found**: Install database-specific ODBC drivers
- **Connection failures**: Verify DSN configuration in `odbc.ini`
- **Permission errors**: Check database credentials and network access

### Performance Optimization

!!! tip "Memory Management"
    - Set appropriate `--max-memory` limits for executors
    - Break large queries into smaller stages
    - Add filtering early in the pipeline
    - Use partitioning for large datasets

!!! tip "Network Optimization"
    - Deploy executors close to data sources
    - Use cloud storage in the same region as executors
    - Minimize data movement between stages