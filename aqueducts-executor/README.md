# Aqueducts Executor

A deployable application used to execute Aqueduct pipeline definitions within your infrastructure. The main use-case is to execute heavy queries within the infrastructure where the data is hosted, minimizing network load and removing the requirement for the client to have direct access to the data store.

## Features

- **Remote Execution**: Run data pipelines securely within your own infrastructure close to the data sources
- **Memory Management**: Configure maximum memory usage to control resource allocation using DataFusion's memory pool
- **Real-time Feedback**: WebSockets provide bidirectional communication with live progress and log updates
- **Cloud Storage Support**: Native integration with S3, GCS, and Azure Blob Storage
- **Database Connectivity**: ODBC support for connecting to various database systems
- **Scalability**: Deploy multiple executors across different regions as needed
- **Exclusive Execution**: Guaranteed single-pipeline execution to optimize resource utilization

## Installation

### Docker (Recommended)

The easiest way to run the executor is using Docker. The Docker image includes **ODBC support with PostgreSQL drivers pre-installed**, making it ready for database connectivity out of the box.

```bash
# Pull from GitHub Container Registry
docker pull ghcr.io/vigimite/aqueducts/aqueducts-executor:latest

# Run with command line arguments
docker run -d \
  --name aqueducts-executor \
  -p 3031:3031 \
  ghcr.io/vigimite/aqueducts/aqueducts-executor:latest \
  --api-key your_secret_key --max-memory 4

# Or run with environment variables
docker run -d \
  --name aqueducts-executor \
  -p 3031:3031 \
  -e API_KEY=your_secret_key \
  -e BIND_ADDRESS=0.0.0.0:3031 \
  -e MAX_MEMORY=4 \
  -e RUST_LOG=info \
  ghcr.io/vigimite/aqueducts/aqueducts-executor:latest
```

### Docker Compose

For local development, use the provided docker-compose setup:

```bash
# Start just the database (default)
docker-compose up

# Start database + executor
docker-compose --profile executor up

# Build and start from source
docker-compose --profile executor up --build
```

The executor will be available at `http://localhost:3031` with:
- API key: `test_secret_key` (configurable)
- Health check: `http://localhost:3031/api/health`
- WebSocket: `ws://localhost:3031/ws/connect`

### Manual Installation

Install the application using cargo:

```bash
# Standard installation with all cloud storage features
cargo install aqueducts-executor

# Installation with ODBC support
cargo install aqueducts-executor --features odbc
```

## Operation Modes

### Standalone Mode

In standalone mode, the executor accepts direct connections from CLI clients:

```bash
# Start in standalone mode (default)
aqueducts-executor --mode standalone --api-key your_secret_key

# Or using environment variables
EXECUTOR_MODE=standalone \
API_KEY=your_secret_key \
aqueducts-executor
```

**Use Cases:**
- Direct CLI-to-executor connections
- Development and testing environments
- Simple remote execution setups

### Managed Mode

In managed mode, the executor connects to an orchestrator and is fully managed:

```bash
# Start in managed mode
aqueducts-executor --mode managed \
  --orchestrator-url ws://orchestrator:3000/ws/executor/register \
  --api-key orchestrator_shared_secret

# Or using environment variables
EXECUTOR_MODE=managed \
ORCHESTRATOR_URL=ws://orchestrator:3000/ws/executor/register \
API_KEY=orchestrator_shared_secret \
aqueducts-executor
```

**Use Cases:**
- Production deployments with centralized management
- Auto-scaling executor pools
- Scheduled and event-driven pipeline execution
- Enterprise environments requiring centralized control

## Configuration Options

| Option              | Description                                    | Default      | Environment Variable |
|---------------------|------------------------------------------------|--------------|----------------------|
| `--mode`            | Operation mode (standalone or managed)         | standalone   | `EXECUTOR_MODE`      |
| `--api-key`         | API key (executor's own or orchestrator's)     | -            | `API_KEY`            |
| `--orchestrator-url`| Orchestrator WebSocket URL (managed mode only) | -            | `ORCHESTRATOR_URL`   |
| `--bind-address`    | Server bind address                            | 0.0.0.0:3031 | `BIND_ADDRESS`       |
| `--max-memory`      | Maximum memory usage in GB (0 for unlimited)   | 0            | `MAX_MEMORY`         |
| `--executor-id`     | Unique identifier for this executor            | auto-gen     | `EXECUTOR_ID`        |
| `--log-level`       | Logging level (info, debug, trace)             | info         | `RUST_LOG`           |

## API Endpoints

### Standalone Mode

| Endpoint       | Method | Auth | Description                                        |
|----------------|--------|------|----------------------------------------------------|
| `/api/health`  | GET    | No   | Basic health check                                 |
| `/ws/connect`  | GET    | Yes  | WebSocket endpoint for CLI client connections      |

### Managed Mode

| Endpoint       | Method | Auth | Description                                        |
|----------------|--------|------|----------------------------------------------------|
| `/api/health`  | GET    | No   | Basic health check (for orchestrator monitoring)   |

*Note: In managed mode, the executor does not accept direct client connections. All execution requests come through the orchestrator via persistent WebSocket connection.*

## ODBC Configuration Requirements

ODBC support requires the UnixODBC library to be installed on your system, along with any database-specific drivers.

### Ubuntu/Debian
```bash
# Install UnixODBC development libraries
sudo apt-get update
sudo apt-get install unixodbc-dev

# Add database-specific drivers (examples)
# For PostgreSQL
sudo apt-get install odbc-postgresql

# For MySQL
sudo apt-get install libmyodbc
```

### Fedora/RHEL/CentOS
```bash
# Install UnixODBC development libraries
sudo dnf install unixODBC-devel

# Add database-specific drivers (examples)
# For PostgreSQL
sudo dnf install postgresql-odbc

# For MySQL
sudo dnf install mysql-connector-odbc
```

### macOS
```bash
# Install UnixODBC via Homebrew
brew install unixodbc

# For database drivers, use Homebrew if available or download from the database vendor
# PostgreSQL example
brew install psqlodbc

# MySQL example
brew install mysql-connector-c++
```

## Example Usage

### Using the CLI

```bash
# Connect to the executor
aqueducts run --executor executor-host:3031 --api-key your_api_key --file pipeline.yml
```

## Troubleshooting

Common issues and solutions:

1. **Connection timeouts**: Check network connectivity and firewall rules
2. **Authentication failures**: Verify API key configuration and correct header usage (X-API-Key)
4. **Memory errors**: 
   - Increase max memory allocation with the `--max-memory` parameter
   - Optimize your pipeline by adding filtering earlier in the process
   - Break large queries into smaller stages with intermediate results
5. **ODBC issues**:
   - Verify your DSN configuration in `odbc.ini` and `odbcinst.ini`
   - Run `isql -v YOUR_DSN YOUR_USERNAME YOUR_PASSWORD` to test connections
   - Check that database-specific drivers are installed correctly

For more information on architecture and advanced usage, see the [Aqueducts Architecture Documentation](https://github.com/vigimite/aqueducts/blob/main/ARCHITECTURE.md).
