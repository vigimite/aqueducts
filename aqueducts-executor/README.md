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

### Manual Installation

Install the application using cargo:

```bash
# Standard installation with all cloud storage features
cargo install aqueducts-executor

# Installation with ODBC support
cargo install aqueducts-executor --features odbc
```

## Configuration Options

| Option          | Description                                         | Default        | Environment Variable    |
|-----------------|-----------------------------------------------------|----------------|-------------------------|
| `--api-key`     | API key for authentication                          | -              | `AQUEDUCTS_API_KEY`     |
| `--host`        | Host address to bind to                             | 0.0.0.0        | `AQUEDUCTS_HOST`        |
| `--port`        | Port to listen on                                   | 8080           | `AQUEDUCTS_PORT`        |
| `--max-memory`  | Maximum memory usage in GB (0 for unlimited)        | 0              | `AQUEDUCTS_MAX_MEMORY`  |
| `--server-url`  | URL of Aqueducts server for registration (optional) | -              | `AQUEDUCTS_SERVER_URL`  |
| `--executor-id` | Unique identifier for this executor                 | auto-generated | `AQUEDUCTS_EXECUTOR_ID` |
| `--log-level`   | Logging level (info, debug, trace)                  | info           | `AQUEDUCTS_LOG_LEVEL`   |

## API Endpoints

| Endpoint    | Method | Auth | Description                                        |
|-------------|--------|------|----------------------------------------------------|
| `/health`   | GET    | No   | Basic health check                                 |
| `/ws`       | GET    | Yes  | WebSocket endpoint for bidirectional communication |

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

For more information on architecture and advanced usage, see the [Aqueducts Architecture Documentation](../ARCHITECTURE.md).
