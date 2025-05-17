# Aqueducts Executor

A deployable application used to execute Aqueduct pipeline definitions within your infrastructure. The main use-case is to execute heavy queries within the infrastructure where the data is hosted, minimizing network load and removing the requirement for the client to have direct access to the data store.

## Features

- **Remote Execution**: Run data pipelines securely within your own infrastructure close to the data sources
- **Memory Management**: Configure maximum memory usage to control resource allocation using DataFusion's memory pool
- **Security**: API key authentication ensures secure access to the executor
- **Real-time Feedback**: WebSockets provide bidirectional communication with live progress and log updates
- **Cloud Storage Support**: Native integration with S3, GCS, and Azure Blob Storage
- **Database Connectivity**: ODBC support for connecting to various database systems
- **Format Flexibility**: Process data in YAML, JSON, and TOML formats
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

### Docker Deployment

Pre-built Docker images are available:

```bash
# Standard image
docker run -p 3031:3031 \
  -e AQUEDUCTS_API_KEY=your_api_key \
  -e AQUEDUCTS_MAX_MEMORY=8 \  # Allocate 8GB for query processing memory pool (0 for unlimited)
  vigimite/aqueducts-executor

# With ODBC support
docker run -p 3031:3031 \
  -e AQUEDUCTS_API_KEY=your_api_key \
  -e AQUEDUCTS_MAX_MEMORY=8 \
  -v /path/to/odbc.ini:/etc/odbc.ini \
  -v /path/to/odbcinst.ini:/etc/odbcinst.ini \
  vigimite/aqueducts-executor:odbc
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
| `/status`   | GET    | Yes  | Get executor status and execution details          |
| `/cancel`   | POST   | Yes  | Cancel a running pipeline execution                |
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
# Connect to WebSocket endpoint
aqueducts run --executor ws://executor-host:8080/ws --api-key your_api_key pipeline.yml
# OR 
aqueducts run --executor http://executor-host:8080 --api-key your_api_key pipeline.yml
```

### WebSocket Protocol

The WebSocket connection uses a typed message protocol:

1. **Connection**: Client connects to `/ws` endpoint with API key header
2. **Execution**: Client sends `execute_request` message with pipeline definition
3. **Updates**: Server sends progress updates, queue position updates, and output data
4. **Completion**: Server sends completion or error message
5. **Close**: Either side can close the connection

All messages are JSON objects with a `type` field indicating the message type.

### Using curl

```bash
curl -X POST http://executor-host:8080/execute \
  -H "X-API-Key: your_api_key" \
  -H "Content-Type: application/json" \
  -d '{
    "pipeline": {
      "sources": [
        {
          "type": "csv",
          "name": "source_data",
          "options": {
            "path": "s3://bucket/path/to/data.csv",
            "has_header": true
          }
        }
      ],
      "stages": [
        [
          {
            "name": "transformed_data",
            "query": "SELECT * FROM source_data WHERE value > 10"
          }
        ]
      ],
      "destination": {
        "type": "csv",
        "options": {
          "path": "s3://bucket/path/to/output/",
          "mode": "overwrite"
        }
      }
    }
  }'
```

## Troubleshooting

Common issues and solutions:

1. **Connection timeouts**: Check network connectivity and firewall rules
2. **Authentication failures**: Verify API key configuration and correct header usage (X-API-Key)
3. **429 Too Many Requests**: An execution is already in progress; wait and retry after the suggested time
4. **Memory errors**: 
   - Increase max memory allocation with the `--max-memory` parameter
   - Optimize your pipeline by adding filtering earlier in the process
   - Break large queries into smaller stages with intermediate results
5. **ODBC issues**:
   - Verify your DSN configuration in `odbc.ini` and `odbcinst.ini`
   - Run `isql -v YOUR_DSN YOUR_USERNAME YOUR_PASSWORD` to test connections
   - Check that database-specific drivers are installed correctly

For more information on architecture and advanced usage, see the [Aqueducts Architecture Documentation](../ARCHITECTURE.md).
