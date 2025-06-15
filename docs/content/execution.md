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

The executor supports two operation modes:

#### Standalone Mode (Direct CLI Connection)

1. **Deploy a Standalone Executor**:

    ```bash
    # Start executor in standalone mode (default)
    aqueducts-executor --mode standalone --api-key your_secret_key --max-memory 4
    ```

2. **Submit Pipeline from CLI**:

    ```bash
    # Execute remotely via direct connection
    aqueducts run --file pipeline.yml \
      --executor executor-host:3031 \
      --api-key your_secret_key \
      --param environment=prod
    ```

#### Managed Mode (Orchestrator-Managed)

1. **Deploy a Managed Executor**:

    ```bash
    # Start executor in managed mode
    aqueducts-executor --mode managed \
      --orchestrator-url ws://orchestrator:3000/ws/executor/register \
      --api-key orchestrator_shared_secret --max-memory 4
    ```

2. **Submit Pipeline via Orchestrator**:
    - Use the orchestrator web interface
    - Or submit via orchestrator API endpoints
    - CLI connects to orchestrator, not directly to executor

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

# Run in standalone mode (default)
docker run -d \
  --name aqueducts-executor \
  -p 3031:3031 \
  ghcr.io/vigimite/aqueducts/aqueducts-executor:latest \
  --mode standalone --api-key your_secret_key --max-memory 4

# Run in managed mode
docker run -d \
  --name aqueducts-executor \
  ghcr.io/vigimite/aqueducts/aqueducts-executor:latest \
  --mode managed \
  --orchestrator-url ws://orchestrator:3000/ws/executor/register \
  --api-key orchestrator_shared_secret --max-memory 4
```

### Environment Variables

Configure the executor using environment variables:

```bash
# Standalone mode
docker run -d \
  --name aqueducts-executor \
  -p 3031:3031 \
  -e EXECUTOR_MODE=standalone \
  -e AQUEDUCTS_API_KEY=your_secret_key \
  -e AQUEDUCTS_HOST=0.0.0.0 \
  -e AQUEDUCTS_PORT=3031 \
  -e AQUEDUCTS_MAX_MEMORY=4 \
  -e AQUEDUCTS_LOG_LEVEL=info \
  ghcr.io/vigimite/aqueducts/aqueducts-executor:latest

# Managed mode
docker run -d \
  --name aqueducts-executor \
  -e EXECUTOR_MODE=managed \
  -e ORCHESTRATOR_URL=ws://orchestrator:3000/ws/executor/register \
  -e AQUEDUCTS_API_KEY=orchestrator_shared_secret \
  -e AQUEDUCTS_MAX_MEMORY=4 \
  -e AQUEDUCTS_LOG_LEVEL=info \
  ghcr.io/vigimite/aqueducts/aqueducts-executor:latest
```

### Executor Operation Modes

#### Standalone Mode

```bash
# Start in standalone mode (default)
aqueducts-executor --mode standalone --api-key your_secret_key

# Environment variables
EXECUTOR_MODE=standalone \
AQUEDUCTS_API_KEY=your_secret_key \
aqueducts-executor
```

**Use Cases:**
- Direct CLI-to-executor connections
- Development and testing environments
- Simple remote execution setups

#### Managed Mode

```bash
# Start in managed mode
aqueducts-executor --mode managed \
  --orchestrator-url ws://orchestrator:3000/ws/executor/register \
  --api-key orchestrator_shared_secret

# Environment variables
EXECUTOR_MODE=managed \
ORCHESTRATOR_URL=ws://orchestrator:3000/ws/executor/register \
AQUEDUCTS_API_KEY=orchestrator_shared_secret \
aqueducts-executor
```

**Use Cases:**
- Production deployments with centralized management
- Auto-scaling executor pools
- Scheduled and event-driven pipeline execution
- Enterprise environments requiring centralized control

### Configuration Options

| Option              | Description                                         | Default     | Environment Variable       |
|---------------------|-----------------------------------------------------|-------------|----------------------------|
| `--mode`            | Operation mode (standalone or managed)             | standalone  | `EXECUTOR_MODE`            |
| `--api-key`         | API key (executor's own or orchestrator's)         | -           | `AQUEDUCTS_API_KEY`        |
| `--orchestrator-url`| Orchestrator WebSocket URL (managed mode only)     | -           | `ORCHESTRATOR_URL`         |
| `--host`            | Host address to bind to                             | 0.0.0.0     | `AQUEDUCTS_HOST`           |
| `--port`            | Port to listen on                                   | 3031        | `AQUEDUCTS_PORT`           |
| `--max-memory`      | Maximum memory usage in GB (0 for unlimited)       | 0           | `AQUEDUCTS_MAX_MEMORY`     |
| `--executor-id`     | Unique identifier for this executor                 | auto-gen    | `AQUEDUCTS_EXECUTOR_ID`    |
| `--log-level`       | Logging level (info, debug, trace)                  | info        | `AQUEDUCTS_LOG_LEVEL`      |

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

For database connectivity, ODBC support requires the `odbc` feature flag during installation and proper system configuration.

### Installation Requirements

First, install Aqueducts with ODBC support:

```bash
# CLI with ODBC support
cargo install aqueducts-cli --features odbc

# Executor with ODBC support  
cargo install aqueducts-executor --features odbc
```

### System Dependencies

=== "Ubuntu/Debian"

    ```bash
    # Install UnixODBC development libraries
    sudo apt-get update
    sudo apt-get install unixodbc-dev

    # PostgreSQL driver
    sudo apt-get install odbc-postgresql

    # MySQL driver
    sudo apt-get install libmyodbc

    # SQL Server driver (optional)
    curl https://packages.microsoft.com/keys/microsoft.asc | sudo apt-key add -
    curl https://packages.microsoft.com/config/ubuntu/20.04/prod.list | sudo tee /etc/apt/sources.list.d/msprod.list
    sudo apt-get update
    sudo apt-get install msodbcsql17
    ```

=== "Fedora/RHEL/CentOS"

    ```bash
    # Install UnixODBC development libraries
    sudo dnf install unixODBC-devel

    # PostgreSQL driver
    sudo dnf install postgresql-odbc

    # MySQL driver
    sudo dnf install mysql-connector-odbc

    # SQL Server driver (optional)
    sudo curl -o /etc/yum.repos.d/msprod.repo https://packages.microsoft.com/config/rhel/8/prod.repo
    sudo dnf install msodbcsql17
    ```

=== "macOS"

    ```bash
    # Install UnixODBC via Homebrew
    brew install unixodbc

    # PostgreSQL driver
    brew install psqlodbc

    # MySQL driver
    brew install mysql-connector-c++

    # SQL Server driver (optional)
    brew tap microsoft/mssql-release https://github.com/Microsoft/homebrew-mssql-release
    brew install msodbcsql17
    ```

### Driver Configuration

=== "PostgreSQL"

    Edit `/etc/odbcinst.ini` (system) or `~/.odbcinst.ini` (user):

    **Linux:**
    ```ini
    [PostgreSQL]
    Description=PostgreSQL ODBC driver
    Driver=/usr/lib/x86_64-linux-gnu/odbc/psqlodbcw.so
    Setup=/usr/lib/x86_64-linux-gnu/odbc/libodbcpsqlS.so
    FileUsage=1
    ```

    **macOS:**
    ```ini
    [PostgreSQL]
    Description=PostgreSQL ODBC driver
    Driver=/opt/homebrew/lib/psqlodbcw.so
    Setup=/opt/homebrew/lib/libodbcpsqlS.so
    FileUsage=1
    ```

=== "MySQL"

    Edit `/etc/odbcinst.ini` (system) or `~/.odbcinst.ini` (user):

    **Linux:**
    ```ini
    [MySQL]
    Description=MySQL ODBC driver
    Driver=/usr/lib/x86_64-linux-gnu/odbc/libmyodbc8w.so
    FileUsage=1
    ```

    **macOS:**
    ```ini
    [MySQL]
    Description=MySQL ODBC driver
    Driver=/opt/homebrew/lib/libmyodbc8w.so
    FileUsage=1
    ```

### Data Source Configuration

Configure database connections in `/etc/odbc.ini` (system) or `~/.odbc.ini` (user):

=== "PostgreSQL DSN"

    ```ini
    [PostgreSQL-Local]
    Description=Local PostgreSQL Database
    Driver=PostgreSQL
    Server=localhost
    Port=5432
    Database=mydb
    UserName=myuser
    Password=mypass
    ReadOnly=no
    ServerType=postgres
    ConnSettings=
    ```

=== "MySQL DSN"

    ```ini
    [MySQL-Local]
    Description=Local MySQL Database  
    Driver=MySQL
    Server=localhost
    Port=3306
    Database=mydb
    User=myuser
    Password=mypass
    ```

### Connection String Examples

=== "PostgreSQL"

    ```yaml
    # Using DSN
    sources:
      - type: odbc
        name: postgres_data
        connection_string: "DSN=PostgreSQL-Local"
        load_query: "SELECT * FROM users WHERE created_at > '2024-01-01'"

    # Direct connection string
    sources:
      - type: odbc
        name: postgres_data
        connection_string: "Driver={PostgreSQL};Server=localhost;Database=mydb;UID=user;PWD=pass;"
        load_query: "SELECT * FROM users LIMIT 1000"
    ```

=== "MySQL"

    ```yaml
    # Using DSN
    sources:
      - type: odbc
        name: mysql_data
        connection_string: "DSN=MySQL-Local"
        load_query: "SELECT * FROM products WHERE price > 100"

    # Direct connection string
    sources:
      - type: odbc
        name: mysql_data
        connection_string: "Driver={MySQL};Server=localhost;Database=mydb;User=user;Password=pass;"
        load_query: "SELECT * FROM orders WHERE date >= '2024-01-01'"
    ```

### Testing Your Setup

#### 1. Test ODBC Installation

```bash
# Check installed drivers
odbcinst -q -d

# Check configured data sources
odbcinst -q -s
```

#### 2. Test Database Connection

```bash
# Test with isql (interactive SQL)
isql -v PostgreSQL-Local username password

# Test MySQL connection
isql -v MySQL-Local username password
```

#### 3. Test with Aqueducts

Create a minimal test pipeline:

```yaml
# yaml-language-server: $schema=https://raw.githubusercontent.com/vigimite/aqueducts/main/json_schema/aqueducts.schema.json

version: "v2"
sources:
  - type: odbc
    name: test_connection
    connection_string: "DSN=PostgreSQL-Local"
    load_query: "SELECT 1 as test_column"

stages:
  - - name: verify
      query: "SELECT * FROM test_connection"
      show: 1
```

Run the test:

```bash
aqueducts run --file test-odbc.yml
```

### Common Driver Paths

=== "Linux"

    ```bash
    # PostgreSQL
    /usr/lib/x86_64-linux-gnu/odbc/psqlodbcw.so

    # MySQL  
    /usr/lib/x86_64-linux-gnu/odbc/libmyodbc8w.so

    # Find drivers
    find /usr -name "*odbc*.so" 2>/dev/null
    ```

=== "macOS"

    ```bash
    # PostgreSQL
    /opt/homebrew/lib/psqlodbcw.so

    # MySQL
    /opt/homebrew/lib/libmyodbc8w.so

    # Find drivers  
    find /opt/homebrew -name "*odbc*.so" 2>/dev/null
    ```

### Performance Considerations

!!! tip "Optimization Tips"
    - **Limit query results**: Use `LIMIT` clauses to avoid memory issues
    - **Filter early**: Apply `WHERE` conditions in your `load_query`
    - **Use indexes**: Ensure your database queries use appropriate indexes
    - **Memory management**: Set executor `--max-memory` limits appropriately

### ODBC Troubleshooting

#### Driver Loading Issues

```bash
# Check if drivers are registered
odbcinst -q -d

# Test driver loading
ldd /path/to/driver.so  # Linux
otool -L /path/to/driver.so  # macOS
```

#### Connection Issues

```bash
# Enable ODBC tracing for debugging
export ODBCSYSINI=/tmp
export ODBCINSTINI=/etc/odbcinst.ini
export ODBCINI=/etc/odbc.ini

# Test with verbose output
isql -v DSN_NAME username password
```

#### Common Error Solutions

- **Driver not found**: Verify driver paths in `odbcinst.ini`
- **DSN not found**: Check `odbc.ini` configuration
- **Permission denied**: Ensure ODBC files are readable
- **Library loading**: Install missing system dependencies

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

