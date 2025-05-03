# Aqueducts Executor

This is a deployable application that is used to execute Aqueduct pipeline definitions within your infrastructure.
The main use-case for this is to execute heavy queries within the infrastructure where the data is hosted, minimizing the network
load on the client side and additionally removing the requirement for the client to have direct access to the data store.

## Architecture

```mermaid
flowchart LR
  %% Internet (Client)
  subgraph Internet[Internet]
    direction TB
    WebClient["Browser<br/>(via WebUI)"]
    CliClient["Aqueducts CLI<br/>(CLI interface using YAML)"]
  end

  %% Server Network (Region A)
  subgraph RegionA["Server Network<br/>(Region A)"]
    direction TB
    WebServer["Web Server<br/>(Orchestrator)"]
  end

  %% Executor Network (Region B / Data VPC)
  subgraph RegionB["Executor Network<br/>(Region B / Data VPC)"]
    direction TB
    Executor["Executor<br/>(Deployed near data)"]
    Data["Data Sources<br/>(DBs, Object store)"]
  end

  %% Client ↔ Server
  WebClient <-- HTTPS/SSE --> WebServer

  %% Client ↔ Executor (direct CLI)
  CliClient <-- HTTPS/SSE --> Executor

  %% Server ↔ Executor
  WebServer <-- HTTPS/SSE --> Executor

  %% Data Processing
  Executor -- Queries/Writes --> Data
```

The executor can be registered to an Aqueducts server or connected to directly by the CLI. In either case an API key is required which can be configured
on the executor startup. Additionally configurable is a max memory pool that the executor can use.

Each execution is exclusive on the executor which means that only one Aqueduct can be run at a time. This is important due to the nature of the performance heavy
computations each pipeline can trigger.

Connecting to the executor is done via HTTPS. When connecting to the executor a Server-Sent Events channel is opened to communicate back to the client. This
facilitates outputting executor logs and progress to the client. An execution will be cancelled if the client disconnects, or manually cancels the execution.

## Features

- **Remote Execution**: Run data pipelines securely within your own infrastructure close to the data sources
- **Memory Management**: Configure maximum memory usage to control resource allocation
- **Security**: API key authentication ensures secure access to the executor
- **Real-time Feedback**: Server-Sent Events provide live progress and log updates to clients
- **Cloud Storage Support**: Native integration with S3, GCS, and Azure Blob Storage
- **Database Connectivity**: ODBC support for connecting to various database systems
- **Format Flexibility**: Process data in YAML, JSON, and TOML formats
- **Scalability**: Deploy multiple executors across different regions as needed
- **Exclusive Execution**: Guaranteed single-pipeline execution to optimize resource utilization

## Configuration Options

| Option          | Description                                         | Default        | Environment Variable    |
|-----------------|-----------------------------------------------------|----------------|-------------------------|
| `--api-key`     | API key for authentication                          | -              | `AQUEDUCTS_API_KEY`     |
| `--host`        | Host address to bind to                             | 0.0.0.0        | `AQUEDUCTS_HOST`        |
| `--port`        | Port to listen on                                   | 8080           | `AQUEDUCTS_PORT`        |
| `--max-memory`  | Maximum memory usage in GB                          | 4              | `AQUEDUCTS_MAX_MEMORY`  |
| `--server-url`  | URL of Aqueducts server for registration (optional) | -              | `AQUEDUCTS_SERVER_URL`  |
| `--executor-id` | Unique identifier for this executor                 | auto-generated | `AQUEDUCTS_EXECUTOR_ID` |
| `--log-level`   | Logging level (info, debug, trace)                  | info           | `AQUEDUCTS_LOG_LEVEL`   |

## API Reference

### Authentication

The executor uses API key authentication. The API key should be provided via the api key header:

**X-API-Key header**:
```
X-API-Key: your_api_key_here
```

### Endpoints

| Endpoint       | Method | Authentication | Description                                        |
|----------------|--------|----------------|----------------------------------------------------|
| `/health`      | GET    | No             | Basic health check to verify the service is running |
| `/status`      | GET    | Yes            | Get current executor status and execution details  |
| `/execute`     | POST   | Yes            | Execute a pipeline with real-time progress updates |
| `/cancel`      | POST   | Yes            | Cancel a running pipeline execution                |

### Pipeline Execution

To execute a pipeline, send a POST request to `/execute` with your pipeline definition in JSON format:

```json
{
  "pipeline": {
    "sources": [...],
    "stages": [...],
    "destination": {...}
  }
}
```

The executor expects a JSON object that matches the Aqueduct pipeline structure with `sources`, `stages`, and optional `destination` fields. All pipeline parameters should be pre-populated before submission as the executor does not support parameter substitution.

The `stages` field is an array of arrays, where the outer array represents sequential stage groups and the inner arrays represent parallel stages. The progress tracker uses both position (outer array index) and sub-position (inner array index) to track progress for each stage.

The response is a Server-Sent Events stream with real-time execution updates:

```json
{
  "event_type": "started",
  "message": "Pipeline execution started",
  "progress": 0,
  "execution_id": "unique-execution-id",
  "current_stage": null,
  "error": null
}

{
  "event_type": "progress",
  "message": "Processing stage: load_data (position: 0, sub-position: 0)",
  "progress": 30,
  "execution_id": "unique-execution-id",
  "current_stage": "load_data:0_0",
  "error": null
}

{
  "event_type": "completed",
  "message": "Pipeline execution completed successfully",
  "progress": 100,
  "execution_id": "unique-execution-id",
  "current_stage": null,
  "error": null
}
```

In case of errors, an error event is sent:

```json
{
  "event_type": "error",
  "message": "Pipeline execution failed",
  "progress": null,
  "execution_id": "unique-execution-id",
  "current_stage": "transform_data",
  "error": "Error details: Failed to execute SQL query"
}
```

### Pipeline Cancellation

To cancel a running pipeline execution, send a POST request to `/cancel`:

```json
{
  "execution_id": "unique-execution-id"  // Optional - if omitted, will cancel the current execution
}
```

The response will indicate whether the cancellation was successful:

```json
{
  "status": "cancelled",
  "message": "Pipeline execution cancelled successfully",
  "cancelled_execution_id": "unique-execution-id"
}
```

If no execution is running:

```json
{
  "status": "not_running",
  "message": "No pipeline execution is currently running",
  "cancelled_execution_id": null
}
```

If you provide an execution ID that doesn't match the currently running execution:

```json
{
  "status": "not_found",
  "message": "Execution ID 'wrong-id' not found. Current execution has ID 'actual-id'",
  "cancelled_execution_id": null
}
```

When a pipeline is cancelled, it will emit a special SSE event:

```json
{
  "event_type": "cancelled",
  "message": "Pipeline execution cancelled",
  "execution_id": "unique-execution-id"
}
```

### Concurrent Execution Handling

The executor processes only one pipeline at a time. If you attempt to execute a pipeline when one is already running:

1. You'll receive a `429 Too Many Requests` response
2. The response includes details about the currently running execution
3. A `Retry-After` header indicates when to retry (in seconds)
4. Standard rate limit headers (`X-RateLimit-*`) provide additional context

Example response:
```json
{
  "error": "A pipeline is already running (ID: abc-123). Please retry after 30 seconds",
  "execution_id": "abc-123",
  "retry_after": 30
}
```

## Deployment

### Manual deployment

Install the application using cargo install:

```bash
cargo install aqueducts-executor --features s3,gcs,azure,yaml,odbc
```

### Docker deployment

There is a pre-built docker image at `vigimite/aqueducts-executor` that can be used to deploy to anything that supports hosting docker containers.

Example usage:
```bash
docker run -p 8080:8080 \
  -e AQUEDUCTS_API_KEY=your_api_key \
  -e AQUEDUCTS_MAX_MEMORY=8 \
  vigimite/aqueducts-executor
```

### Helm deployment

An official helm chart can be found at `vigimite/aqueducts-helm-charts/aqueducts-executor`

### Environment Setup

For proper functionality, ensure the following:

1. For ODBC connections, appropriate drivers must be installed on the host system
2. For cloud storage access, proper credentials must be configured:
   - AWS: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
   - GCS: GOOGLE_APPLICATION_CREDENTIALS
   - Azure: AZURE_STORAGE_ACCOUNT, AZURE_STORAGE_KEY

## Health Checks

The executor provides health endpoints:
- GET `/health` - Basic health check

## Example Usage

When using the CLI to connect directly to an executor:

```bash
aqueducts run --executor http://executor-host:8080 --api-key your_api_key pipeline.yml
```

When registering with an Aqueducts server:

```bash
aqueducts-executor --server-url https://aqueducts-server:8000 --api-key server_api_key
```

Executing a pipeline with curl:

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

Checking executor status:

```bash
curl http://executor-host:8080/status -H "X-API-Key: your_api_key"
```

Cancelling a running pipeline:

```bash
curl -X POST http://executor-host:8080/cancel \
  -H "X-API-Key: your_api_key" \
  -H "Content-Type: application/json" \
  -d '{
    "execution_id": "unique-execution-id"  # Optional - omit to cancel the current execution
  }'
```

## Troubleshooting

Common issues and solutions:

1. **Connection timeouts**: Check network connectivity and firewall rules
2. **Authentication failures**: Verify API key configuration and correct header usage (X-API-Key)
3. **429 Too Many Requests**: An execution is already in progress; wait and retry after the suggested time
4. **Memory errors**: Increase max memory allocation or optimize pipeline
5. **Database connectivity**: Confirm ODBC drivers are properly installed
6. **Log location**: Logs are written to stdout/stderr and can be redirected as needed
