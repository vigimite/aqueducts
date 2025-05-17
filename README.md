# Aqueducts

[![Build status](https://github.com/vigimite/aqueducts/actions/workflows/build.yml/badge.svg?branch=main)](https://github.com/vigimite/aqueducts/actions/workflows/CI.yml) [![Crates.io](https://img.shields.io/crates/v/aqueducts)](https://crates.io/crates/aqueducts) [![Documentation](https://docs.rs/aqueducts/badge.svg)](https://docs.rs/aqueducts)

<img src="https://github.com/vigimite/aqueducts/raw/main/logo.png" width="100">

Aqueducts is a framework to write and execute ETL data pipelines declaratively.

**Features:**

- Define ETL pipelines in YAML, JSON, or TOML format
- Run pipelines locally or remotely with the CLI
- Process data using SQL with the power of DataFusion
- Extract data from CSV files, JSONL, Parquet files, or Delta tables
- Connect to databases via ODBC for both sources and destinations
- Load data into object stores as CSV/Parquet or Delta tables
- Support for file and Delta table partitioning
- Support for Upsert/Replace/Append operations on Delta tables
- Support for Local, S3, GCS, and Azure Blob storage
- Memory management for resource-intensive operations
- Real-time progress tracking for pipeline execution

## WebSocket Migration Plan

We're planning to migrate the Aqueducts executor from using Server-Sent Events (SSE) to WebSockets for real-time communication. Here's the migration plan:

### Tasks

- [x] 1. Add WebSocket dependencies to Cargo.toml
  - Add `tokio-tungstenite` for WebSocket support
  - Add `futures-util` for stream utilities

- [x] 2. Create core WebSocket types and message definitions
  - Define typed WebSocket message structures 
  - Implement message serialization/deserialization
  - Create a message queue using VecDeque

- [x] 3. Implement WebSocket server handler
  - Create WebSocket connection handler
  - Implement client authentication via WebSocket
  - Add connection management with per-client state

- [x] 4. Create execution queue management
  - Implement job queue using VecDeque
  - Add position tracking in the queue
  - Provide queue position updates to clients

- [x] 5. Refactor executor to use WebSockets
  - Update pipeline execution logic
  - Implement message dispatch to connected clients
  - Maintain backward compatibility during migration

- [x] 6. Modify CLI to use WebSocket client
  - Create WebSocket client implementation
  - Update remote execution logic for WebSockets
  - Handle reconnection and error scenarios

- [x] 7. Add tests for WebSocket implementation
  - Unit tests for message serialization
  - Integration tests for connection handling
  - End-to-end tests for pipeline execution

- [x] 8. Update documentation
  - Document new WebSocket protocol
  - Update API references
  - Add migration guide for client implementations

## Documentation

- [Full Documentation](https://vigimite.github.io/aqueducts)
- [Architecture Documentation](ARCHITECTURE.md)
- [Change Log](CHANGELOG.md)
- [Contributing Guide](CONTRIBUTING.md)

## Community

Join our Discord community to get help, share your work, and connect with other Aqueducts users:

[![Discord](https://img.shields.io/discord/1234567890?color=7289DA&label=Discord&logo=discord&logoColor=white)](https://discord.gg/astQZM3wqy)

## Installation

### CLI

Install the CLI to run pipelines locally or connect to remote executors:

```bash
# Install with default features (s3, gcs, azure, yaml)
cargo install aqueducts-cli --locked

# Install with odbc support
cargo install aqueducts-cli --locked --features odbc

# Install with minimal features
cargo install aqueducts-cli --locked --no-default-features --features yaml
```

### Executor

Install the executor to run pipelines remotely, closer to data sources:

```bash
# Install the executor (with default cloud storage support)
cargo install aqueducts-executor --locked

# For ODBC support (requires unixodbc-dev to be installed)
cargo install aqueducts-executor --locked --features odbc
```

## Quick Start

### Local Execution

Run a pipeline locally:

```bash
aqueducts run --file examples/aqueduct_pipeline_example.yml --param year=2024 --param month=jan
```

### Remote Execution

1. Start a remote executor:

```bash
# Run the executor with an API key
aqueducts-executor --api-key your_secret_key --max-memory 4
```

2. Execute a pipeline remotely:

```bash
aqueducts run --file examples/aqueduct_pipeline_example.yml \
  --param year=2024 --param month=jan \
  --executor http://executor-host:3031 \
  --api-key your_secret_key
```

## Example Pipeline

Here's a simple example pipeline in YAML format ([full example](examples/aqueduct_pipeline_simple.yml)):

```yaml
sources:
  # Register a local file source containing temperature readings for various cities
  - type: File
    name: temp_readings
    file_type:
      type: Csv
      options: {}
    # use built-in templating functionality
    location: ./examples/temp_readings_${month}_${year}.csv

  #Register a local file source containing a mapping between location_ids and location names
  - type: File
    name: locations
    file_type:
      type: Csv
      options: {}
    location: ./examples/location_dict.csv

stages:
  # Query to aggregate temperature data by date and location
  - - name: aggregated
      query: >
          SELECT
            cast(timestamp as date) date,
            location_id,
            round(min(temperature_c),2) min_temp_c,
            round(min(humidity),2) min_humidity,
            round(max(temperature_c),2) max_temp_c,
            round(max(humidity),2) max_humidity,
            round(avg(temperature_c),2) avg_temp_c,
            round(avg(humidity),2) avg_humidity
          FROM temp_readings
          GROUP by 1,2
          ORDER by 1 asc
      # print the query plan to stdout for debugging purposes
      explain: true

  # Enrich aggregation with the location name
  - - name: enriched
      query: >
        SELECT
          date,
          location_name,
          min_temp_c,
          max_temp_c,
          avg_temp_c,
          min_humidity,
          max_humidity,
          avg_humidity
        FROM aggregated
        JOIN locations 
          ON aggregated.location_id = locations.location_id
        ORDER BY date, location_name
      # print 10 rows to stdout for debugging purposes
      show: 10

# Write the pipeline result to a parquet file (can be omitted if you don't want an output)
destination:
  type: File
  name: results
  file_type:
    type: Parquet
    options: {}
  location: ./examples/output_${month}_${year}.parquet
```

## Components

Aqueducts consists of several components:

- **Core Library**: The main engine for defining and executing data pipelines
- **CLI**: Command-line interface to run pipelines locally or connect to remote executors
- **Executor**: Server component for running pipelines remotely, closer to data sources

For component-specific details, see the respective README files:
- [CLI README](aqueducts-cli/README.md)
- [Executor README](aqueducts-executor/README.md)

## Acknowledgements

This project would not be possible without the incredible work done by the open source community, particularly the maintainers and contributors of:

- [Apache Arrow](https://github.com/apache/arrow-rs) - The memory model and data structures that are used by many projects in this space
- [Apache DataFusion](https://github.com/apache/datafusion) - The SQL execution engine that handles all querying capabilities and more
- [Delta Lake Rust](https://github.com/delta-io/delta-rs) - Enabling Delta table support for rust projects

Please show these projects some love and support! ❤️

## Contributing

Contributions to Aqueducts are welcome! Whether it's bug reports, feature requests, or code contributions, we appreciate all forms of help.

Please see the [Contributing Guide](CONTRIBUTING.md) for detailed instructions on how to:
- Set up your development environment
- Run the project locally
- Set up Docker Compose for testing
- Configure ODBC connections
- Run the executor and CLI side-by-side
- Submit pull requests

## Roadmap

- [x] Docs
- [x] ODBC source and destination
- [x] Parallel processing of stages
- [x] Remote execution
- [x] Memory management
- [ ] Apache Iceberg support
- [ ] Web server for pipeline management and orchestration
- [ ] Data catalog implementation for deltalake and apache iceberg