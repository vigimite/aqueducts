# Aqueducts

[![Build status](https://github.com/vigimite/aqueducts/actions/workflows/build.yml/badge.svg?branch=main)](https://github.com/vigimite/aqueducts/actions/workflows/CI.yml) [![Crates.io](https://img.shields.io/crates/v/aqueducts)](https://crates.io/crates/aqueducts) [![Documentation](https://docs.rs/aqueducts/badge.svg)](https://docs.rs/aqueducts)

<img src="https://github.com/vigimite/aqueducts/raw/main/logo.png" width="100">

Aqueducts is a framework to write and execute ETL data pipelines declaratively.

**Features:**

- **Declarative Pipeline Configuration**: Define ETL pipelines in YAML, JSON, or TOML
- **Pipeline Execution**: Run pipelines locally, remotely, or embedded in your applications
- **Data Source Support**: Extract from CSV, JSONL, Parquet files, Delta tables, and ODBC databases
- **Data Destination Support**: Load data into local files, object stores, or Delta tables
- **Cloud Storage Support**: Built-in support for Local, S3, GCS, and Azure Blob storage
- **Advanced Operations**: Upsert/Replace/Append operations with partition support
- **Template System**: Parameter substitution with `${variable}` syntax

## Documentation

- [Full Documentation](https://vigimite.github.io/aqueducts)
- [Architecture Documentation](ARCHITECTURE.md)
- [Change Log](CHANGELOG.md)
- [Contributing Guide](CONTRIBUTING.md)

## Community

Join our Discord community to get help, share your work, and connect with other Aqueducts users:

[![Discord](https://img.shields.io/discord/1234567890?color=7289DA&label=Discord&logo=discord&logoColor=white)](https://discord.gg/astQZM3wqy)

## Installation

### Library

Add Aqueducts to your Rust project:

```toml
[dependencies]
# Default setup with file processing and cloud storage
aqueducts = "0.10"

# Minimal setup for local file processing only
aqueducts = { version = "0.10", default-features = false, features = ["yaml"] }

# Full-featured setup with all storage providers and databases
aqueducts = { version = "0.10", features = ["json", "toml", "yaml", "s3", "gcs", "azure", "odbc", "delta"] }
```

### CLI

#### Recommended Installation Methods

**Homebrew (macOS and Linux):**
```bash
# Add the tap and install
brew tap vigimite/aqueducts
brew install aqueducts-cli
```

**Shell Installer (Linux, macOS, Windows):**
```bash
# One-line installer
curl --proto '=https' --tlsv1.2 -LsSf https://github.com/vigimite/aqueducts/releases/latest/download/aqueducts-installer.sh | sh
```

**Direct Download:**
Download pre-built binaries from the [latest release](https://github.com/vigimite/aqueducts/releases/latest).

#### Build from Source

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
version: "v2"
sources:
  # Register a local file source containing temperature readings for various cities
  - type: file
    name: temp_readings
    format:
      type: csv
      options: {}
    # use built-in templating functionality
    location: ./examples/temp_readings_${month}_${year}.csv

  #Register a local file source containing a mapping between location_ids and location names
  - type: file
    name: locations
    format:
      type: csv
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
  type: file
  name: results
  format:
    type: parquet
    options: {}
  location: ./examples/output_${month}_${year}.parquet
```

## Library Usage

Use Aqueducts as a library in your Rust applications:

```rust
use aqueducts::prelude::*;
use datafusion::prelude::SessionContext;
use std::{collections::HashMap, sync::Arc};

#[tokio::main]
async fn main() -> Result<()> {
    // Load pipeline configuration with parameters
    let mut params = HashMap::new();
    params.insert("year".to_string(), "2024".to_string());
    params.insert("month".to_string(), "jan".to_string());
    
    let pipeline = Aqueduct::from_file("pipeline.yml", params)?;
    
    // Create DataFusion execution context
    let ctx = Arc::new(SessionContext::new());
    
    // Execute pipeline with progress tracking
    let tracker = Arc::new(LoggingProgressTracker);
    let _result = run_pipeline(ctx, pipeline, Some(tracker)).await?;
    
    Ok(())
}
```

## Components

Aqueducts consists of several components:

- **Meta Library** (`aqueducts`): Unified interface providing all functionality through feature flags
- **Core Library** (`aqueducts-core`): The main engine for defining and executing data pipelines
- **Schema Library** (`aqueducts-schemas`): Configuration types and validation
- **Provider Libraries**: Delta Lake (`aqueducts-delta`) and ODBC (`aqueducts-odbc`) integrations
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
- [ ] Web server for pipeline management and orchestration
- [ ] Apache Iceberg support
- [ ] Data catalog implementation for deltalake and apache iceberg
