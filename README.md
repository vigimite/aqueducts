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

## Documentation

- [Architecture Documentation](ARCHITECTURE.md)
- [Full Documentation](https://vigimite.github.io/aqueducts)
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
cargo install aqueducts-cli

# Install with odbc support
cargo install aqueducts-cli --features odbc

# Install with minimal features
cargo install aqueducts-cli --no-default-features --features yaml
```

### Executor

Install the executor to run pipelines remotely, closer to data sources:

```bash
# Install the executor (with default cloud storage support)
cargo install aqueducts-executor

# For ODBC support (requires unixodbc-dev to be installed)
cargo install aqueducts-executor --features odbc
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
- **Utils**: Shared utilities and models used across components

For component-specific details, see the respective README files:
- [CLI README](aqueducts-cli/README.md)
- [Executor README](aqueducts-executor/README.md)

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