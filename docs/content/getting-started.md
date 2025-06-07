# Getting Started

This guide will help you install Aqueducts and run your first pipeline.

## Installation

### CLI

The Aqueducts CLI is the primary way to execute pipelines locally or submit them to remote executors.

=== "Homebrew (macOS/Linux)"

    ```bash
    # Add the tap and install
    brew tap vigimite/aqueducts
    brew install aqueducts-cli
    ```

=== "Shell Installer"

    ```bash
    # One-line installer (Linux, macOS, Windows)
    curl --proto '=https' --tlsv1.2 -LsSf https://github.com/vigimite/aqueducts/releases/latest/download/aqueducts-installer.sh | sh
    ```

=== "Direct Download"

    Download pre-built binaries from the [latest release](https://github.com/vigimite/aqueducts/releases/latest).

=== "Build from Source"

    ```bash
    # Install with default features (s3, gcs, azure, yaml)
    cargo install aqueducts-cli --locked

    # Install with ODBC support
    cargo install aqueducts-cli --locked --features odbc

    # Install with minimal features
    cargo install aqueducts-cli --locked --no-default-features --features yaml
    ```

### Executor (Optional)

For advanced use cases, you can deploy executors to run pipelines remotely within your infrastructure. Executors are typically deployed using Docker images. See [Execution](execution.md#aqueducts-executor) for detailed setup instructions.

## Quick Start

### Run Your First Pipeline

Execute the simple example pipeline:

```bash
aqueducts run --file examples/aqueduct_pipeline_simple.yml --param year=2024 --param month=jan
```

This pipeline:

1. **Reads** temperature data from CSV files
2. **Aggregates** the data by date and location
3. **Enriches** with location names
4. **Outputs** to a Parquet file

### Example Pipeline Structure

Here's what a basic pipeline looks like:

```yaml
# yaml-language-server: $schema=https://raw.githubusercontent.com/vigimite/aqueducts/main/json_schema/aqueducts.schema.json

version: "v2"
sources:
  # Read temperature readings from CSV
  - type: file
    name: temp_readings
    format:
      type: csv
      options: {}
    location: ./examples/temp_readings_${month}_${year}.csv

stages:
  # Aggregate temperature data by date and location
  - - name: aggregated
      query: >
          SELECT
            cast(timestamp as date) date,
            location_id,
            round(avg(temperature_c),2) avg_temp_c
          FROM temp_readings
          GROUP by 1,2
          ORDER by 1 asc

# Write results to Parquet file
destination:
  type: file
  name: results
  format:
    type: parquet
    options: {}
  location: ./examples/output_${month}_${year}.parquet
```

## Next Steps

- **Learn Pipeline Development**: See [Writing Pipelines](pipelines.md) to understand sources, stages, and destinations
- **Explore Execution Options**: Check [Execution](execution.md) for local and remote execution patterns
- **Browse Examples**: View more complex examples in the [examples folder](https://github.com/vigimite/aqueducts/tree/main/examples)
- **Schema Reference**: Detailed configuration options in [Schema Reference](schema_reference.md)

!!! tip "Parameter Templates"
    Notice the `${month}` and `${year}` parameters in the example. Aqueducts supports parameter substitution to make pipelines reusable across different inputs and outputs.

!!! tip "Editor Support"
    The `yaml-language-server` comment at the top enables autocompletion and validation in VS Code, Neovim, and other editors with YAML Language Server support.
