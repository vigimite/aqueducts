# Aqueducts CLI

A command-line interface for executing Aqueducts data pipelines, with support for both local and remote execution.

## Features

- Run pipelines defined in YAML, JSON, or TOML formats
- Execute pipelines locally or remotely via the Aqueducts Executor
- Check status of remote executors
- Cancel running pipelines on remote executors
- Real-time progress tracking and event streaming
- Cloud storage support (S3, GCS, Azure) via feature flags
- ODBC database connectivity via feature flags

## Installation

### Recommended Installation Methods

**Homebrew (macOS and Linux):**
```bash
# Add the tap and install
brew tap vigimite/aqueducts
brew install aqueducts-cli
```

**Shell Installer (Cross-platform):**
```bash
# One-line installer for Linux, macOS, and Windows
curl --proto '=https' --tlsv1.2 -LsSf https://github.com/vigimite/aqueducts/releases/latest/download/aqueducts-installer.sh | sh
```

**Direct Download:**
Download pre-built binaries for your platform from the [latest release](https://github.com/vigimite/aqueducts/releases/latest):
- Linux x86_64
- macOS Apple Silicon (ARM64)  
- macOS Intel (x86_64)

### Build from Source

```bash
# Install with default features (s3, gcs, azure, yaml)
cargo install aqueducts-cli --locked

# Install with odbc support (requires unixodbc-dev)
cargo install aqueducts-cli --locked --features odbc

# Install with minimal features
cargo install aqueducts-cli --locked --no-default-features --features yaml
```

## Usage

### Running Pipelines

Run a pipeline locally:

```bash
# Basic usage (YAML)
aqueducts run --file ./pipeline.yml

# With parameters
aqueducts run --file ./pipeline.yml --params key1=value1 --params key2=value2

# Using TOML or JSON (with appropriate feature flags)
aqueducts run --file ./pipeline.toml
aqueducts run --file ./pipeline.json
```

Run a pipeline on a remote executor:

```bash
# Execute on remote executor
aqueducts run --file ./pipeline.yml --executor executor-host:3031 --api-key your_api_key
```

Cancel a running pipeline on a remote executor:

```bash
# Cancel a specific execution by ID
aqueducts cancel --executor executor-host:3031 --api-key your_api_key --execution-id abc-123
```

## Pipeline Definition Examples

YAML pipeline example:

```yaml
sources:
  - type: File
    name: temp_readings
    file_type:
      type: Csv
      options: {}
    location: ./examples/temp_readings_${month}_${year}.csv

stages:
  - - name: transformed_data
      query: "SELECT * FROM source_data WHERE value > 10"

destination:
  type: File
  name: results
  file_type:
    type: Parquet
    options: {}
  location: ./examples/output_${month}_${year}.parquet
```

## Troubleshooting

Common issues:

1. **Authentication failures**: Verify API key is correct
2. **Connectivity issues**: Check network connectivity and firewall rules
3. **Pipeline validation errors**: Ensure your pipeline definition is valid
4. **Executor busy**: Only one pipeline can run at a time on an executor
5. **Missing features**: Make sure the CLI was compiled with the needed features

For more information on architecture and advanced usage, see the [Aqueducts Architecture Documentation](../ARCHITECTURE.md).
