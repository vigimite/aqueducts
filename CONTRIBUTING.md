# Contributing to Aqueducts

Thank you for your interest in contributing to Aqueducts! This guide will help you set up your development environment and understand how to effectively contribute to the project.

## Community

Join our Discord community to connect with other contributors, get help, and discuss development:

[Join Aqueducts Discord](https://discord.gg/astQZM3wqy)

## Table of Contents

1. [Development Environment Setup](#development-environment-setup)
2. [Crate Structure](#crate-structure)
3. [Running the Components](#running-the-components)
4. [Testing](#testing)
5. [Code Style](#code-style)
6. [Commit Guidelines](#commit-guidelines)
7. [Changelog Generation](#changelog-generation)
8. [Pull Request Process](#pull-request-process)
9. [Special Configurations](#special-configurations)

## Development Environment Setup

### Prerequisites

- [Rust](https://www.rust-lang.org/tools/install) (latest stable version)
- [Docker](https://docs.docker.com/get-docker/) and [Docker Compose](https://docs.docker.com/compose/install/)

### Basic Setup

1. Clone the repository:
   ```bash
   git clone https://github.com/vigimite/aqueducts.git
   cd aqueducts
   ```

2. Build the workspace:
   ```bash
   cargo build --workspace
   ```

3. Run tests to verify your setup:
   ```bash
   cargo test --workspace
   ```

### Docker Compose Setup

The project includes a Docker Compose configuration to set up supporting services:

1. Start the development environment:
   ```bash
   docker-compose up -d
   ```

   This will start:
   - PostgreSQL database (for testing ODBC connections)

2. To stop the services:
   ```bash
   docker-compose down
   ```

### Feature Flags

Aqueducts uses a unified meta crate with feature flags. When working on features, build with appropriate flags:

```bash
# For ODBC support
cargo build --features odbc

# For Delta Lake support
cargo build --features delta

# For all cloud storage options
cargo build --features s3,gcs,azure

# For all format options
cargo build --features yaml,json,toml

# For development with all features
cargo build --all-features
```

## Crate Structure

Aqueducts follows a modular architecture:

### Library Crates

- **`aqueducts/meta`** (meta crate): Unified interface with feature flags
  - Re-exports from all other crates
  - Provides `prelude`
  - Controls feature flags

- **`aqueducts/core/`**: Pipeline execution engine
  - `run_pipeline()` function
  - Progress tracking system
  - Template parameter substitution

- **`aqueducts/schemas/`**: Configuration types and parsing
  - Serde serialization/deserialization
  - Schema validation

- **`aqueducts/delta/`**: Delta Lake integration
  - Delta table sources and destinations
  - Append/upsert/replace operations
  - Time travel support

- **`aqueducts/odbc/`**: Database connectivity
  - ODBC sources and destinations
  - Secure connection string handling
  - Transaction support

### Application Crates

- **`aqueducts-cli/`**: Command-line interface
- **`aqueducts-executor/`**: Remote execution server

### Working with the Crate Structure

When contributing:

1. **For new features**: Add to the appropriate library crate
2. **For user-facing APIs**: Ensure they're re-exported in the meta crate
3. **For internal APIs**: Use `#[doc(hidden)]` if they must be public
4. **For errors**: Use the unified `AqueductsError` system
5. **For tests**: Place in the most specific crate possible

## Running the Components

### Running the CLI and Executor Side-by-Side

To test the complete pipeline execution flow locally (with remote execution), you can run both components:

1. First, start the executor in one terminal:
   ```bash
   # Terminal 1
   AQUEDUCTS_API_KEY=development cargo run --bin aqueducts-executor -- --port 3031 --max-memory 2
   ```

2. Then, in another terminal, use the CLI to send pipelines to the executor:
   ```bash
   # Terminal 2
   cargo run --bin aqueducts-cli -- run \
     --file examples/aqueduct_pipeline_example.yml \
     --param year=2024 --param month=jan \
     --executor http://localhost:3031 \
     --api-key development
   ```

This setup simulates a real-world deployment scenario where the executor runs close to the data sources and the CLI interfaces with it remotely.

## Testing

### Running Tests

```bash
# Run all tests (excluding ODBC tests)
cargo test --workspace

# Run tests for specific crates
cargo test -p aqueducts-core
cargo test -p aqueducts-schemas  
cargo test -p aqueducts          # meta crate

# Run a specific test
cargo test test_name

# Run ODBC tests (requires PostgreSQL server from docker-compose)
cargo test --features odbc_tests -p aqueducts-odbc

# Run tests with all features enabled
cargo test --workspace --all-features

# Test configuration file parsing
cargo test -p aqueducts-schemas --test integration
```

## Commit Guidelines

We use [Conventional Commits](https://www.conventionalcommits.org/) specification for commit messages to ensure consistent commit history and automatic changelog generation.

### Commit Message Format

```
<type>(<scope>): <subject>
```

#### Type

The type must be one of the following:

- **feat**: A new feature
- **fix**: A bug fix
- **docs**: Documentation only changes
- **style**: Changes that do not affect the meaning of the code (formatting, etc.)
- **refactor**: A code change that neither fixes a bug nor adds a feature
- **perf**: A code change that improves performance
- **test**: Adding missing or correcting existing tests
- **chore**: Changes to the build process or auxiliary tools

#### Scope

The scope is optional and should be a noun describing a section of the codebase:

- **cli**: Changes related to the CLI interface
- **executor**: Changes related to the executor component
- **core**: Changes to core library functionality (pipeline execution, error handling)
- **schemas**: Changes to configuration types
- **meta**: Changes to the unified meta crate interface
- **odbc**: Changes related to ODBC functionality
- **delta**: Changes related to Delta Lake functionality
- **docs**: Documentation updates

### Examples

```
feat(cli): add support for custom configuration files
```

```
fix(executor): resolve memory leak during large file processing
```

```
refactor(core): implement unified error handling across all crates
```

```
feat(schemas): add v2 configuration format support
```

```
docs: update examples to use v2 format
```

## Changelog Generation

We use [git-cliff](https://github.com/orhun/git-cliff) to generate our changelog automatically from commit messages. The configuration is defined in the `cliff.toml` file at the root of the repository.

### Installation

```bash
# Install git-cliff
cargo install git-cliff
```

### Generating the Changelog

```bash
# Generate the changelog
git cliff --output CHANGELOG.md
```

## Pull Request Process

1. Create a new branch for your feature or bugfix
2. Implement your changes with appropriate tests
3. Ensure all tests pass and code styling is consistent:
   ```bash
   cargo fmt
   cargo clippy --workspace
   cargo test --workspace
   ```
4. Update documentation if necessary:
   - Add examples to public APIs
   - Update configuration examples to use v2 format
   - Update relevant README/ARCHITECTURE docs
5. Verify the meta crate re-exports any new public APIs
6. Submit a pull request with a clear description of the changes

## Special Configurations

### ODBC Setup

For ODBC support, follow these steps:

1. Install the required system dependencies:
   - On Ubuntu/Debian: `sudo apt-get install unixodbc-dev`
   - On Fedora/RHEL/CentOS: `sudo dnf install unixODBC-devel`
   - On macOS: `brew install unixodbc`

2. Install database-specific drivers:
   - PostgreSQL: `sudo apt-get install odbc-postgresql` (Ubuntu/Debian)
   - MySQL: `sudo apt-get install libmyodbc` (Ubuntu/Debian)

3. Configure your ODBC system:
   - Edit `/etc/odbcinst.ini` to register drivers
   - Edit `/etc/odbc.ini` or `~/.odbc.ini` to define data sources

For more detailed ODBC setup instructions, refer to the [arrow-odbc](https://github.com/pacman82/arrow-odbc) project documentation, which Aqueducts uses for ODBC support.

Example `odbcinst.ini` configuration for PostgreSQL (Unicode version):
```ini
[PostgreSQL Unicode]
Description=PostgreSQL ODBC driver (Unicode version)
Driver=/usr/lib/x86_64-linux-gnu/odbc/psqlodbcw.so
Setup=/usr/lib/x86_64-linux-gnu/odbc/libodbcpsqlS.so
Debug=0
CommLog=1
UsageCount=1
```

Example `odbc.ini` configuration:
```ini
[testdb]
Driver=PostgreSQL Unicode
Database=postgres
Servername=localhost
UserName=postgres
Password=postgres
Port=5432
```

## Troubleshooting

### Common Issues

1. **ODBC Connection Problems**:
   - Run `odbcinst -j` to verify ODBC configuration locations
   - Test connections using `isql -v YOUR_DSN YOUR_USERNAME YOUR_PASSWORD`
   - Check that appropriate drivers are installed and configured

2. **Cargo Build Errors**:
   - Ensure you have the correct system dependencies installed
   - For ODBC issues, verify that `unixodbc-dev` is installed
   - Clean your Cargo cache: `cargo clean`

3. **Docker Issues**:
   - Ensure Docker daemon is running
   - Check port conflicts with `netstat -tulpn | grep 3031`
   - Verify Docker Compose is installed and configured correctly
