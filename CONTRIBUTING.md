# Contributing to Aqueducts

Thank you for your interest in contributing to Aqueducts! This guide will help you set up your development environment and understand how to effectively contribute to the project.

## Community

Join our Discord community to connect with other contributors, get help, and discuss development:

[Join Aqueducts Discord](https://discord.gg/astQZM3wqy)

## Table of Contents

1. [Development Environment Setup](#development-environment-setup)
2. [Running the Components](#running-the-components)
3. [Testing](#testing)
4. [Code Style](#code-style)
5. [Commit Guidelines](#commit-guidelines)
6. [Changelog Generation](#changelog-generation)
7. [Pull Request Process](#pull-request-process)
8. [Special Configurations](#special-configurations)

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

The project includes a Docker Compose configuration for easily setting up supporting services:

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

When working on features that require specific feature flags, build with those flags enabled:

```bash
# For ODBC support
cargo build --features odbc

# For all cloud storage options
cargo build --features s3,gcs,azure

# For all format options
cargo build --features yaml,json,toml
```

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
# Run all tests
cargo test --workspace

# Run tests for a specific crate
cargo test -p aqueducts-core

# Run a specific test
cargo test test_name

# Run tests with all features enabled, this needs the PostgreSQL server provided in the docker-compose
cargo test --workspace --all-features
```

## Code Style

Follow the established code style in the project:

1. Format your code using `rustfmt`:
   ```bash
   cargo fmt
   ```

2. Check for code style issues:
   ```bash
   cargo fmt -- --check
   ```

3. Run linter checks:
   ```bash
   cargo clippy --workspace
   ```

Key style guidelines:
- Use `snake_case` for functions and variables, `CamelCase` for types/structs
- Group imports: std library first, external crates second, internal modules last
- Use `thiserror` for error types, implement `std::error::Error` trait
- Follow the existing module organization by domain concepts

## Commit Guidelines

We use [Conventional Commits](https://www.conventionalcommits.org/) specification for commit messages to ensure consistent commit history and automatic changelog generation.

### Commit Message Format

Each commit message consists of a **header**, a **body**, and a **footer**:

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
- **core**: Changes to core library functionality
- **odbc**: Changes related to ODBC functionality
- **s3**: Changes related to S3 storage
- **delta**: Changes related to Delta Lake functionality

#### Subject

The subject contains a succinct description of the change:

- Use the imperative, present tense: "add" not "added" nor "adds"
- Don't capitalize the first letter
- No period (.) at the end

### Examples

```
feat(cli): add support for custom configuration files
```

```
fix(executor): resolve memory leak during large file processing
```

```
refactor(core): improve error handling in pipeline execution
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
3. Ensure all tests pass and code styling is consistent
4. Update documentation if necessary
5. Submit a pull request with a clear description of the changes

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
