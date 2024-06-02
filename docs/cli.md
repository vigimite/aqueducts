# Aqueducts CLI

Example CLI application utilizing the Aqueducts framework to run ETL pipelines declared in YAML.

## Install

```bash
# install with default features (s3, gcs, azure)
cargo install aqueducts-cli

# install with odbc support
cargo install aqueducts-cli --features odbc

# install with s3 support only
cargo install aqueducts-cli --no-default-features --features s3
```

## Run

```bash
aqueducts --file ./example.yml --param key1=value1 --param key2=value2  
```
