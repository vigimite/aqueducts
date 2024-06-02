# Aqueducts

This is the documentation for [Aqueducts](https://github.com/vigimite/aqueducts)

[![Build status](https://github.com/vigimite/aqueducts/actions/workflows/build.yml/badge.svg?branch=main)](https://github.com/vigimite/aqueducts/actions/workflows/CI.yml) [![Crates.io](https://img.shields.io/crates/v/aqueducts)](https://crates.io/crates/aqueducts) [![Documentation](https://docs.rs/aqueducts/badge.svg)](https://docs.rs/aqueducts)

<img src="assets/logo.png" width="100">

Aqueducts is a framework to write and execute ETL data pipelines declaratively.

**Features:**

- Define ETL pipelines in YAML
- Extract data from csv files, parquet files or delta tables
- Process data using SQL
- Load data into object stores as csv/parquet or delta tables
- Support for file and delta table partitioning
- Support for Upsert/Replace/Append operation on delta tables
- Support for Local, S3, GCS and Azure Blob storage
- *EXPERIMENTAL* Support for ODBC Sources and Destinations

This framework builds on the fantastic work done by projects such as:

- [arrow-rs](https://github.com/apache/arrow-rs)
- [datafusion](https://github.com/apache/datafusion)
- [delta-rs](https://github.com/delta-io/delta-rs)

Please show these projects some support :heart:!
