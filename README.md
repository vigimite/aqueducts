# Aqueducts

[![Build status](https://github.com/vigimite/aqueducts/actions/workflows/build.yml/badge.svg?branch=main)](https://github.com/vigimite/aqueducts/actions/workflows/CI.yml) [![Crates.io](https://img.shields.io/crates/v/aqueducts)](https://crates.io/crates/aqueducts) [![Documentation](https://docs.rs/aqueducts/badge.svg)](https://docs.rs/aqueducts)

<img src="https://github.com/vigimite/aqueducts/raw/main/logo.png" width="100">

Aqueducts is a framework to write and execute ETL data pipelines declaratively.

**Features:**

- Define ETL pipelines in YAML
- Extract data from csv files, JSONL, parquet files or delta tables
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

## Documentation

You can find the docs at <https://vigimite.github.io/aqueducts>

Change log: [CHANGELOG](CHANGELOG.md)

## Quick start

To define and execute an Aqueduct pipeline there are a couple of options

- using a yaml configuration file
- using a json configuration file
- manually in code

You can check out some examples in the [examples](examples) directory.
Here is a simple example defining an Aqueduct pipeline using the yaml config format [link](examples/aqueduct_pipeline_simple.yml):

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

This repository contains a minimal example implementation of the Aqueducts framework which can be used to test out pipeline definitions like the one above:

```bash
cargo install aqueducts-cli
aqueducts --file examples/aqueduct_pipeline_example.yml --param year=2024 --param month=jan
```

## Roadmap

- [x] Docs
- [x] ODBC source
- [x] ODBC destination
- [x] Parallel processing of stages
- [ ] Streaming Source (initially kafka + maybe aws kinesis)
- [ ] Streaming destination (initially kafka)
