# Aqueducts

<img src="logo.png" width="200">

Aqueducts is a framework to write and execute ETL data pipelines declaratively.
This framework builds on the fantastic work done by projects such as:

- [arrow-rs](https://github.com/apache/arrow-rs)
- [datafusion](https://github.com/apache/datafusion)
- [delta-rs](https://github.com/delta-io/delta-rs)

Please show these projects some support :heart:!

## Table of contents

- [Aqueducts](#aqueducts)
  - [Table of contents](#table-of-contents)
  - [Quick start](#quick-start)
  - [Using the Aqueducts framework in your application](#using-the-aqueducts-framework-in-your-application)
  - [Architecture](#architecture)
    - [Source](#source)
    - [Stage](#stage)
    - [Destination](#destination)
  - [Roadmap](#roadmap)

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
  - name: aggregated
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
  - name: enriched
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

## Using the Aqueducts framework in your application

In order to load and execute an aqueduct pipeline we can first read the yaml configuration from a local file path:

```rust
use aqueduct::prelude::*;

// Provide params that will be substituted in the aqueduct template
let params = HashMap::from_iter(vec![
    ("date".into(), "2024-01-01".into()),
]);

// Load pipeline from file
let aqueduct = Aqueduct::try_from_yml("./examples/aqueduct_pipeline_example.yml", params).unwrap();
```

We can then execute the pipeline:

```rust
use aqueduct::prelude::*;

// Optionally set up a `SessionContext` to register necessary object_stores or UDFs, UDAFs
let result = run_pipeline(aqueduct, None).await.unwrap();
```

The pipeline execution will:

1. register all sources into the SessionContext using the given name as a table identifier
2. execute all defined stages sequentially top to bottom, caching the result of each stage as a table using the name of the stage (can be referenced downstream via SQL using the stage name)
3. use the result of the final stage to write data to a destination if defined

## Architecture

An aqueduct is a pipeline definition and consists of 3 main parts

- Source -> the source data for this pipeline
- Stage -> transformations applied within this pipeline
- Destination -> output of the pipeline result

### Source

An Aqueduct source can be:

- CSV or Parquet file(s)
  - single file
  - directory
- Delta table
- PostgreSQL table (NOT IMPLEMENTED YET)

For file based sources a schema can be provided optionally.

The source is registered within the `SessionContext` as a table that can be referenced using the sources configured name. A prerequisite here is that the necessary features for the underlying obejct stores are enabled.
This can be provided by an external `SessionContext` passed into the `run_pipeline` function or by registering the correct handlers for deltalake.

### Stage

An Aqueduct stage defines a transformation using SQL. Each stage has access to all defined sources and to every previously executed stage within the SQL context using the respectively configured names.
Once executed the stage will then persist its result into the SQL context making it accessible to downstream consumers.

The stage can be set to print the result and/or the result schema to the `stdout`. This is usefull for development/debugging purposes.

### Destination

An Aqueduct destination can be:

- CSV or Parquet file(s)
  - single file
  - directory
- Delta table
- PostgreSQL table (NOT IMPLEMENTED YET)

An Aqueduct destination is the target for the execution of the pipeline, the result of the final stage that was executed is used as the input for the destination to write the data to the underlying table/file.

**File based destinations**
File based destinations have support for HDFS style partitioning (`output/location=1/...`) and can be set to output only a single file or multiple files based on the configuration.

**Delta Table destination**
For a DeltaTable there is some additional logic that is utilized to maintain the table integrity.

The destination will first cast and validate the schema of the input data and then use one of 3 configurable modes to write the data:

- Append -> appends the data to the destination
- Upsert -> merges the data to the destination, using the provided configuration for this mode to identify cohort columns that are used to determine which data should be updated
  - provided merge columns are used to check equality e.g. `vec!["date", "country"]` -> update data where `old.date = new.date AND old.country = new.country`
- Replace -> replaces the data using a configurable predicate to determine which data should be replaced by the operation
  - provided replacement conditions are used to check equality e.g. `ReplacementCondition { column: "date", value: "1970-01-01" }` -> replace data where `old.date = '1970-01-01'`

## Roadmap

- [ ] Docs
- [ ] PostgreSQL source
- [ ] PostgreSQL destination
- [ ] Parallel processing of stages
