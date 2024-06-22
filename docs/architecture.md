# Architecture

An aqueduct is a pipeline definition and consists of 3 main parts

- Source -> the source data for this pipeline
- Stage -> transformations applied within this pipeline
- Destination -> output of the pipeline result

## Source

An Aqueduct source can be:

- CSV or Parquet file(s)
  - single file
  - directory
- Delta table
- ODBC query (EXPERIMENTAL)

For file based sources a schema can be provided optionally.

The source is registered within the `SessionContext` as a table that can be referenced using the sources configured name. A prerequisite here is that the necessary features for the underlying obejct stores are enabled.
This can be provided by an external `SessionContext` passed into the `run_pipeline` function or by registering the correct handlers for deltalake.

**EXPERIMENTAL ODBC support**

As an experimental feature it is possible to query various databases using ODBC. This is enabled through [arrow-odbc](https://crates.io/crates/arrow-odbc).
Besides enabling the `odbc` feature flag in your `Cargo.toml` there are some other prerequisites for the executing system:

- `unixodbc` on unix based systems
- ODBC driver for the database you want to access like [ODBC Driver for SQL server](https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server) or [psqlodbc](https://github.com/postgresql-interfaces/psqlodbc)
- registering the driver in the ODBC manager configuration (usually located in `/etc/odbcinst.ini`)

If you have issues setting this up there are many resources online explaining how to set this up, it is a bit of a hassle.

## Stage

An Aqueduct stage defines a transformation using SQL. Each stage has access to all defined sources and to every previously executed stage within the SQL context using the respectively configured names.
Once executed the stage will then persist its result into the SQL context making it accessible to downstream consumers.

The stage can be set to print the result and/or the result schema to the `stdout`. This is useful for development/debugging purposes.

Nested stages are executed in parallel

## Destination

An Aqueduct destination can be:

- CSV or Parquet file(s)
  - single file
  - directory
- Delta table
- ODBC query (NOT IMPLEMENTED YET)

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
