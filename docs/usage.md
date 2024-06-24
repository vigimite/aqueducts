# Using the Aqueducts framework in your application

## Quickstart

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


## Example YAML configurations

Here are some examples on how to use the Aqueducts deserialization schema for YAML files

### Sources


!!! example

    === "local CSV file source"

        ```yaml
        sources:
          - type: File
            name: feb_data
            file_type:
              type: Csv
              options:
                has_header: true
                delimiter: ","
            location: ./examples/temp_readings_feb_2024.csv
        ```

    === "local JSONL file source"

        ```yaml
        sources:
          - type: File
            name: feb_data
            file_type:
              type: Json
              options:
                has_header: true
                delimiter: ","
            location: ./examples/temp_readings_feb_2024.csv
        ```

    === "Parquet file on S3"

        ```yaml
        sources:
          - type: File
            name: feb_data
            file_type:
              type: Parquet
              options: {}
            location: s3://example_bucket_name/prefix/temp_readings_feb_2024.csv
        ```

    === "Directory with parquet files on S3"

        ```yaml
        sources:
          - type: Directory
            name: feb_data
            file_type:
              type: Parquet
              options: {}
            # location has to end in `/`
            location: s3://example_bucket_name/prefix/
            # hdfs style partitioning applied e.g. ...prefix/date=2024-01-01/location=US/
            partition_cols:
              - [date, Date32] 
              - [location, Utf8] 
        ```

    === "Delta source"

        ```yaml
        sources:
          - type: Delta
            name: temp_data
            location: s3://example_bucket_name/prefix/temp_readings
            storage_options:
              TIMEOUT: "300s" # S3 client timeout set to 5 minutes
        ```

    === "ODBC Postgres"

        ```yaml
        sources:
          - type: Odbc
            name: feb_data
            connection_string: Driver={PostgreSQL Unicode};Server=localhost;UID=${user};PWD=${pass};
            # Query to execute against the ODBC source
            query: SELECT * FROM temp_readings WHERE timestamp BETWEEN '2024-02-01' AND '2024-02-29'
        ```

### Processing stages

!!! example

    === "Simple query"

        ```yaml
        stages:
          - - name: simple_select
              query: SELECT * FROM readings

          - - name: multiline_example
              query: >
                SELECT
                  a,
                  b,
                  c
                FROM example
        ```

    === "Parallel execution"

        ```yaml
        stages:
          - - name: parallel_1_a
              query: SELECT * FROM readings

            - name: parallel_1_b
              query: >
                SELECT
                  a,
                  b,
                  c
                FROM example

          - - name: parallel_2_a
              query: SELECT * FROM readings

            - name: parallel_2_b
              query: >
                SELECT
                  a,
                  b,
                  c
                FROM example
        ```

    === "Debugging options"

        ```yaml
        stages:
          - - name: show_all
              query: SELECT * FROM readings
              show: 0 # show complete result set

            - name: show_limit
              query: SELECT * FROM readings
              show: 10 # show 10 values

            - name: print_schema
              query: SELECT * FROM readings
              print_schema: true # print the data frame schema to stdout

            - name: explain
              query: SELECT * FROM readings
              explain: true # print the query plan to stdout

            - name: explain_analyze
              query: SELECT * FROM readings
              explain_analyze: true # print the query plan with execution statistics to stdout, takes precedence over explain

            - name: combine
              query: SELECT * FROM readings

              # combine multiple debug options together
              explain_analyze: true
              print_schema: true
              show: 10
        ```

### Destination configuration

!!! example

    === "CSV File destination"

        ```yaml
    
        destination:
          type: file
          name: results
          file_type:
            type: Csv
            options: {}
          location: ./examples/output_${month}_${year}.parquet
        ```

    === "Delta append"

        ```yaml
        destination:
          type: delta
          name: example_output
          location: ${local_path}/examples/output_delta_example/${run_id}
          storage_options: {}
          table_properties: {}

          write_mode:
            # appends data to the table
            operation: append

          # columns by which to partition the table
          partition_cols:
            - date

          # table schema using de-serialization provided by `deltalake::kernel::StructField`
          schema:
            - name: date
              type: date
              nullable: true
              metadata: {}
            - name: location_id
              type: integer
              nullable: true
              metadata: {}
            - name: avg_temp_c
              type: double
              nullable: true
              metadata: {}
            - name: avg_humidity
              type: double
              nullable: true
              metadata: {}
        ```

    === "Delta upsert"

        ```yaml
        destination:
          type: delta
          name: example_output
          location: ${local_path}/examples/output_delta_example/${run_id}
          storage_options: {}
          table_properties: {}

          write_mode:
            # upserts using the date as the "primary" key
            operation: upsert
            params: 
              - date

          # columns by which to partition the table
          partition_cols:
            - date

          # table schema using de-serialization provided by `deltalake::kernel::StructField`
          schema:
            - name: date
              type: date
              nullable: true
              metadata: {}
            - name: location_id
              type: integer
              nullable: true
              metadata: {}
            - name: avg_temp_c
              type: double
              nullable: true
              metadata: {}
            - name: avg_humidity
              type: double
              nullable: true
              metadata: {}
        ```

    === "Delta replace"

        ```yaml
        destination:
          type: delta
          name: example_output
          location: ${local_path}/examples/output_delta_example/${run_id}
          storage_options: {}
          table_properties: {}

          write_mode:
            # replaces using the date column to delete all data for that date
            operation: replace
            params: 
              - column: date
                value: '2024-01-01'

          # columns by which to partition the table
          partition_cols:
            - date

          # table schema using de-serialization provided by `deltalake::kernel::StructField`
          schema:
            - name: date
              type: date
              nullable: true
              metadata: {}
            - name: location_id
              type: integer
              nullable: true
              metadata: {}
            - name: avg_temp_c
              type: double
              nullable: true
              metadata: {}
            - name: avg_humidity
              type: double
              nullable: true
              metadata: {}
        ```
