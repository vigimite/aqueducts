# Writing Pipelines

Aqueducts pipelines are declarative YAML configurations that define data processing workflows. This guide covers the key concepts and patterns for building effective pipelines.

## Pipeline Structure

Every Aqueducts pipeline follows this basic structure:

```yaml
# yaml-language-server: $schema=https://raw.githubusercontent.com/vigimite/aqueducts/main/json_schema/aqueducts.schema.json

version: "v2"        # Schema version
sources: [...]       # Where to read data from
stages: [...]        # How to transform the data  
destination: {...}   # Where to write the results (optional)
```

### Editor Support & Validation

For the best development experience, add the schema directive at the top of your pipeline files:

```yaml
# yaml-language-server: $schema=https://raw.githubusercontent.com/vigimite/aqueducts/main/json_schema/aqueducts.schema.json
```

This enables:

- **Autocompletion** for all configuration options
- **Real-time validation** with error highlighting  
- **Inline documentation** on hover
- **Schema-aware suggestions** for valid values

**Supported Editors:**
- VS Code (with YAML extension)
- Neovim (with yaml-language-server)
- IntelliJ IDEA / PyCharm
- Any editor with YAML Language Server support

## Data Sources

Sources define where your pipeline reads data from. Aqueducts supports multiple source types:

### File Sources

Read from individual files in various formats:

```yaml
sources:
  - type: file
    name: sales_data
    format:
      type: csv
      options:
        has_header: true
        delimiter: ","
    location: ./data/sales.csv
```

**Supported formats:** CSV, Parquet, JSON

### Directory Sources

Process all files in a directory:

```yaml
sources:
  - type: directory
    name: daily_logs
    format:
      type: parquet
      options: {}
    location: s3://bucket/logs/
    partition_columns:
      - ["date", "date32"]
```

### Delta Table Sources

Read from Delta Lake tables:

```yaml
sources:
  - type: delta
    name: user_events
    location: s3://datalake/events/
    # Optional: read specific version or timestamp
    version: 42
    # timestamp: "2024-01-15T10:30:00Z"
```

### ODBC Sources

!!! note "Feature Flag Required"
    ODBC support requires the `odbc` feature flag during installation.

```yaml
sources:
  - type: odbc
    name: postgres_table
    connection_string: "Driver={PostgreSQL};Server=localhost;Database=test;"
    load_query: "SELECT * FROM users WHERE created_at > '2024-01-01'"
```

### Cloud Storage Configuration

All file and directory sources support cloud storage with authentication:

=== "S3"

    ```yaml
    sources:
      - type: file
        name: s3_data
        location: s3://my-bucket/data.csv
        storage_config:
          AWS_REGION: us-east-1
          AWS_ACCESS_KEY_ID: ${AWS_ACCESS_KEY_ID}
          AWS_SECRET_ACCESS_KEY: ${AWS_SECRET_ACCESS_KEY}
    ```

=== "Google Cloud Storage"

    ```yaml
    sources:
      - type: file
        name: gcs_data
        location: gs://my-bucket/data.csv
        storage_config:
          GOOGLE_SERVICE_ACCOUNT_PATH: /path/to/service-account.json
    ```

=== "Azure Blob Storage"

    ```yaml
    sources:
      - type: file
        name: azure_data
        location: abfss://container@account.dfs.core.windows.net/data.csv
        storage_config:
          AZURE_STORAGE_ACCOUNT_NAME: myaccount
          AZURE_STORAGE_ACCOUNT_KEY: ${AZURE_KEY}
    ```

## Data Transformation (Stages)

Stages define SQL queries that transform your data. Aqueducts uses [DataFusion](https://datafusion.apache.org/user-guide/sql/index.html) for SQL processing.

### Basic Stages

```yaml
stages:
  - - name: daily_summary
      query: >
        SELECT 
          date_trunc('day', timestamp) as date,
          count(*) as events,
          avg(value) as avg_value
        FROM source_data
        GROUP BY 1
        ORDER BY 1
```

### Parallel Execution

Stages at the same level execute in parallel:

```yaml
stages:
  # These run in parallel
  - - name: sales_summary
      query: "SELECT region, sum(amount) FROM sales GROUP BY region"
    
    - name: user_summary  
      query: "SELECT country, count(*) FROM users GROUP BY country"
  
  # This runs after both above stages complete
  - - name: combined_report
      query: >
        SELECT s.region, s.total_sales, u.user_count
        FROM sales_summary s
        JOIN user_summary u ON s.region = u.country
```

### Debugging Stages

Add debugging options to inspect your data:

```yaml
stages:
  - - name: debug_stage
      query: "SELECT * FROM source_data LIMIT 10"
      show: 10              # Print 10 rows to stdout
      explain: true         # Show query execution plan
      print_schema: true    # Print output schema
```

## Data Destinations

Destinations define where to write your pipeline results.

### File Destinations

Write to various file formats:

```yaml
destination:
  type: file
  name: output
  location: ./results/output.parquet
  format:
    type: parquet
    options: {}
  single_file: true
  partition_columns: ["region"]
```

### Delta Table Destinations

Write to Delta Lake with advanced operations:

```yaml
destination:
  type: delta
  name: target_table
  location: s3://datalake/target/
  write_mode:
    operation: upsert    # append, replace, or upsert
    params: ["user_id"]  # upsert key columns
  partition_columns: ["date"]
  schema:
    - name: user_id
      data_type: int64
      nullable: false
    - name: name
      data_type: string
      nullable: true
```

## Advanced Features

### Parameter Templating

Make pipelines reusable with parameter substitution:

```yaml
sources:
  - type: file
    name: data
    location: ./data/${environment}/${date}.csv
    
destination:
  type: file
  location: ./output/${environment}_${date}_results.parquet
```

Execute with parameters:

```bash
aqueducts run --file pipeline.yml --param environment=prod --param date=2024-01-15
```

### Schema Definitions

Define explicit schemas for type safety:

```yaml
sources:
  - type: file
    name: typed_data
    format:
      type: csv
      options:
        schema:
          - name: user_id
            data_type: int64
            nullable: false
          - name: score
            data_type: "decimal<10,2>"
            nullable: true
```

### Complex Data Types

Aqueducts supports rich data types including:

- **Basic types**: `string`, `int64`, `float64`, `bool`, `date32`
- **Complex types**: `list<string>`, `struct<name:string,age:int32>`
- **Temporal types**: `timestamp<millisecond,UTC>`, `date32`
- **Decimal types**: `decimal<10,2>` (precision, scale)

See the [Schema Reference](schema_reference.md) for complete type documentation.

## Examples

Explore real-world pipeline patterns:

- **[Simple Pipeline](https://github.com/vigimite/aqueducts/blob/main/examples/aqueduct_pipeline_simple.yml)**: Basic CSV processing
- **[Complex Pipeline](https://github.com/vigimite/aqueducts/blob/main/examples/aqueduct_pipeline_example.yml)**: Multi-source Delta operations
- **[ODBC Pipeline](https://github.com/vigimite/aqueducts/blob/main/examples/aqueduct_pipeline_odbc.yml)**: Database integration

## Best Practices

!!! tip "Performance Tips"
    - Use partitioning for large datasets
    - Leverage parallel stages for independent operations
    - Consider Delta Lake for complex update patterns
    - Use explicit schemas for better performance and type safety

!!! warning "Common Pitfalls"
    - Ensure column names match between stages and joins
    - Check data types when joining tables from different sources
    - Be mindful of memory usage with large datasets
