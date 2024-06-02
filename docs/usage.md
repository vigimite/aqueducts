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

