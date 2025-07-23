use aqueducts_delta::{
    register_delta_source, {prepare_delta_destination, write_to_delta_destination},
};
use aqueducts_schemas::{DeltaSource, DeltaWriteMode, ReplaceCondition};
use datafusion::{
    arrow::{
        array::{ArrayRef, Int32Array, StringArray},
        datatypes::{DataType as ArrowDataType, Field as ArrowField},
        record_batch::RecordBatch,
    },
    assert_batches_eq,
    execution::context::SessionContext,
};
use std::{collections::HashMap, sync::Arc};
use tempfile::TempDir;
use url::Url;

fn generate_test_table_path(test_name: &str) -> (TempDir, Url) {
    let temp_dir = TempDir::new().expect("Failed to create temp directory");
    let table_path = temp_dir.path().join(test_name).join("test_table");

    std::fs::create_dir_all(&table_path).expect("Failed to create table directory");

    let url = Url::from_file_path(&table_path).expect("Failed to create file URL");

    (temp_dir, url)
}

fn create_test_arrow_schema() -> Vec<ArrowField> {
    vec![
        ArrowField::new("col_1", ArrowDataType::Utf8, false),
        ArrowField::new("col_2", ArrowDataType::Int32, false),
    ]
}

#[tokio::test]
async fn test_delta_source_register_ok() {
    let ctx = Arc::new(SessionContext::new());

    // Create test data first
    let (_temp_dir, location) = generate_test_table_path("source_test");

    // Create some test data
    let col_1 = Arc::new(StringArray::from(vec!["a", "b", "c"])) as ArrayRef;
    let col_2 = Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef;

    let batch = RecordBatch::try_from_iter(vec![("col_1", col_1), ("col_2", col_2)]).unwrap();

    let df = ctx.read_batch(batch).unwrap();

    // Write data to create a delta table first
    let arrow_schema = create_test_arrow_schema();
    let storage_config = HashMap::new();
    let partition_columns = vec![];
    let table_properties = HashMap::new();

    // Create and write to the table
    prepare_delta_destination(
        "test_table",
        location.as_str(),
        &storage_config,
        &partition_columns,
        &table_properties,
        &arrow_schema,
    )
    .await
    .unwrap();

    write_to_delta_destination(
        "test_table",
        location.as_str(),
        &datafusion::arrow::datatypes::Schema::new(arrow_schema),
        &storage_config,
        &DeltaWriteMode::Append,
        df,
    )
    .await
    .unwrap();

    // Now test reading it as a source
    let source_config = DeltaSource {
        name: "source_table".to_string(),
        location: aqueducts_schemas::Location(location),
        version: None,
        timestamp: None,
        storage_config: HashMap::new(),
    };

    register_delta_source(ctx.clone(), &source_config)
        .await
        .unwrap();

    // Verify we can query the registered table
    let result = ctx
        .sql("SELECT * FROM source_table ORDER BY col_1")
        .await
        .unwrap();
    let batches = result.collect().await.unwrap();

    assert_batches_eq!(
        [
            "+-------+-------+",
            "| col_1 | col_2 |",
            "+-------+-------+",
            "| a     | 1     |",
            "| b     | 2     |",
            "| c     | 3     |",
            "+-------+-------+",
        ],
        batches.as_slice()
    );
}

#[tokio::test]
async fn test_delta_destination_append_ok() {
    let ctx = Arc::new(SessionContext::new());
    let (_temp_dir, location) = generate_test_table_path("append_test");

    let arrow_schema = create_test_arrow_schema();
    let storage_config = HashMap::new();
    let partition_columns = vec![];
    let table_properties = HashMap::new();

    // Prepare the destination
    prepare_delta_destination(
        "test_table",
        location.as_str(),
        &storage_config,
        &partition_columns,
        &table_properties,
        &arrow_schema,
    )
    .await
    .unwrap();

    // Create test data
    let col_1 = Arc::new(StringArray::from(vec!["x", "y", "z"])) as ArrayRef;
    let col_2 = Arc::new(Int32Array::from(vec![10, 20, 30])) as ArrayRef;

    let batch = RecordBatch::try_from_iter(vec![("col_1", col_1), ("col_2", col_2)]).unwrap();

    let df = ctx.read_batch(batch).unwrap();

    // Write the data
    write_to_delta_destination(
        "test_table",
        location.as_str(),
        &datafusion::arrow::datatypes::Schema::new(arrow_schema),
        &storage_config,
        &DeltaWriteMode::Append,
        df,
    )
    .await
    .unwrap();

    // Verify data was written by reading it back
    let source_config = DeltaSource {
        name: "result_table".to_string(),
        location: aqueducts_schemas::Location(location),
        version: None,
        timestamp: None,
        storage_config: HashMap::new(),
    };

    register_delta_source(ctx.clone(), &source_config)
        .await
        .unwrap();

    let result = ctx
        .sql("SELECT * FROM result_table ORDER BY col_1")
        .await
        .unwrap();
    let batches = result.collect().await.unwrap();

    assert_batches_eq!(
        [
            "+-------+-------+",
            "| col_1 | col_2 |",
            "+-------+-------+",
            "| x     | 10    |",
            "| y     | 20    |",
            "| z     | 30    |",
            "+-------+-------+",
        ],
        batches.as_slice()
    );
}

#[tokio::test]
async fn test_delta_destination_upsert_ok() {
    let ctx = Arc::new(SessionContext::new());
    let (_temp_dir, location) = generate_test_table_path("upsert_test");

    let arrow_schema = create_test_arrow_schema();
    let storage_config = HashMap::new();
    let partition_columns = vec![];
    let table_properties = HashMap::new();

    // Prepare the destination
    prepare_delta_destination(
        "test_table",
        location.as_str(),
        &storage_config,
        &partition_columns,
        &table_properties,
        &arrow_schema,
    )
    .await
    .unwrap();

    // Create initial test data
    let col_1 = Arc::new(StringArray::from(vec!["a", "b", "c"])) as ArrayRef;
    let col_2 = Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef;

    let batch = RecordBatch::try_from_iter(vec![("col_1", col_1), ("col_2", col_2)]).unwrap();

    let df = ctx.read_batch(batch).unwrap();

    // Write initial data
    write_to_delta_destination(
        "test_table",
        location.as_str(),
        &datafusion::arrow::datatypes::Schema::new(arrow_schema.clone()),
        &storage_config,
        &DeltaWriteMode::Upsert(vec!["col_1".to_string()]),
        df,
    )
    .await
    .unwrap();

    // Create updated data (should upsert)
    let col_1_update = Arc::new(StringArray::from(vec!["a", "d"])) as ArrayRef;
    let col_2_update = Arc::new(Int32Array::from(vec![100, 4])) as ArrayRef;

    let batch_update =
        RecordBatch::try_from_iter(vec![("col_1", col_1_update), ("col_2", col_2_update)]).unwrap();

    let df_update = ctx.read_batch(batch_update).unwrap();

    // Write updated data
    write_to_delta_destination(
        "test_table",
        location.as_str(),
        &datafusion::arrow::datatypes::Schema::new(arrow_schema),
        &storage_config,
        &DeltaWriteMode::Upsert(vec!["col_1".to_string()]),
        df_update,
    )
    .await
    .unwrap();

    // Verify the result - should have upserted 'a' and added 'd'
    let source_config = DeltaSource {
        name: "result_table".to_string(),
        location: aqueducts_schemas::Location(location),
        version: None,
        timestamp: None,
        storage_config: HashMap::new(),
    };

    register_delta_source(ctx.clone(), &source_config)
        .await
        .unwrap();

    let result = ctx
        .sql("SELECT * FROM result_table ORDER BY col_1")
        .await
        .unwrap();
    let batches = result.collect().await.unwrap();

    assert!(!batches.is_empty());
}

#[tokio::test]
async fn test_delta_destination_replace_ok() {
    let ctx = Arc::new(SessionContext::new());
    let (_temp_dir, location) = generate_test_table_path("replace_test");

    let arrow_schema = create_test_arrow_schema();
    let storage_config = HashMap::new();
    let partition_columns = vec![];
    let table_properties = HashMap::new();

    // Prepare the destination
    prepare_delta_destination(
        "test_table",
        location.as_str(),
        &storage_config,
        &partition_columns,
        &table_properties,
        &arrow_schema,
    )
    .await
    .unwrap();

    // Create replacement data that matches the replace condition
    let col_1 = Arc::new(StringArray::from(vec!["a", "a"])) as ArrayRef;
    let col_2 = Arc::new(Int32Array::from(vec![999, 888])) as ArrayRef;

    let batch = RecordBatch::try_from_iter(vec![("col_1", col_1), ("col_2", col_2)]).unwrap();

    let df = ctx.read_batch(batch).unwrap();

    // Write replacement data
    write_to_delta_destination(
        "test_table",
        location.as_str(),
        &datafusion::arrow::datatypes::Schema::new(arrow_schema),
        &storage_config,
        &DeltaWriteMode::Replace(vec![ReplaceCondition {
            column: "col_1".to_string(),
            value: "a".to_string(),
        }]),
        df,
    )
    .await
    .unwrap();

    // Verify data was written
    let source_config = DeltaSource {
        name: "result_table".to_string(),
        location: aqueducts_schemas::Location(location),
        version: None,
        timestamp: None,
        storage_config: HashMap::new(),
    };

    register_delta_source(ctx.clone(), &source_config)
        .await
        .unwrap();

    let result = ctx
        .sql("SELECT * FROM result_table ORDER BY col_1, col_2")
        .await
        .unwrap();
    let batches = result.collect().await.unwrap();

    assert_batches_eq!(
        [
            "+-------+-------+",
            "| col_1 | col_2 |",
            "+-------+-------+",
            "| a     | 888   |",
            "| a     | 999   |",
            "+-------+-------+",
        ],
        batches.as_slice()
    );
}
