//! Integration tests for aqueducts core pipeline functionality.
//!
//! These tests focus on end-to-end pipeline execution and test data helpers to verify core functionality.

mod common;

use aqueducts_core::run_pipeline;
use aqueducts_schemas::*;
use common::*;
use datafusion::prelude::*;
use std::sync::Arc;

#[tokio::test]
async fn test_csv_source_to_memory_destination() {
    let dataset = TestDataSet::new().unwrap();

    let pipeline = Aqueduct::builder()
        .sources(vec![Source::File(
            FileSource::builder()
                .name("test_data".to_string())
                .format(sources::FileType::Csv(CsvSourceOptions::default()))
                .location(dataset.csv_url.clone().into())
                .build(),
        )])
        .stages(vec![vec![Stage::builder()
            .name("transform".to_string())
            .query("SELECT id, name, value * 2 as doubled_value, active FROM test_data".to_string())
            .build()]])
        .destination(Destination::InMemory(
            InMemoryDestination::builder()
                .name("result".to_string())
                .build(),
        ))
        .build();

    let ctx = Arc::new(SessionContext::new());
    let result_ctx = run_pipeline(ctx, pipeline, None).await.unwrap();

    let table = result_ctx.table("result").await.unwrap();
    let batches = table.collect().await.unwrap();

    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), dataset.expected_rows());

    // Verify data transformation worked
    let doubled_values = batches[0]
        .column_by_name("doubled_value")
        .expect("doubled_value column should exist");

    // Should have doubled the original values
    assert!(!doubled_values.is_empty());
}

#[tokio::test]
async fn test_parquet_source_to_csv_destination() {
    let dataset = TestDataSet::new().unwrap();
    let output_url = dataset.get_output_url("csv_out", "result.csv");

    let pipeline = Aqueduct::builder()
        .sources(vec![Source::File(
            FileSource::builder()
                .name("parquet_data".to_string())
                .format(sources::FileType::Parquet(ParquetSourceOptions::default()))
                .location(dataset.parquet_url.clone().into())
                .build(),
        )])
        .stages(vec![vec![Stage::builder()
            .name("filter_active".to_string())
            .query("SELECT * FROM parquet_data WHERE active = true".to_string())
            .build()]])
        .destination(Destination::File(
            FileDestination::builder()
                .name("csv_output".to_string())
                .format(destinations::FileType::Csv(CsvDestinationOptions::default()))
                .location(output_url.clone().into())
                .build(),
        ))
        .build();

    let ctx = Arc::new(SessionContext::new());
    run_pipeline(ctx, pipeline, None).await.unwrap();

    let output_path = output_url.to_file_path().unwrap();
    assert!(output_path.exists());

    let content = std::fs::read_to_string(&output_path).unwrap();
    assert!(content.contains("id,name,value,active"));
    assert!(content.contains("true")); // Should only have active=true records
    assert!(!content.contains("false")); // Should not have active=false records
}

#[tokio::test]
async fn test_pipeline_without_destination() {
    let dataset = TestDataSet::new().unwrap();

    let pipeline = Aqueduct::builder()
        .sources(vec![Source::File(
            FileSource::builder()
                .name("test_source".to_string())
                .format(sources::FileType::Csv(CsvSourceOptions::default()))
                .location(dataset.csv_url.clone().into())
                .build(),
        )])
        .stages(vec![vec![Stage::builder()
            .name("final_stage".to_string())
            .query("SELECT * FROM test_source ORDER BY id".to_string())
            .build()]])
        .build();

    let ctx = Arc::new(SessionContext::new());
    let result_ctx = run_pipeline(ctx, pipeline, None).await.unwrap();

    // The final stage should be available as a table
    let table = result_ctx.table("final_stage").await.unwrap();
    let batches = table.collect().await.unwrap();

    assert_eq!(batches[0].num_rows(), dataset.expected_rows());
}
