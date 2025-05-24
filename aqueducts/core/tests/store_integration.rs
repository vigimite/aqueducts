mod common;

use aqueducts_core::store::{register_object_store, ObjectStoreRegistry};
use aqueducts_schemas::{
    Aqueduct, CsvSourceOptions, Destination, DestinationFileType, FileDestination, FileSource,
    Location, Source, SourceFileType, Stage,
};
use common::*;
use datafusion::prelude::*;
use std::collections::HashMap;
use std::fs;
use std::sync::Arc;
use tempfile::TempDir;
use url::Url;

#[tokio::test]
async fn test_file_source_with_new_store_registration() {
    // Create test data using helper
    let dataset = TestDataSet::new().unwrap();

    // Create file source using bon builder
    let source = Source::File(
        FileSource::builder()
            .name("test_csv".to_string())
            .format(SourceFileType::Csv(CsvSourceOptions::default()))
            .location(dataset.csv_url.clone().into())
            .build(),
    );

    // Create context and run pipeline
    let ctx = Arc::new(SessionContext::new());

    // Test that our new store registration works
    aqueducts_core::sources::register_source(ctx.clone(), source)
        .await
        .unwrap();

    // Verify the table was registered
    let table = ctx.table("test_csv").await.unwrap();
    let result = table.collect().await.unwrap();

    assert!(!result.is_empty());
    assert_eq!(result[0].num_rows(), dataset.expected_rows());
}

#[tokio::test]
async fn test_memory_store_functionality() -> Result<(), Box<dyn std::error::Error>> {
    let ctx = Arc::new(SessionContext::new());
    let memory_url = Url::parse("memory://test")?;
    let options = HashMap::new();

    // Test that memory store registration works
    register_object_store(ctx.clone(), &memory_url, &options)?;

    Ok(())
}

#[tokio::test]
async fn test_store_registry_with_different_schemes() -> Result<(), Box<dyn std::error::Error>> {
    let registry = ObjectStoreRegistry::new();
    let options = HashMap::new();

    // Test local file system
    let file_url = Url::parse("file:///tmp/test")?;
    let store = registry.create_store(&file_url, &options)?;
    assert!(!store.to_string().is_empty());

    // Test memory store
    let memory_url = Url::parse("memory://test")?;
    let store = registry.create_store(&memory_url, &options)?;
    assert!(!store.to_string().is_empty());

    Ok(())
}

#[tokio::test]
async fn test_end_to_end_pipeline_with_new_store() -> Result<(), Box<dyn std::error::Error>> {
    // Create test data
    let temp_dir = TempDir::new()?;
    let input_path = temp_dir.path().join("input.csv");
    let output_path = temp_dir.path().join("output.parquet");

    let csv_content = "id,name,score\n1,Alice,95\n2,Bob,87\n3,Charlie,92\n";
    fs::write(&input_path, csv_content)?;

    // Create aqueduct pipeline
    let aqueduct = Aqueduct {
        version: "1.0.0".to_string(),
        sources: vec![Source::File(FileSource {
            name: "input".to_string(),
            format: SourceFileType::Csv(CsvSourceOptions::default()),
            location: Location(Url::from_file_path(&input_path).unwrap()),
            storage_config: HashMap::new(),
        })],
        stages: vec![vec![Stage {
            name: "filtered".to_string(),
            query: "SELECT id, name, score FROM input WHERE score > 90".to_string(),
            show: None,
            explain: false,
            explain_analyze: false,
            print_schema: false,
        }]],
        destination: Some(Destination::File(FileDestination {
            name: "output".to_string(),
            location: Location(Url::from_file_path(&output_path).unwrap()),
            format: DestinationFileType::Parquet(HashMap::new()),
            single_file: true,
            partition_columns: Vec::new(),
            storage_config: HashMap::new(),
        })),
    };

    // Run the pipeline
    let ctx = Arc::new(SessionContext::new());
    let result_ctx = aqueducts_core::run_pipeline(ctx, aqueduct, None).await?;

    // Verify output file was created
    assert!(output_path.exists());

    // Verify we can read the output
    let output_table = result_ctx
        .read_parquet(output_path.to_str().unwrap(), ParquetReadOptions::default())
        .await?;
    let output_data = output_table.collect().await?;

    // Should have 2 rows (Alice: 95, Charlie: 92) since score > 90
    assert_eq!(output_data[0].num_rows(), 2);

    Ok(())
}

#[cfg(feature = "s3")]
#[tokio::test]
async fn test_s3_provider_configuration_parsing() -> Result<(), Box<dyn std::error::Error>> {
    use aqueducts_core::store::ObjectStoreProvider;
    use aqueducts_core::store::S3Provider;

    let provider = S3Provider;
    let url = Url::parse("s3://test-bucket/path")?;

    // Test with minimal configuration (this will fail authentication but should parse correctly)
    let mut options = HashMap::new();
    options.insert("aws_region".to_string(), "us-east-1".to_string());
    options.insert("aws_access_key_id".to_string(), "test_key".to_string());
    options.insert(
        "aws_secret_access_key".to_string(),
        "test_secret".to_string(),
    );

    // This should create the store builder without errors (though authentication would fail)
    let result = provider.create_store(&url, &options);

    // We expect this to fail due to invalid credentials, but not due to parsing errors
    // The specific error depends on whether it tries to validate credentials immediately
    match result {
        Ok(_) => {
            // Store was created successfully (credentials not validated immediately)
        }
        Err(e) => {
            // Should be a configuration/authentication error, not a parsing error
            let error_string = e.to_string();
            assert!(!error_string.contains("parsing") && !error_string.contains("Unsupported"));
        }
    }

    Ok(())
}

#[cfg(feature = "gcs")]
#[tokio::test]
async fn test_gcs_provider_configuration_parsing() -> Result<(), Box<dyn std::error::Error>> {
    use aqueducts_core::store::GcsProvider;
    use aqueducts_core::store::ObjectStoreProvider;

    let provider = GcsProvider;
    let url = Url::parse("gs://test-bucket/path")?;

    // Test with minimal configuration
    let mut options = HashMap::new();
    options.insert(
        "google_service_account".to_string(),
        "/fake/path/to/service-account.json".to_string(),
    );

    let result = provider.create_store(&url, &options);

    match result {
        Ok(_) => {
            // Store was created successfully
        }
        Err(e) => {
            // Should be a configuration/authentication error, not a parsing error
            let error_string = e.to_string();
            assert!(!error_string.contains("Unsupported"));
        }
    }

    Ok(())
}

#[cfg(feature = "azure")]
#[tokio::test]
async fn test_azure_provider_configuration_parsing() -> Result<(), Box<dyn std::error::Error>> {
    use aqueducts_core::store::AzureProvider;
    use aqueducts_core::store::ObjectStoreProvider;

    let provider = AzureProvider;
    let url = Url::parse("azure://account.blob.core.windows.net/container/path")?;

    // Test with minimal configuration
    let mut options = HashMap::new();
    options.insert(
        "azure_storage_account_name".to_string(),
        "testaccount".to_string(),
    );
    options.insert(
        "azure_storage_account_key".to_string(),
        "fake_key".to_string(),
    );

    let result = provider.create_store(&url, &options);

    match result {
        Ok(_) => {
            // Store was created successfully
        }
        Err(e) => {
            // Should be a configuration/authentication error, not a parsing error
            let error_string = e.to_string();
            assert!(!error_string.contains("Unsupported"));
        }
    }

    Ok(())
}
