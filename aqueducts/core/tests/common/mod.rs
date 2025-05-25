//! Common test utilities for aqueducts core testing.
//!
//! This module provides test data generation helpers and utilities
//! that can be shared between unit tests and integration tests.

use datafusion::arrow::array::{ArrayRef, BooleanArray, Float64Array, Int32Array, StringArray};
use datafusion::arrow::datatypes::{
    DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema,
};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::parquet::arrow::arrow_writer::ArrowWriter;
use datafusion::parquet::file::properties::WriterProperties;
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tempfile::TempDir;
use url::Url;

/// Sample data structure for generating test files
#[derive(Debug, Clone)]
pub struct TestRecord {
    pub id: i32,
    pub name: String,
    pub value: f64,
    pub active: bool,
}

impl TestRecord {
    pub fn new(id: i32, name: &str, value: f64, active: bool) -> Self {
        Self {
            id,
            name: name.to_string(),
            value,
            active,
        }
    }
}

/// Default test dataset with varied data types
pub fn default_test_data() -> Vec<TestRecord> {
    vec![
        TestRecord::new(1, "Alice", 100.5, true),
        TestRecord::new(2, "Bob", 200.0, false),
        TestRecord::new(3, "Charlie", 300.75, true),
        TestRecord::new(4, "Diana", 150.25, false),
        TestRecord::new(5, "Eve", 250.0, true),
    ]
}

/// Generate test CSV file and return its URL
pub fn generate_test_csv<P: AsRef<Path>>(
    path: P,
    data: &[TestRecord],
) -> Result<Url, Box<dyn std::error::Error>> {
    let mut file = File::create(&path)?;

    // Write header
    writeln!(file, "id,name,value,active")?;

    // Write data rows
    for record in data {
        writeln!(
            file,
            "{},{},{},{}",
            record.id, record.name, record.value, record.active
        )?;
    }

    file.flush()?;

    let url = Url::from_file_path(path.as_ref()).map_err(|_| "Failed to create file URL")?;

    Ok(url)
}

/// Generate test JSONL (newline-delimited JSON) file and return its URL
pub fn generate_test_jsonl<P: AsRef<Path>>(
    path: P,
    data: &[TestRecord],
) -> Result<Url, Box<dyn std::error::Error>> {
    let mut file = File::create(&path)?;

    for record in data {
        let json_line = format!(
            r#"{{"id":{},"name":"{}","value":{},"active":{}}}"#,
            record.id, record.name, record.value, record.active
        );
        writeln!(file, "{}", json_line)?;
    }

    file.flush()?;

    let url = Url::from_file_path(path.as_ref()).map_err(|_| "Failed to create file URL")?;

    Ok(url)
}

/// Generate test Parquet file and return its URL
pub fn generate_test_parquet<P: AsRef<Path>>(
    path: P,
    data: &[TestRecord],
) -> Result<Url, Box<dyn std::error::Error>> {
    // Create Arrow schema
    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", ArrowDataType::Int32, false),
        ArrowField::new("name", ArrowDataType::Utf8, false),
        ArrowField::new("value", ArrowDataType::Float64, false),
        ArrowField::new("active", ArrowDataType::Boolean, false),
    ]));

    // Create Arrow arrays from data
    let ids: Vec<i32> = data.iter().map(|r| r.id).collect();
    let names: Vec<String> = data.iter().map(|r| r.name.clone()).collect();
    let values: Vec<f64> = data.iter().map(|r| r.value).collect();
    let actives: Vec<bool> = data.iter().map(|r| r.active).collect();

    let id_array = Arc::new(Int32Array::from(ids)) as ArrayRef;
    let name_array = Arc::new(StringArray::from(names)) as ArrayRef;
    let value_array = Arc::new(Float64Array::from(values)) as ArrayRef;
    let active_array = Arc::new(BooleanArray::from(actives)) as ArrayRef;

    // Create record batch
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![id_array, name_array, value_array, active_array],
    )?;

    // Write to Parquet file
    let file = File::create(&path)?;
    let props = WriterProperties::builder().build();
    let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
    writer.write(&batch)?;
    writer.close()?;

    let url = Url::from_file_path(path.as_ref()).map_err(|_| "Failed to create file URL")?;

    Ok(url)
}

/// Create a temporary directory with test files of all formats
pub struct TestDataSet {
    #[allow(dead_code)]
    pub temp_dir: TempDir,
    pub csv_url: Url,
    #[allow(dead_code)]
    pub jsonl_url: Url,
    #[allow(dead_code)]
    pub parquet_url: Url,
    pub data: Vec<TestRecord>,
}

impl TestDataSet {
    pub fn new() -> Result<Self, Box<dyn std::error::Error>> {
        Self::with_data(&default_test_data())
    }

    pub fn with_data(data: &[TestRecord]) -> Result<Self, Box<dyn std::error::Error>> {
        let temp_dir = TempDir::new()?;
        let base_path = temp_dir.path();

        let csv_path = base_path.join("test_data.csv");
        let jsonl_path = base_path.join("test_data.jsonl");
        let parquet_path = base_path.join("test_data.parquet");

        let csv_url = generate_test_csv(&csv_path, data)?;
        let jsonl_url = generate_test_jsonl(&jsonl_path, data)?;
        let parquet_url = generate_test_parquet(&parquet_path, data)?;

        Ok(Self {
            temp_dir,
            csv_url,
            jsonl_url,
            parquet_url,
            data: data.to_vec(),
        })
    }

    /// Get expected row count for validation
    pub fn expected_rows(&self) -> usize {
        self.data.len()
    }

    /// Create output directory for testing persistence
    #[allow(dead_code)]
    pub fn create_output_dir(&self, name: &str) -> PathBuf {
        let output_path = self.temp_dir.path().join("output").join(name);
        std::fs::create_dir_all(&output_path).expect("Failed to create output directory");
        output_path
    }

    /// Get output file URL for testing persistence
    #[allow(dead_code)]
    pub fn get_output_url(&self, dir_name: &str, file_name: &str) -> Url {
        let output_path = self.create_output_dir(dir_name).join(file_name);
        Url::from_file_path(output_path).expect("Failed to create output URL")
    }
}

/// Generate larger dataset for performance testing
#[allow(dead_code)]
pub fn generate_large_dataset(size: usize) -> Vec<TestRecord> {
    (0..size)
        .map(|i| {
            TestRecord::new(
                i as i32,
                &format!("user_{}", i),
                (i as f64) * 1.5,
                i % 2 == 0,
            )
        })
        .collect()
}

/// Generate dataset with specific patterns for testing edge cases
#[allow(dead_code)]
pub fn generate_edge_case_dataset() -> Vec<TestRecord> {
    vec![
        // Empty string
        TestRecord::new(1, "", 0.0, false),
        // Special characters
        TestRecord::new(2, "Name,with,commas", 1.0, true),
        TestRecord::new(3, "Name\nwith\nnewlines", 2.0, false),
        TestRecord::new(4, "Name\"with\"quotes", 3.0, true),
        // Unicode
        TestRecord::new(5, "José García", 4.0, false),
        TestRecord::new(6, "中文名字", 5.0, true),
        // Numbers as edge cases
        TestRecord::new(i32::MAX, "max_id", f64::MAX, true),
        TestRecord::new(i32::MIN, "min_id", f64::MIN, false),
    ]
}

/// Helper to create test Arrow record batch with sample data
#[allow(dead_code)]
pub fn create_test_record_batch() -> Result<RecordBatch, Box<dyn std::error::Error>> {
    let col_1 = Arc::new(StringArray::from(vec!["a", "b", "c", "d"])) as ArrayRef;
    let col_2 = Arc::new(Int32Array::from(vec![1, 2, 3, 4])) as ArrayRef;
    let col_3 = Arc::new(BooleanArray::from(vec![true, true, false, false])) as ArrayRef;
    let col_4 = Arc::new(Int32Array::from(vec![10, 20, 30, 40])) as ArrayRef;

    let batch = RecordBatch::try_from_iter(vec![
        ("col_str", col_1),
        ("col_int", col_2),
        ("col_bool", col_3),
        ("col_val", col_4),
    ])?;

    Ok(batch)
}
