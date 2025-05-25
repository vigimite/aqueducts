//! Destination configuration types and schemas.
//!
//! This module defines all output destination types supported by aqueducts, including
//! file outputs (CSV, JSON, Parquet), databases via ODBC, and Delta Lake tables.

use crate::location::Location;
use crate::serde_helpers::{default_batch_size, default_comma, default_true};
use bon::Builder;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Target output destination for aqueducts pipelines.
///
/// Destinations define where processed data is written and include various formats
/// and storage systems with their specific configuration options.
///
/// # Examples
///
/// ```
/// use aqueducts_schemas::{Destination, FileDestination, DestinationFileType};
/// use std::collections::HashMap;
///
/// // Simple CSV destination - single_file, partition_columns, storage_config use defaults
/// let csv_dest = Destination::File(
///     FileDestination::builder()
///         .name("output".to_string())
///         .location("./output.csv".try_into().unwrap())
///         .format(DestinationFileType::Csv(Default::default()))
///         .build()
/// );
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "schema_gen", derive(schemars::JsonSchema))]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum Destination {
    /// An in-memory destination
    #[serde(alias = "memory", alias = "in_memory", alias = "InMemory")]
    InMemory(InMemoryDestination),
    /// A file output destination
    #[serde(alias = "file", alias = "File")]
    File(FileDestination),
    /// An ODBC insert query to write to a DB table
    #[serde(alias = "odbc", alias = "database", alias = "Odbc")]
    Odbc(OdbcDestination),
    /// A delta table destination
    #[serde(alias = "delta", alias = "Delta")]
    Delta(DeltaDestination),
}

/// An in-memory table destination
#[derive(Debug, Clone, Serialize, Deserialize, Builder)]
#[cfg_attr(feature = "schema_gen", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub struct InMemoryDestination {
    /// Name to register the table with in the provided `SessionContext`
    pub name: String,
}

/// A file output destination
#[derive(Debug, Clone, Serialize, Deserialize, Builder)]
#[cfg_attr(feature = "schema_gen", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub struct FileDestination {
    ///  Name of the file to write
    pub name: String,

    /// Location of the file as a URL e.g. file:///tmp/output.csv, s3://bucket_name/prefix/output.parquet, s3:://bucket_name/prefix
    pub location: Location,

    /// File format, supported types are Parquet and CSV
    #[serde(alias = "file_type")]
    pub format: FileType,

    /// Describes whether to write a single file (can be used to overwrite destination file)
    #[serde(default = "default_true")]
    #[builder(default = default_true())]
    pub single_file: bool,

    /// Columns to partition table by
    #[serde(default, alias = "partition_cols")]
    #[builder(default)]
    pub partition_columns: Vec<String>,

    /// Object store storage configuration
    #[serde(default, alias = "storage_options")]
    #[builder(default)]
    pub storage_config: HashMap<String, String>,
}

/// File type and options for destinations
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "schema_gen", derive(schemars::JsonSchema))]
#[serde(tag = "type", content = "options")]
#[serde(rename_all = "snake_case")]
pub enum FileType {
    /// Parquet options map, please refer to <https://docs.rs/datafusion-common/latest/datafusion_common/config/struct.TableParquetOptions.html> for possible options
    #[serde(alias = "parquet", alias = "Parquet")]
    Parquet(#[serde(default)] HashMap<String, String>),

    /// CSV options
    #[serde(alias = "csv", alias = "Csv")]
    Csv(CsvDestinationOptions),

    /// Json destination, no supported options
    #[serde(alias = "json", alias = "Json")]
    Json,
}

/// CSV destination options
#[derive(Debug, Clone, Serialize, Deserialize, Builder)]
#[cfg_attr(feature = "schema_gen", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub struct CsvDestinationOptions {
    /// Set to `true` to include headers in CSV
    #[serde(default = "default_true")]
    pub has_header: bool,

    /// Set delimiter character to write CSV with
    #[serde(default = "default_comma")]
    pub delimiter: char,

    /// Compression type for CSV output
    #[serde(default)]
    pub compression: Option<String>,
}

impl Default for CsvDestinationOptions {
    fn default() -> Self {
        Self {
            has_header: true,
            delimiter: ',',
            compression: None,
        }
    }
}

/// An ODBC destination
#[derive(Debug, Clone, Serialize, Deserialize, Builder)]
#[cfg_attr(feature = "schema_gen", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub struct OdbcDestination {
    /// Name of the destination
    pub name: String,

    /// ODBC connection string
    /// Please reference the respective database connection string syntax (e.g. <https://www.connectionstrings.com/postgresql-odbc-driver-psqlodbc/>)
    pub connection_string: String,

    /// Strategy for performing ODBC write operation
    pub write_mode: WriteMode,

    /// Batch size for inserts (defaults to 1000)
    #[serde(default = "default_batch_size")]
    #[builder(default = default_batch_size())]
    pub batch_size: usize,
}

/// Write modes for the `Destination` output.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "schema_gen", derive(schemars::JsonSchema))]
#[serde(tag = "operation", content = "transaction", rename_all = "snake_case")]
pub enum WriteMode {
    /// `Append`: appends data to the `Destination`
    #[serde(alias = "append", alias = "Append")]
    Append,

    /// `Custom`: Inserts data with a prepared stament. Option to perform any number of (non-insert) preliminary statements
    #[serde(alias = "custom", alias = "Custom")]
    Custom(CustomStatements),
}

/// SQL statements for `Custom` write mode.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Builder)]
#[cfg_attr(feature = "schema_gen", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub struct CustomStatements {
    /// Optional (non-insert) preliminary statement
    pub pre_insert: Option<String>,

    /// Insert prepared statement
    pub insert: String,
}

// Delta destination configuration (will be used by aqueducts-delta crate)
/// A delta table destination
#[derive(Debug, Clone, Serialize, Deserialize, Builder)]
#[cfg_attr(feature = "schema_gen", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub struct DeltaDestination {
    /// Name of the destination
    pub name: String,

    /// A URL or Path to the location of the delta table
    /// Supports relative local paths
    pub location: Location,

    /// Write mode for the delta destination
    #[serde(alias = "mode")]
    pub write_mode: DeltaWriteMode,

    /// Storage configuration for the delta table
    /// Please reference the delta-rs github repo for more information on available keys (e.g. <https://github.com/delta-io/delta-rs/blob/main/crates/aws/src/storage.rs>)
    /// additionally also reference the `object_store` docs (e.g. <https://docs.rs/object_store/latest/object_store/aws/enum.AmazonS3ConfigKey.html>)
    #[serde(default, alias = "storage_options")]
    #[builder(default)]
    pub storage_config: HashMap<String, String>,

    /// Partition columns for the delta table
    #[serde(default, alias = "partition_cols")]
    #[builder(default)]
    pub partition_columns: Vec<String>,

    /// DeltaTable table properties: <https://docs.delta.io/latest/table-properties.html>
    #[serde(default)]
    #[builder(default)]
    pub table_properties: HashMap<String, Option<String>>,

    /// Custom metadata to include with the table creation
    #[serde(default, alias = "custom_metadata")]
    #[builder(default)]
    pub metadata: HashMap<String, String>,

    /// Table schema definition using universal Field types
    #[serde(default)]
    #[builder(default)]
    pub schema: Vec<crate::data_types::Field>,
}

/// Write mode for delta destinations
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "schema_gen", derive(schemars::JsonSchema))]
#[serde(tag = "operation", content = "params")]
#[serde(rename_all = "snake_case")]
pub enum DeltaWriteMode {
    /// Append data to the destination table
    #[serde(alias = "append", alias = "Append")]
    Append,
    /// Upsert data using the specified merge columns for uniqueness
    #[serde(alias = "upsert", alias = "Upsert")]
    Upsert(Vec<String>),
    /// Replace data matching the specified conditions
    #[serde(alias = "replace", alias = "Replace")]
    Replace(Vec<ReplaceCondition>),
}

/// Condition used to build a predicate for data replacement.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "schema_gen", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub struct ReplaceCondition {
    /// Column name to match against
    pub column: String,
    /// Value to match for replacement
    pub value: String,
}
