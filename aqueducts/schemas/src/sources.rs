//! Source configuration types and schemas.
//!
//! This module defines all data source types supported by aqueducts, including
//! file-based sources (CSV, JSON, Parquet), directories, databases via ODBC,
//! and Delta Lake tables.

use crate::data_types::{DataType, Field};
use crate::location::Location;
use crate::serde_helpers::{default_comma, default_true, deserialize_partition_columns};
use bon::Builder;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// A data source for aqueducts pipelines.
///
/// Sources define where data is read from and include various formats and storage systems.
/// Each source type has specific configuration options for its format and location.
///
/// # Examples
///
/// ```
/// use aqueducts_schemas::{Source, FileSource, SourceFileType, CsvSourceOptions};
///
/// // CSV file source - storage_config defaults to empty HashMap
/// let csv_source = Source::File(
///     FileSource::builder()
///         .name("sales_data".to_string())
///         .format(SourceFileType::Csv(CsvSourceOptions::default()))
///         .location("./data/sales.csv".try_into().unwrap())
///         .build()
/// );
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "schema_gen", derive(schemars::JsonSchema))]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum Source {
    /// An in-memory source
    #[serde(alias = "memory", alias = "in_memory", alias = "InMemory")]
    InMemory(InMemorySource),
    /// A file source
    #[serde(alias = "file", alias = "File")]
    File(FileSource),
    /// A directory source
    #[serde(alias = "directory", alias = "dir", alias = "Directory")]
    Directory(DirSource),
    /// An ODBC source
    #[serde(alias = "odbc", alias = "database", alias = "Odbc")]
    Odbc(OdbcSource),
    /// A delta table source
    #[serde(alias = "delta", alias = "Delta")]
    Delta(DeltaSource),
}

impl Source {
    pub fn name(&self) -> String {
        match self {
            Source::InMemory(in_memory_source) => in_memory_source.name.clone(),
            Source::File(file_source) => file_source.name.clone(),
            Source::Directory(dir_source) => dir_source.name.clone(),
            Source::Odbc(odbc_source) => odbc_source.name.clone(),
            Source::Delta(delta_source) => delta_source.name.clone(),
        }
    }
}

/// An in memory source already present in the provided session context
#[derive(Debug, Clone, Serialize, Deserialize, Builder)]
#[cfg_attr(feature = "schema_gen", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub struct InMemorySource {
    /// Name of the in-memory table, existence will be checked at runtime
    pub name: String,
}

/// A file source
#[derive(Debug, Clone, Serialize, Deserialize, Builder)]
#[cfg_attr(feature = "schema_gen", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub struct FileSource {
    /// Name of the file source, will be the registered table name in the SQL context
    pub name: String,

    /// File format of the file to be ingested
    /// Supports `Parquet` for parquet files, `Csv` for CSV files and `Json` for JSON files
    #[serde(alias = "file_type")]
    pub format: SourceFileType,

    /// A URL or Path to the location of the file
    /// Supports relative local paths
    pub location: Location,

    /// Storage configuration for the file
    /// Please reference the delta-rs github repo for more information on available keys (e.g. <https://github.com/delta-io/delta-rs/blob/main/crates/aws/src/storage.rs>)
    /// additionally also reference the `object_store` docs (e.g. <https://docs.rs/object_store/latest/object_store/aws/enum.AmazonS3ConfigKey.html>)
    #[serde(default, alias = "storage_options")]
    #[builder(default)]
    pub storage_config: HashMap<String, String>,
}

/// A Directory Source
#[derive(Debug, Clone, Serialize, Deserialize, Builder)]
#[cfg_attr(feature = "schema_gen", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub struct DirSource {
    /// Name of the directory source, will be the registered table name in the SQL context
    pub name: String,

    /// File format of the files to be ingested
    /// Supports `Parquet` for parquet files, `Csv` for CSV files and `Json` for JSON files
    #[serde(alias = "file_type")]
    pub format: SourceFileType,

    /// Columns to partition the table by
    /// This is a list of key value tuples where the key is the column name and the value is a DataType
    #[serde(
        default,
        alias = "partition_cols",
        deserialize_with = "deserialize_partition_columns"
    )]
    #[builder(default)]
    pub partition_columns: Vec<(String, DataType)>,

    /// A URL or Path to the location of the directory
    /// Supports relative local paths
    pub location: Location,

    /// Storage configuration for the directory
    /// Please reference the delta-rs github repo for more information on available keys (e.g. <https://github.com/delta-io/delta-rs/blob/main/crates/aws/src/storage.rs>)
    /// additionally also reference the `object_store` docs (e.g. <https://docs.rs/object_store/latest/object_store/aws/enum.AmazonS3ConfigKey.html>)
    #[serde(default, alias = "storage_options")]
    #[builder(default)]
    pub storage_config: HashMap<String, String>,
}

/// An ODBC source
#[derive(Debug, Clone, Serialize, Deserialize, Builder)]
#[cfg_attr(feature = "schema_gen", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub struct OdbcSource {
    /// Name of the ODBC source, will be the registered table name in the SQL context
    pub name: String,

    /// Query to execute when fetching data from the ODBC connection
    /// This query will execute eagerly before the data is processed by the pipeline
    /// Size of data returned from the query cannot exceed work memory
    #[serde(alias = "query")]
    pub load_query: String,

    /// ODBC connection string
    /// Please reference the respective database connection string syntax (e.g. <https://www.connectionstrings.com/postgresql-odbc-driver-psqlodbc/>)
    pub connection_string: String,
}

/// File type of the source file, supports `Parquet`, `Csv` or `Json`
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "schema_gen", derive(schemars::JsonSchema))]
#[serde(tag = "type", content = "options")]
#[serde(rename_all = "snake_case")]
pub enum SourceFileType {
    /// Parquet source options
    #[serde(alias = "parquet", alias = "Parquet")]
    Parquet(ParquetSourceOptions),

    /// Csv source options
    #[serde(alias = "csv", alias = "Csv")]
    Csv(CsvSourceOptions),

    /// Json source options
    #[serde(alias = "json", alias = "Json")]
    Json(JsonSourceOptions),
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, Builder)]
#[cfg_attr(feature = "schema_gen", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub struct ParquetSourceOptions {
    /// schema to read this Parquet with
    /// Schema definition using universal Field types
    #[serde(default)]
    pub schema: Vec<Field>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Builder)]
#[cfg_attr(feature = "schema_gen", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub struct CsvSourceOptions {
    /// set to `true` to treat first row of CSV as the header
    /// column names will be inferred from the header, if there is no header the column names are `column_1, column_2, ... column_x`
    #[serde(default = "default_true")]
    pub has_header: bool,

    /// set a delimiter character to read this CSV with
    #[serde(default = "default_comma")]
    pub delimiter: char,

    /// schema to read this CSV with
    /// Schema definition using universal Field types
    #[serde(default)]
    pub schema: Vec<Field>,
}

impl Default for CsvSourceOptions {
    fn default() -> Self {
        Self {
            has_header: true,
            delimiter: ',',
            schema: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, Builder)]
#[cfg_attr(feature = "schema_gen", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub struct JsonSourceOptions {
    /// schema to read this JSON with
    /// Schema definition using universal Field types
    #[serde(default)]
    pub schema: Vec<Field>,
}

// Delta source configuration (will be used by aqueducts-delta crate)
/// A delta table source
#[derive(Debug, Clone, Serialize, Deserialize, Builder)]
#[cfg_attr(feature = "schema_gen", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub struct DeltaSource {
    /// Name of the delta source, will be the registered table name in the SQL context
    pub name: String,

    /// A URL or Path to the location of the delta table
    /// Supports relative local paths
    pub location: Location,

    /// Storage configuration for the delta table
    /// Please reference the delta-rs github repo for more information on available keys (e.g. <https://github.com/delta-io/delta-rs/blob/main/crates/aws/src/storage.rs>)
    /// additionally also reference the `object_store` docs (e.g. <https://docs.rs/object_store/latest/object_store/aws/enum.AmazonS3ConfigKey.html>)
    #[serde(default, alias = "storage_options")]
    #[builder(default)]
    pub storage_config: HashMap<String, String>,

    /// Delta table version to read from
    /// When unspecified, will read the latest version
    pub version: Option<i64>,

    /// RFC3339 timestamp to read the delta table at
    /// When unspecified, will read the latest version
    pub timestamp: Option<DateTime<Utc>>,
}
