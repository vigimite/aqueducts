//! Models for data sources

use aqueducts_storage::serde::deserialize_file_location;
use chrono::{DateTime, Utc};
use datafusion::arrow::datatypes::{DataType, Schema};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use url::Url;

pub(crate) mod error;
pub(crate) type Result<T> = core::result::Result<T, error::Error>;

use async_trait::async_trait;
use datafusion::execution::context::SessionContext;
use std::sync::Arc;

use crate::execution::sources::register_source;
use crate::execution::traits::SourceProvider;
// Removed redundant imports

/// A data source that can be either a delta table (`delta`), a `file`, a `directory` or an `odbc` connection
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "schema_gen", derive(schemars::JsonSchema))]
#[serde(tag = "type")]
pub enum Source {
    /// An in-memory source
    InMemory(InMemorySource),
    /// A delta table source
    Delta(DeltaSource),
    /// A file source
    File(FileSource),
    /// A directory source
    Directory(DirSource),
    #[cfg(feature = "odbc")]
    /// An ODBC source
    Odbc(OdbcSource),
}

impl Source {
    pub fn name(&self) -> String {
        match self {
            Source::InMemory(in_memory_source) => in_memory_source.name.clone(),
            Source::Delta(delta_source) => delta_source.name.clone(),
            Source::File(file_source) => file_source.name.clone(),
            Source::Directory(dir_source) => dir_source.name.clone(),
            #[cfg(feature = "odbc")]
            Source::Odbc(odbc_source) => odbc_source.name.clone(),
        }
    }
}

#[async_trait]
impl SourceProvider for Source {
    async fn register(
        &self,
        ctx: Arc<SessionContext>,
    ) -> std::result::Result<(), crate::error::Error> {
        register_source(ctx, self.clone())
            .await
            .map_err(crate::error::Error::ModelSourceError)
    }
}

/// An in memory source already present in the provided session context
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "schema_gen", derive(schemars::JsonSchema))]
pub struct InMemorySource {
    /// Name of the in-memory table, existence will be checked at runtime
    pub name: String,
}

/// A delta table source
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "schema_gen", derive(schemars::JsonSchema))]
pub struct DeltaSource {
    /// Name of the delta source, will be the registered table name in the SQL context
    pub name: String,

    /// A URL or Path to the location of the delta table
    /// Supports relative local paths
    #[serde(deserialize_with = "deserialize_file_location")]
    pub location: Url,

    /// A RFC3339 compliant timestamp to load the delta table state at a specific point in time
    /// Used for deltas time traveling feature
    pub version_ts: Option<DateTime<Utc>>,

    /// Storage options for the delta table
    /// Please reference the delta-rs github repo for more information on available keys (e.g. <https://github.com/delta-io/delta-rs/blob/main/crates/aws/src/storage.rs>)
    /// additionally also reference the `object_store` docs (e.g. <https://docs.rs/object_store/latest/object_store/aws/enum.AmazonS3ConfigKey.html>)
    #[serde(default)]
    pub storage_options: HashMap<String, String>,
}

/// A file source
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "schema_gen", derive(schemars::JsonSchema))]
pub struct FileSource {
    /// Name of the file source, will be the registered table name in the SQL context
    pub name: String,

    /// File type of the file to be ingested
    /// Supports `Parquet` for parquet files, `Csv` for CSV files and `Json` for JSON files
    pub file_type: FileType,
    #[serde(deserialize_with = "deserialize_file_location")]

    /// A URL or Path to the location of the delta table
    /// Supports relative local paths
    pub location: Url,

    /// Storage options for the delta table
    /// Please reference the delta-rs github repo for more information on available keys (e.g. <https://github.com/delta-io/delta-rs/blob/main/crates/aws/src/storage.rs>)
    /// additionally also reference the `object_store` docs (e.g. <https://docs.rs/object_store/latest/object_store/aws/enum.AmazonS3ConfigKey.html>)
    #[serde(default)]
    pub storage_options: HashMap<String, String>,
}

/// A Directory Source
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "schema_gen", derive(schemars::JsonSchema))]
pub struct DirSource {
    /// Name of the directory source, will be the registered table name in the SQL context
    pub name: String,

    /// File type of the file to be ingested
    /// Supports `Parquet` for parquet files, `Csv` for CSV files and `Json` for JSON files
    pub file_type: FileType,

    /// Columns to partition the table by
    /// This is a list of key value tuples where the key is the column name and the value is an [arrow::datatypes::DataType](https://docs.rs/arrow/latest/arrow/datatypes/enum.DataType.html)
    #[cfg_attr(feature = "schema_gen", schemars(skip))]
    #[serde(default)]
    pub partition_cols: Vec<(String, DataType)>,

    /// A URL or Path to the location of the delta table
    /// Supports relative local paths
    #[serde(deserialize_with = "deserialize_file_location")]
    pub location: Url,

    /// Storage options for the delta table
    /// Please reference the delta-rs github repo for more information on available keys (e.g. <https://github.com/delta-io/delta-rs/blob/main/crates/aws/src/storage.rs>)
    /// additionally also reference the `object_store` docs (e.g. <https://docs.rs/object_store/latest/object_store/aws/enum.AmazonS3ConfigKey.html>)
    #[serde(default)]
    pub storage_options: HashMap<String, String>,
}

/// An ODBC source
#[cfg(feature = "odbc")]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "schema_gen", derive(schemars::JsonSchema))]
pub struct OdbcSource {
    /// Name of the ODBC source, will be the registered table name in the SQL context
    pub name: String,

    /// Query to execute when fetching data from the ODBC connection
    /// This query will execute eagerly before the data is processed by the pipeline
    /// Size of data returned from the query cannot exceed work memory
    pub query: String,

    /// ODBC connection string
    /// Please reference the respective database connection string syntax (e.g. <https://www.connectionstrings.com/postgresql-odbc-driver-psqlodbc/>)
    pub connection_string: String,
}

/// File type of the source file, supports `Parquet`, `Csv` or `Json`
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "schema_gen", derive(schemars::JsonSchema))]
#[serde(tag = "type", content = "options")]
pub enum FileType {
    /// Parquet source options
    Parquet(ParquetSourceOptions),

    /// Csv source options
    Csv(CsvSourceOptions),

    /// Json source options
    Json(JsonSourceOptions),
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, derive_new::new)]
#[cfg_attr(feature = "schema_gen", derive(schemars::JsonSchema))]
pub struct ParquetSourceOptions {
    /// schema to read this CSV with
    /// uses [arrow::datatypes::Schema](https://docs.rs/arrow/latest/arrow/datatypes/struct.Schema.html) for ser-de
    #[cfg_attr(feature = "schema_gen", schemars(skip))]
    pub schema: Option<Schema>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, derive_new::new)]
#[cfg_attr(feature = "schema_gen", derive(schemars::JsonSchema))]
pub struct CsvSourceOptions {
    /// set to `true` to treat first row of CSV as the header
    /// column names will be inferred from the header, if there is no header the column names are `column_1, column_2, ... column_x`
    pub has_header: Option<bool>,

    /// set a delimiter character to read this CSV with
    pub delimiter: Option<char>,

    /// schema to read this CSV with
    /// uses [arrow::datatypes::Schema](https://docs.rs/arrow/latest/arrow/datatypes/struct.Schema.html) for ser-de
    #[cfg_attr(feature = "schema_gen", schemars(skip))]
    pub schema: Option<Schema>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, derive_new::new)]
#[cfg_attr(feature = "schema_gen", derive(schemars::JsonSchema))]
pub struct JsonSourceOptions {
    /// schema to read this JSON with
    /// uses [arrow::datatypes::Schema](https://docs.rs/arrow/latest/arrow/datatypes/struct.Schema.html) for ser-de
    #[cfg_attr(feature = "schema_gen", schemars(skip))]
    pub schema: Option<Schema>,
}
