//! Models for data destinations

use aqueducts_storage::serde::deserialize_file_location;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use url::Url;

pub(crate) mod error;
pub(crate) type Result<T> = core::result::Result<T, error::Error>;

use async_trait::async_trait;
use datafusion::dataframe::DataFrame;
use datafusion::execution::context::SessionContext;
use std::sync::Arc;

use crate::execution::destinations::{register_destination, write_to_destination};
use crate::execution::traits::DestinationProvider;
// Removed redundant imports

/// Target output for the Aqueduct table
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "schema_gen", derive(schemars::JsonSchema))]
#[serde(tag = "type")]
pub enum Destination {
    /// An in-memory destination
    InMemory(InMemoryDestination),
    /// A delta table destination
    #[cfg_attr(feature = "schema_gen", schemars(skip))]
    Delta(DeltaDestination),
    /// A file output destination
    File(FileDestination),
    #[cfg(feature = "odbc")]
    /// An ODBC insert query to write to a DB table
    Odbc(OdbcDestination),
}

#[async_trait]
impl DestinationProvider for Destination {
    async fn register(
        &self,
        ctx: Arc<SessionContext>,
    ) -> std::result::Result<(), crate::error::Error> {
        register_destination(ctx, self)
            .await
            .map_err(crate::error::Error::ModelDestinationError)
    }

    async fn write(
        &self,
        ctx: Arc<SessionContext>,
        data: DataFrame,
    ) -> std::result::Result<(), crate::error::Error> {
        write_to_destination(ctx, self, data)
            .await
            .map_err(crate::error::Error::ModelDestinationError)
    }
}

/// An in-memory table destination
#[derive(Debug, Clone, Serialize, Deserialize, derive_new::new)]
#[cfg_attr(feature = "schema_gen", derive(schemars::JsonSchema))]
pub struct InMemoryDestination {
    /// Name to register the table with in the provided `SessionContext`
    pub name: String,
}

/// A delta table destination
#[derive(Debug, Clone, Serialize, Deserialize, derive_new::new)]
#[cfg_attr(feature = "schema_gen", derive(schemars::JsonSchema))]
pub struct DeltaDestination {
    /// Name of the table
    pub name: String,

    /// Location of the table as a URL e.g. file:///tmp/delta_table/, s3://bucket_name/delta_table
    #[serde(deserialize_with = "deserialize_file_location")]
    pub location: Url,

    /// DeltaTable storage options
    #[serde(default)]
    pub storage_options: HashMap<String, String>,

    /// DeltaTable table properties: <https://docs.delta.io/latest/table-properties.html>
    pub table_properties: HashMap<String, Option<String>>,

    /// Columns that will be used to determine uniqueness during merge operation
    /// Supported types: All primitive types and lists of primitive types
    pub write_mode: WriteMode,

    /// Columns to partition table by
    pub partition_cols: Vec<String>,

    /// Table schema definition `deltalake_core::models::schema::StructField`
    #[cfg_attr(feature = "schema_gen", schemars(skip))]
    pub schema: Vec<deltalake::kernel::StructField>,
}

/// Write modes for the `Destination` output.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "schema_gen", derive(schemars::JsonSchema))]
#[serde(tag = "operation", content = "params")]
pub enum WriteMode {
    /// `Append`: appends data to the `Destination`
    Append,

    /// `Upsert`: upserts data to the `Destination` using the specified merge columns
    Upsert(Vec<String>),

    /// `Replace`: replaces data to the `Destination` using the specified `ReplaceCondition`s
    Replace(Vec<ReplaceCondition>),
}

/// Condition used to build a predicate by which data should be replaced in a `Destination`
/// Expression built is checking equality for the given `value` of a field with `field_name`
#[derive(Debug, Clone, Serialize, Deserialize, derive_new::new)]
#[cfg_attr(feature = "schema_gen", derive(schemars::JsonSchema))]
pub struct ReplaceCondition {
    pub column: String,
    pub value: String,
}

/// A file output destination
#[derive(Debug, Clone, Serialize, Deserialize, derive_new::new)]
#[cfg_attr(feature = "schema_gen", derive(schemars::JsonSchema))]
pub struct FileDestination {
    ///  Name of the file to write
    pub name: String,

    /// Location of the file as a URL e.g. file:///tmp/output.csv, s3://bucket_name/prefix/output.parquet, s3:://bucket_name/prefix
    #[serde(deserialize_with = "deserialize_file_location")]
    pub location: Url,

    /// File type, supported types are Parquet and CSV
    pub file_type: FileType,

    /// Describes whether to write a single file (can be used to overwrite destination file)
    #[serde(default)]
    pub single_file: bool,

    /// Columns to partition table by
    #[serde(default)]
    pub partition_cols: Vec<String>,

    /// Object store storage options
    #[serde(default)]
    pub storage_options: HashMap<String, String>,
}

/// File type and options
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "schema_gen", derive(schemars::JsonSchema))]
#[serde(tag = "type", content = "options")]
pub enum FileType {
    /// Parquet options map, please refer to <https://docs.rs/datafusion-common/latest/datafusion_common/config/struct.TableParquetOptions.html> for possible options
    Parquet(#[serde(default)] HashMap<String, String>),

    /// CSV options
    Csv(CsvDestinationOptions),

    /// Json destination, no supported options
    Json,
}

/// Csv options
#[derive(Debug, Clone, Serialize, Deserialize, Default, derive_new::new)]
#[cfg_attr(feature = "schema_gen", derive(schemars::JsonSchema))]
pub struct CsvDestinationOptions {
    /// Defaults to true, sets a header for the CSV file
    pub has_header: Option<bool>,

    /// Defaults to `,`, sets the delimiter char for the CSV file
    pub delimiter: Option<char>,
}

/// An ODBC destination
#[cfg(feature = "odbc")]
#[derive(Debug, Clone, Serialize, Deserialize, derive_new::new)]
#[cfg_attr(feature = "schema_gen", derive(schemars::JsonSchema))]
pub struct OdbcDestination {
    /// Name of the table, this will be the table name in the SQL insert, e.g. `INSERT INTO table_name VALUES (...)`
    pub name: String,

    /// ODBC connection string
    /// Please reference the respective database connection string syntax (e.g. <https://www.connectionstrings.com/postgresql-odbc-driver-psqlodbc/>)
    pub connection_string: String,
}
