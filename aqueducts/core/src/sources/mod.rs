use aqueducts_utils::serde::deserialize_file_location;
use aqueducts_utils::store::register_object_store;
use chrono::{DateTime, Utc};
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::datatypes::Schema;
use datafusion::{
    datasource::{
        file_format::{csv::CsvFormat, json::JsonFormat, parquet::ParquetFormat},
        listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl},
    },
    prelude::*,
};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, sync::Arc};
use tracing::{info, instrument};
use url::Url;

pub(crate) mod error;
pub(crate) type Result<T> = core::result::Result<T, error::Error>;

/// A data source that can be either a delta table (`delta`), a `file`, a `directory` or an `odbc` connection
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "schema_gen", derive(schemars::JsonSchema))]
#[serde(tag = "type")]
pub enum Source {
    /// A delta table source
    Delta(DeltaSource),
    /// A file source
    File(FileSource),
    /// A directory source
    Directory(DirSource),
    /// An ODBC source
    Odbc(OdbcSource),
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
    schema: Option<Schema>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, derive_new::new)]
#[cfg_attr(feature = "schema_gen", derive(schemars::JsonSchema))]
pub struct CsvSourceOptions {
    /// set to `true` to treat first row of CSV as the header
    /// column names will be inferred from the header, if there is no header the column names are `column_1, column_2, ... column_x`
    has_header: Option<bool>,

    /// set a delimiter character to read this CSV with
    delimiter: Option<char>,

    /// schema to read this CSV with
    /// uses [arrow::datatypes::Schema](https://docs.rs/arrow/latest/arrow/datatypes/struct.Schema.html) for ser-de
    #[cfg_attr(feature = "schema_gen", schemars(skip))]
    schema: Option<Schema>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, derive_new::new)]
#[cfg_attr(feature = "schema_gen", derive(schemars::JsonSchema))]
pub struct JsonSourceOptions {
    /// schema to read this JSON with
    /// uses [arrow::datatypes::Schema](https://docs.rs/arrow/latest/arrow/datatypes/struct.Schema.html) for ser-de
    #[cfg_attr(feature = "schema_gen", schemars(skip))]
    schema: Option<Schema>,
}

/// Register an Aqueduct source
/// Supports Delta tables, Parquet files, Csv Files and Json Files
#[instrument(skip(ctx, source), err)]
pub async fn register_source(ctx: Arc<SessionContext>, source: Source) -> Result<()> {
    match source {
        Source::Delta(delta_source) => {
            info!(
                "Registering delta source '{}' at location '{}'",
                delta_source.name, delta_source.location,
            );

            register_delta_source(ctx, delta_source).await?
        }
        Source::File(file_source) => {
            info!(
                "Registering file source '{}' at location '{}'",
                file_source.name, file_source.location,
            );

            register_file_source(ctx, file_source).await?
        }
        Source::Directory(dir_source) => {
            info!(
                "Registering directory source '{}' at location '{}' for type '{:?}'",
                dir_source.name, dir_source.location, dir_source.file_type
            );

            register_dir_source(ctx, dir_source).await?
        }
        #[cfg(feature = "odbc")]
        Source::Odbc(odbc_source) if cfg!(feature = "odbc") => {
            info!(
                "Registering ODBC source '{}' using query '{}'",
                odbc_source.name, odbc_source.query
            );

            aqueducts_utils::odbc::register_odbc_source(
                ctx,
                odbc_source.connection_string.as_str(),
                odbc_source.query.as_str(),
                odbc_source.name.as_str(),
            )
            .await?
        }
        Source::Odbc(_) => {
            return Err(error::Error::OdbcFeatureDisabled);
        }
    };

    Ok(())
}

async fn register_delta_source(ctx: Arc<SessionContext>, delta_source: DeltaSource) -> Result<()> {
    let builder = deltalake::DeltaTableBuilder::from_valid_uri(delta_source.location)?
        .with_storage_options(delta_source.storage_options);

    let table = if let Some(timestamp) = delta_source.version_ts {
        builder
            .with_datestring(timestamp.to_rfc3339())?
            .load()
            .await?
    } else {
        builder.load().await?
    };

    let _ = ctx.register_table(delta_source.name.as_str(), Arc::new(table))?;

    Ok(())
}

async fn register_file_source(ctx: Arc<SessionContext>, file_source: FileSource) -> Result<()> {
    // register the object store for this source
    register_object_store(
        ctx.clone(),
        &file_source.location,
        &file_source.storage_options,
    )?;

    match file_source.file_type {
        FileType::Parquet(ParquetSourceOptions {
            schema: Some(schema),
        }) => {
            let options = ParquetReadOptions::default().schema(&schema);

            ctx.register_parquet(
                file_source.name.as_str(),
                file_source.location.as_str(),
                options,
            )
            .await?
        }
        FileType::Parquet(ParquetSourceOptions { schema: None }) => {
            let options = ParquetReadOptions::default();

            ctx.register_parquet(
                file_source.name.as_str(),
                file_source.location.as_str(),
                options,
            )
            .await?
        }

        FileType::Csv(CsvSourceOptions {
            has_header,
            delimiter,
            schema: Some(schema),
        }) => {
            ctx.register_csv(
                file_source.name.as_str(),
                file_source.location.as_str(),
                CsvReadOptions::default()
                    .has_header(has_header.unwrap_or(true))
                    .delimiter_option(delimiter.map(|d| d as u8))
                    .schema(&schema),
            )
            .await?
        }
        FileType::Csv(CsvSourceOptions {
            has_header,
            delimiter,
            schema: None,
        }) => {
            ctx.register_csv(
                file_source.name.as_str(),
                file_source.location.as_str(),
                CsvReadOptions::default()
                    .has_header(has_header.unwrap_or(true))
                    .delimiter_option(delimiter.map(|d| d as u8)),
            )
            .await?
        }
        FileType::Json(JsonSourceOptions {
            schema: Some(schema),
        }) => {
            ctx.register_json(
                file_source.name.as_str(),
                file_source.location.as_str(),
                NdJsonReadOptions::default().schema(&schema),
            )
            .await?;
        }
        FileType::Json(JsonSourceOptions { schema: None }) => {
            ctx.register_json(
                file_source.name.as_str(),
                file_source.location.as_str(),
                NdJsonReadOptions::default(),
            )
            .await?;
        }
    };

    Ok(())
}

async fn register_dir_source(ctx: Arc<SessionContext>, dir_source: DirSource) -> Result<()> {
    // register the object store for this source
    register_object_store(
        ctx.clone(),
        &dir_source.location,
        &dir_source.storage_options,
    )?;

    let session_state = ctx.state();

    let listing_table_url = ListingTableUrl::parse(dir_source.location)?;
    let listing_config = match dir_source.file_type {
        FileType::Parquet(ParquetSourceOptions { schema }) => {
            let listing_options = ListingOptions::new(Arc::new(ParquetFormat::default()))
                .with_table_partition_cols(dir_source.partition_cols);

            let schema = if let Some(schema) = schema {
                Arc::new(schema)
            } else {
                listing_options
                    .infer_schema(&session_state, &listing_table_url)
                    .await?
            };

            ListingTableConfig::new(listing_table_url)
                .with_listing_options(listing_options)
                .with_schema(schema)
        }
        FileType::Csv(CsvSourceOptions {
            has_header,
            delimiter,
            schema,
        }) => {
            let format = CsvFormat::default()
                .with_has_header(has_header.unwrap_or(true))
                .with_delimiter(delimiter.unwrap_or(',') as u8);

            let listing_options = ListingOptions::new(Arc::new(format))
                .with_table_partition_cols(dir_source.partition_cols);

            let schema = if let Some(schema) = schema {
                Arc::new(schema)
            } else {
                listing_options
                    .infer_schema(&session_state, &listing_table_url)
                    .await?
            };

            ListingTableConfig::new(listing_table_url)
                .with_listing_options(listing_options)
                .with_schema(schema)
        }

        FileType::Json(JsonSourceOptions { schema }) => {
            let format = JsonFormat::default();

            let listing_options = ListingOptions::new(Arc::new(format))
                .with_table_partition_cols(dir_source.partition_cols);

            let schema = if let Some(schema) = schema {
                Arc::new(schema)
            } else {
                listing_options
                    .infer_schema(&session_state, &listing_table_url)
                    .await?
            };

            ListingTableConfig::new(listing_table_url)
                .with_listing_options(listing_options)
                .with_schema(schema)
        }
    };

    let provider = Arc::new(ListingTable::try_new(listing_config)?);
    let _ = ctx.register_table(dir_source.name.as_str(), provider)?;

    Ok(())
}
