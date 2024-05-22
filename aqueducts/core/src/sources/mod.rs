use aqueducts_utils::serde::deserialize_file_location;
use aqueducts_utils::store::register_object_store;
use chrono::{DateTime, Utc};
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::datatypes::Schema;
use datafusion::{
    datasource::{
        file_format::{csv::CsvFormat, parquet::ParquetFormat},
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

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Source {
    Delta(DeltaSource),
    File(FileSource),
    Directory(DirSource),
}

impl Source {
    pub fn name(&self) -> &str {
        match self {
            Source::Delta(delta) => delta.name.as_str(),
            Source::File(file) => file.name.as_str(),
            Source::Directory(dir) => dir.name.as_str(),
        }
    }

    pub fn location(&self) -> &Url {
        match self {
            Source::Delta(delta) => &delta.location,
            Source::File(file) => &file.location,
            Source::Directory(dir) => &dir.location,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, derive_new::new)]
pub struct DeltaSource {
    pub name: String,
    #[serde(deserialize_with = "deserialize_file_location")]
    pub location: Url,
    pub version_ts: Option<DateTime<Utc>>,
    #[serde(default)]
    pub storage_options: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, derive_new::new)]
pub struct FileSource {
    pub name: String,
    pub file_type: FileType,
    #[serde(deserialize_with = "deserialize_file_location")]
    pub location: Url,
    #[serde(default)]
    pub storage_options: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, derive_new::new)]
pub struct DirSource {
    pub name: String,
    pub file_type: FileType,
    #[serde(default)]
    pub partition_cols: Vec<(String, DataType)>,
    #[serde(deserialize_with = "deserialize_file_location")]
    pub location: Url,
    #[serde(default)]
    pub storage_options: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "options")]
pub enum FileType {
    Parquet(ParquetSourceOptions),
    Csv(CsvSourceOptions),
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, derive_new::new)]
pub struct ParquetSourceOptions {
    schema: Option<Schema>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, derive_new::new)]
pub struct CsvSourceOptions {
    has_header: Option<bool>,
    delimiter: Option<char>,
    schema: Option<Schema>,
}

/// Register an Aqueduct source
/// Supports Delta tables, Parquet files and Csv Files
#[instrument(skip(ctx, source), err)]
pub async fn register_source(ctx: &SessionContext, source: Source) -> Result<()> {
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
    };

    Ok(())
}

async fn register_delta_source(ctx: &SessionContext, delta_source: DeltaSource) -> Result<()> {
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

async fn register_file_source(ctx: &SessionContext, file_source: FileSource) -> Result<()> {
    // register the object store for this source
    register_object_store(ctx, &file_source.location, &file_source.storage_options)?;

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
    };

    Ok(())
}

async fn register_dir_source(ctx: &SessionContext, dir_source: DirSource) -> Result<()> {
    // register the object store for this source
    register_object_store(ctx, &dir_source.location, &dir_source.storage_options)?;

    let session_state = ctx.state();

    let listing_table_url = ListingTableUrl::parse(dir_source.location)?;
    let listing_config = match dir_source.file_type {
        FileType::Parquet(ParquetSourceOptions { schema }) => {
            let listing_options = ListingOptions::new(Arc::new(ParquetFormat::default()))
                .with_table_partition_cols(dir_source.partition_cols);

            let resolved_schema = listing_options
                .infer_schema(&session_state, &listing_table_url)
                .await?;

            let schema = schema
                .map(Arc::new)
                .unwrap_or(resolved_schema);

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

            let resolved_schema = listing_options
                .infer_schema(&session_state, &listing_table_url)
                .await?;

            let schema = schema
                .map(Arc::new)
                .unwrap_or(resolved_schema);

            ListingTableConfig::new(listing_table_url)
                .with_listing_options(listing_options)
                .with_schema(schema)
        }
    };

    let provider = Arc::new(ListingTable::try_new(listing_config)?);
    let _ = ctx.register_table(dir_source.name.as_str(), provider)?;

    Ok(())
}
