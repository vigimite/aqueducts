//! Source registration and behavior implementations

use aqueducts_storage::register_object_store;
use datafusion::{
    datasource::{
        file_format::{csv::CsvFormat, json::JsonFormat, parquet::ParquetFormat},
        listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl},
    },
    execution::context::SessionContext,
    prelude::*,
};
use std::sync::Arc;
use tracing::debug;
use tracing::instrument;

use crate::model::sources::{
    error, CsvSourceOptions, DeltaSource, DirSource, FileSource, FileType, JsonSourceOptions,
    ParquetSourceOptions, Source,
};

pub(crate) type Result<T> = core::result::Result<T, error::Error>;

/// Register an Aqueduct source
/// Supports Delta tables, Parquet files, Csv Files and Json Files
#[instrument(skip(ctx, source), err)]
pub async fn register_source(ctx: Arc<SessionContext>, source: Source) -> Result<()> {
    match source {
        Source::InMemory(memory_source) => {
            debug!("Registering in-memory source '{}'", memory_source.name);

            if !ctx.table_exist(memory_source.name.as_str())? {
                return Err(error::Error::MissingInMemory(memory_source.name));
            }
        }
        Source::Delta(delta_source) => {
            debug!(
                "Registering delta source '{}' at location '{}'",
                delta_source.name, delta_source.location,
            );

            register_delta_source(ctx, delta_source).await?
        }
        Source::File(file_source) => {
            debug!(
                "Registering file source '{}' at location '{}'",
                file_source.name, file_source.location,
            );

            register_file_source(ctx, file_source).await?
        }
        Source::Directory(dir_source) => {
            debug!(
                "Registering directory source '{}' at location '{}' for type '{:?}'",
                dir_source.name, dir_source.location, dir_source.file_type
            );

            register_dir_source(ctx, dir_source).await?
        }
        #[cfg(feature = "odbc")]
        Source::Odbc(odbc_source) => {
            debug!(
                "Registering ODBC source '{}' using query '{}'",
                odbc_source.name, odbc_source.query
            );

            aqueducts_odbc::register_odbc_source(
                ctx,
                odbc_source.connection_string.as_str(),
                odbc_source.query.as_str(),
                odbc_source.name.as_str(),
            )
            .await?
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
