use std::sync::Arc;

use aqueducts_schemas::{
    sources::SourceFileType, CsvSourceOptions, DirSource, FileSource, JsonSourceOptions,
    ParquetSourceOptions, Source,
};
use datafusion::{
    datasource::{
        file_format::{csv::CsvFormat, json::JsonFormat, parquet::ParquetFormat},
        listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl},
    },
    prelude::*,
};
use miette::Diagnostic;
use tracing::debug;
use tracing::instrument;

use crate::store::register_object_store;
use crate::{
    schema_transform::{data_type_to_arrow, fields_to_arrow_schema},
    store::StoreError,
};

#[derive(Debug, thiserror::Error, Diagnostic)]
pub enum SourceError {
    #[error("Source '{0}' not found in context")]
    #[diagnostic(
        code(aqueducts::source::not_found),
        help("Make sure the source is included in the SessionContext that is being passed to aqueducts.\n\
              Check for typos in the source name.")
    )]
    NotFound(String),

    #[error("Object store error for source '{name}'. {error}")]
    #[diagnostic(
        code(aqueducts::source::store_error),
        help(
            "Check your storage configuration and credentials.\n\
              Common issues:\n\
              • Missing or incorrect credentials\n\
              • Network connectivity problems\n\
              • Incorrect bucket/container names"
        )
    )]
    Store {
        name: String,
        #[source]
        error: StoreError,
    },

    #[error("Failed to register file source '{name}'. {error}")]
    #[diagnostic(
        code(aqueducts::source::register_file),
        help(
            "Check that the file exists and is accessible.\n\
              Supported formats: Parquet (.parquet), CSV (.csv), JSON (.json)\n\
              \n\
              Common issues:\n\
              • File doesn't exist at the specified location\n\
              • Incorrect file permissions\n\
              • Unsupported file format"
        )
    )]
    RegisterFile {
        name: String,
        #[source]
        error: datafusion::error::DataFusionError,
    },

    #[cfg(feature = "odbc")]
    #[error("Failed to register ODBC source '{name}'. {error}")]
    #[diagnostic(
        code(aqueducts::source::register_odbc),
        help(
            "Check your ODBC connection:\n\
              • Verify the connection string format\n\
              • Ensure the ODBC driver is installed\n\
              • Check ODBC configuration: odbcinst -j\n\
              • Check database permissions for the query"
        )
    )]
    RegisterOdbc {
        name: String,
        #[source]
        error: aqueducts_odbc::error::OdbcError,
    },

    #[cfg(feature = "delta")]
    #[error("Failed to register Delta source '{name}'. {error}")]
    #[diagnostic(
        code(aqueducts::source::register_delta),
        help(
            "Delta Lake source issues:\n\
              • Verify the table path exists\n\
              • Check you have read permissions\n\
              • For S3/GCS/Azure, ensure credentials are configured\n\
              • Verify Delta table version compatibility"
        )
    )]
    RegisterDelta {
        name: String,
        #[source]
        error: aqueducts_delta::error::DeltaError,
    },

    #[error("Source '{name}' of type '{tpe}' is unsupported")]
    #[diagnostic(
        code(aqueducts::source::unsupported),
        help(
            "The '{tpe}' source type requires enabling the '{tpe}' feature.\n\
              \n\
              Install CLI using the corresponding feature flag:\n\
              cargo install aqueducts-cli --features {tpe}"
        )
    )]
    Unsupported { name: String, tpe: String },
}

/// Register an Aqueduct source
/// Supports Delta tables, Parquet files, Csv Files and Json Files
#[instrument(skip(ctx, source))]
pub async fn register_source(ctx: Arc<SessionContext>, source: Source) -> Result<(), SourceError> {
    match source {
        Source::InMemory(memory_source) => {
            debug!("Registering in-memory source '{}'", memory_source.name);

            if !ctx
                .table_exist(memory_source.name.as_str())
                .expect("failure while checking memory source")
            {
                return Err(SourceError::NotFound(memory_source.name));
            }
        }
        Source::File(file_source) => {
            debug!(
                "Registering file source '{}' at location '{}'",
                file_source.name, file_source.location,
            );

            register_object_store(
                ctx.clone(),
                &file_source.location,
                &file_source.storage_config,
            )
            .map_err(|e| SourceError::Store {
                name: file_source.name.clone(),
                error: e,
            })?;

            register_file_source(ctx, &file_source).await.map_err(|e| {
                SourceError::RegisterFile {
                    name: file_source.name.clone(),
                    error: e,
                }
            })?
        }
        Source::Directory(dir_source) => {
            debug!(
                "Registering directory source '{}' at location '{}' for type '{:?}'",
                dir_source.name, dir_source.location, dir_source.format
            );

            register_object_store(
                ctx.clone(),
                &dir_source.location,
                &dir_source.storage_config,
            )
            .map_err(|e| SourceError::Store {
                name: dir_source.name.clone(),
                error: e,
            })?;

            register_dir_source(ctx, &dir_source)
                .await
                .map_err(|e| SourceError::RegisterFile {
                    name: dir_source.name.clone(),
                    error: e,
                })?
        }
        #[cfg(feature = "odbc")]
        Source::Odbc(odbc_source) => {
            debug!("Registering ODBC source '{}'", odbc_source.name);
            aqueducts_odbc::register_odbc_source(
                ctx,
                &odbc_source.connection_string,
                &odbc_source.load_query,
                &odbc_source.name,
            )
            .await
            .map_err(|e| SourceError::RegisterOdbc {
                name: odbc_source.name.clone(),
                error: e,
            })?;
        }
        #[cfg(feature = "delta")]
        Source::Delta(delta_source) => {
            debug!("Registering Delta source '{}'", delta_source.name);
            aqueducts_delta::register_delta_source(ctx, &delta_source)
                .await
                .map_err(|e| SourceError::RegisterDelta {
                    name: delta_source.name.clone(),
                    error: e,
                })?;
        }
        #[cfg(not(feature = "odbc"))]
        Source::Odbc(source) => {
            return Err(SourceError::Unsupported {
                name: source.name.clone(),
                tpe: String::from("odbc"),
            });
        }
        #[cfg(not(feature = "delta"))]
        Source::Delta(source) => {
            return Err(SourceError::Unsupported {
                name: source.name.clone(),
                tpe: String::from("delta"),
            });
        }
    };

    Ok(())
}

async fn register_file_source(
    ctx: Arc<SessionContext>,
    file_source: &FileSource,
) -> Result<(), datafusion::error::DataFusionError> {
    match &file_source.format {
        SourceFileType::Parquet(ParquetSourceOptions { schema }) => {
            if !schema.is_empty() {
                let arrow_schema = fields_to_arrow_schema(schema);
                let options = ParquetReadOptions::default().schema(&arrow_schema);
                ctx.register_parquet(
                    file_source.name.as_str(),
                    file_source.location.as_str(),
                    options,
                )
                .await?;
            } else {
                let options = ParquetReadOptions::default();
                ctx.register_parquet(
                    file_source.name.as_str(),
                    file_source.location.as_str(),
                    options,
                )
                .await?
            }
        }

        SourceFileType::Csv(CsvSourceOptions {
            has_header,
            delimiter,
            schema,
        }) => {
            if !schema.is_empty() {
                let arrow_schema = fields_to_arrow_schema(schema);
                ctx.register_csv(
                    file_source.name.as_str(),
                    file_source.location.as_str(),
                    CsvReadOptions::default()
                        .has_header(*has_header)
                        .delimiter(*delimiter as u8)
                        .schema(&arrow_schema),
                )
                .await?
            } else {
                ctx.register_csv(
                    file_source.name.as_str(),
                    file_source.location.as_str(),
                    CsvReadOptions::default()
                        .has_header(*has_header)
                        .delimiter(*delimiter as u8),
                )
                .await?
            }
        }

        SourceFileType::Json(JsonSourceOptions { schema }) => {
            if !schema.is_empty() {
                let arrow_schema = fields_to_arrow_schema(schema);
                ctx.register_json(
                    file_source.name.as_str(),
                    file_source.location.as_str(),
                    NdJsonReadOptions::default().schema(&arrow_schema),
                )
                .await?;
            } else {
                ctx.register_json(
                    file_source.name.as_str(),
                    file_source.location.as_str(),
                    NdJsonReadOptions::default(),
                )
                .await?;
            }
        }
    };

    Ok(())
}

async fn register_dir_source(
    ctx: Arc<SessionContext>,
    dir_source: &DirSource,
) -> Result<(), datafusion::error::DataFusionError> {
    // register the object store for this source
    let session_state = ctx.state();

    let listing_table_url = ListingTableUrl::parse(dir_source.location.as_str())?;
    let listing_config = match &dir_source.format {
        SourceFileType::Parquet(ParquetSourceOptions { schema }) => {
            let partition_cols = dir_source
                .partition_columns
                .iter()
                .map(|(name, dt)| (name.clone(), data_type_to_arrow(dt)))
                .collect::<Vec<_>>();

            let listing_options = ListingOptions::new(Arc::new(ParquetFormat::default()))
                .with_table_partition_cols(partition_cols);

            let schema = if !schema.is_empty() {
                Arc::new(fields_to_arrow_schema(schema))
            } else {
                listing_options
                    .infer_schema(&session_state, &listing_table_url)
                    .await?
            };

            ListingTableConfig::new(listing_table_url)
                .with_listing_options(listing_options)
                .with_schema(schema)
        }
        SourceFileType::Csv(CsvSourceOptions {
            has_header,
            delimiter,
            schema,
        }) => {
            let format = CsvFormat::default()
                .with_has_header(*has_header)
                .with_delimiter(*delimiter as u8);

            let partition_cols = dir_source
                .partition_columns
                .iter()
                .map(|(name, dt)| (name.clone(), data_type_to_arrow(dt)))
                .collect::<Vec<_>>();
            let listing_options =
                ListingOptions::new(Arc::new(format)).with_table_partition_cols(partition_cols);

            let schema = if !schema.is_empty() {
                Arc::new(fields_to_arrow_schema(schema))
            } else {
                listing_options
                    .infer_schema(&session_state, &listing_table_url)
                    .await?
            };

            ListingTableConfig::new(listing_table_url)
                .with_listing_options(listing_options)
                .with_schema(schema)
        }

        SourceFileType::Json(JsonSourceOptions { schema }) => {
            let format = JsonFormat::default();

            let partition_cols = dir_source
                .partition_columns
                .iter()
                .map(|(name, dt)| (name.clone(), data_type_to_arrow(dt)))
                .collect::<Vec<_>>();
            let listing_options =
                ListingOptions::new(Arc::new(format)).with_table_partition_cols(partition_cols);

            let schema = if !schema.is_empty() {
                Arc::new(fields_to_arrow_schema(schema))
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
