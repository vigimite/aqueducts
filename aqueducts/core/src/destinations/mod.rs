use aqueducts_schemas::Destination;
use datafusion::{
    common::error::GenericError, datasource::MemTable, execution::context::SessionContext,
    prelude::DataFrame,
};
use std::sync::Arc;
use tracing::{debug, instrument};

use crate::store::{register_object_store, StoreError};

pub mod file;

#[derive(Debug, thiserror::Error)]
pub enum DestinationError {
    #[error("Object store error {name}: {error}")]
    Store { name: String, error: StoreError },

    #[error("Failed to register in-memory destination {name}: {error}")]
    RegisterMemory {
        name: String,
        error: datafusion::error::DataFusionError,
    },

    #[error("Failed to write to in-memory destination {name}: {error}")]
    WriteMemory {
        name: String,
        error: datafusion::error::DataFusionError,
    },

    #[error("Failed to register file destination {name}: {error}")]
    RegisterFile {
        name: String,
        error: datafusion::error::DataFusionError,
    },

    #[error("Failed to write to file destination {name}: {error}")]
    WriteFile {
        name: String,
        error: datafusion::error::DataFusionError,
    },

    #[cfg(feature = "odbc")]
    #[error("Failed to register ODBC destination {name}: {error}")]
    RegisterOdbc {
        name: String,
        error: aqueducts_odbc::error::OdbcError,
    },

    #[cfg(feature = "odbc")]
    #[error("Failed to write to ODBC destination {name}: {error}")]
    WriteOdbc {
        name: String,
        error: aqueducts_odbc::error::OdbcError,
    },

    #[cfg(feature = "delta")]
    #[error("Failed to register Delta destination {name}: {error}")]
    RegisterDelta {
        name: String,
        error: aqueducts_delta::error::DeltaError,
    },

    #[cfg(feature = "delta")]
    #[error("Failed to write to Delta destination {name}: {error}")]
    WriteDelta {
        name: String,
        error: aqueducts_delta::error::DeltaError,
    },

    #[error("Destination {name} of type {tpe} is unsupported. Make sure to enable the corresponding feature flag")]
    Unsupported { name: String, tpe: String },
}

/// Creates a `Destination`
#[instrument(skip(ctx, destination), err)]
pub async fn register_destination(
    ctx: Arc<SessionContext>,
    destination: &Destination,
) -> Result<(), DestinationError> {
    match destination {
        Destination::InMemory(dest) => match ctx.table_exist(&dest.name) {
            Ok(false) => Ok(()),
            Ok(true) => Err(DestinationError::RegisterMemory {
                name: dest.name.clone(),
                error: datafusion::error::DataFusionError::External(GenericError::from(
                    String::from("Table already exists"),
                )),
            }),
            Err(error) => Err(DestinationError::RegisterMemory {
                name: dest.name.clone(),
                error,
            }),
        },
        Destination::File(file_def) => {
            register_object_store(ctx, &file_def.location, &file_def.storage_config).map_err(
                |error| DestinationError::Store {
                    name: file_def.name.clone(),
                    error,
                },
            )?;
            Ok(())
        }
        #[cfg(feature = "odbc")]
        Destination::Odbc(odbc_dest) => {
            debug!("Preparing ODBC destination '{}'", odbc_dest.name);
            aqueducts_odbc::register_odbc_destination(
                &odbc_dest.connection_string,
                &odbc_dest.name,
            )
            .await
            .map_err(|error| DestinationError::RegisterOdbc {
                name: odbc_dest.name.clone(),
                error,
            })?;
            Ok(())
        }
        #[cfg(feature = "delta")]
        Destination::Delta(delta_dest) => {
            debug!("Preparing Delta destination '{}'", delta_dest.name);

            let arrow_fields = delta_dest
                .schema
                .iter()
                .map(|field| crate::schema_transform::field_to_arrow(field))
                .collect::<Vec<_>>();

            aqueducts_delta::prepare_delta_destination(
                &delta_dest.name,
                delta_dest.location.as_str(),
                &delta_dest.storage_config,
                &delta_dest.partition_columns,
                &delta_dest.table_properties,
                &arrow_fields,
            )
            .await
            .map_err(|error| DestinationError::RegisterDelta {
                name: delta_dest.name.clone(),
                error,
            })?;
            Ok(())
        }
        #[cfg(not(feature = "odbc"))]
        Destination::Odbc(dest) => Err(DestinationError::Unsupported {
            name: dest.name.clone(),
            tpe: String::from("odbc"),
        }),
        #[cfg(not(feature = "delta"))]
        Destination::Delta(dest) => Err(DestinationError::Unsupported {
            name: dest.name.clone(),
            tpe: String::from("delta"),
        }),
    }
}

/// Write a `DataFrame` to an Aqueduct `Destination`
#[instrument(skip(ctx, destination, data), err)]
pub async fn write_to_destination(
    ctx: Arc<SessionContext>,
    destination: &Destination,
    data: DataFrame,
) -> Result<(), DestinationError> {
    match destination {
        Destination::InMemory(mem_def) => {
            debug!("Writing data to in-memory table '{}'", mem_def.name);
            let schema = data.schema().as_arrow().clone();
            let data = data
                .collect_partitioned()
                .await
                .expect("failed to fetch data from memory table");

            let table = MemTable::try_new(Arc::new(schema), data).map_err(|error| {
                DestinationError::WriteMemory {
                    name: mem_def.name.clone(),
                    error,
                }
            })?;

            ctx.register_table(mem_def.name.as_str(), Arc::new(table))
                .map_err(|error| DestinationError::WriteMemory {
                    name: mem_def.name.clone(),
                    error,
                })?;

            Ok(())
        }
        Destination::File(file_def) => {
            debug!("Writing data to file at location '{}'", file_def.location);
            file::write(file_def, data).await
        }
        #[cfg(feature = "odbc")]
        Destination::Odbc(odbc_dest) => {
            debug!("Writing data to ODBC destination '{}'", odbc_dest.name);

            let schema = data.schema().as_arrow().clone();
            let batches = data
                .collect()
                .await
                .expect("failed to fetch data from memory table");

            aqueducts_odbc::write_arrow_batches(
                &odbc_dest.connection_string,
                &odbc_dest.name, // Using name as table name
                odbc_dest.write_mode.clone(),
                batches,
                Arc::new(schema),
                odbc_dest.batch_size,
            )
            .await
            .map_err(|error| DestinationError::WriteOdbc {
                name: odbc_dest.name.clone(),
                error,
            })
        }
        #[cfg(feature = "delta")]
        Destination::Delta(delta_dest) => {
            debug!("Writing data to Delta destination '{}'", delta_dest.name);

            let arrow_fields = delta_dest
                .schema
                .iter()
                .map(|field| crate::schema_transform::field_to_arrow(field))
                .collect::<Vec<_>>();
            let arrow_schema = datafusion::arrow::datatypes::Schema::new(arrow_fields);

            aqueducts_delta::write_to_delta_destination(
                &delta_dest.name,
                delta_dest.location.as_str(),
                &arrow_schema,
                &delta_dest.storage_config,
                &delta_dest.write_mode,
                data,
            )
            .await
            .map_err(|error| DestinationError::WriteDelta {
                name: delta_dest.name.clone(),
                error,
            })
        }
        #[cfg(not(feature = "odbc"))]
        Destination::Odbc(dest) => Err(DestinationError::Unsupported {
            name: dest.name.clone(),
            tpe: String::from("odbc"),
        }),
        #[cfg(not(feature = "delta"))]
        Destination::Delta(dest) => Err(DestinationError::Unsupported {
            name: dest.name.clone(),
            tpe: String::from("delta"),
        }),
    }
}
