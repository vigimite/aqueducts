use aqueducts_schemas::Destination;
use datafusion::{
    common::error::GenericError, datasource::MemTable, execution::context::SessionContext,
    prelude::DataFrame,
};
use miette::Diagnostic;
use std::sync::Arc;
use tracing::{debug, instrument};

use crate::store::{register_object_store, StoreError};

pub mod file;

#[derive(Debug, thiserror::Error, Diagnostic)]
pub enum DestinationError {
    #[error("Object store error '{name}'. {error}")]
    #[diagnostic(
        code(aqueducts::destination::store_error),
        help(
            "Check your storage configuration:\n\
              • Verify credentials and access permissions\n\
              • Ensure the storage bucket/container exists\n\
              • Check network connectivity to the storage service"
        )
    )]
    Store {
        name: String,
        #[source]
        error: StoreError,
    },

    #[error("Failed to register in-memory destination '{name}'. {error}")]
    #[diagnostic(
        code(aqueducts::destination::register_memory_error),
        help(
            "Check the in-memory destination:\n\
              • A table with this name may already exist\n\
              • Verify the destination name is unique"
        )
    )]
    RegisterMemory {
        name: String,
        #[source]
        error: datafusion::error::DataFusionError,
    },

    #[error("Failed to write to in-memory destination '{name}'. {error}")]
    #[diagnostic(
        code(aqueducts::destination::write_memory_error),
        help(
            "Writing to memory failed. Common issues:\n\
              • Insufficient memory for the dataset\n\
              • Data type incompatibilities"
        )
    )]
    WriteMemory {
        name: String,
        #[source]
        error: datafusion::error::DataFusionError,
    },

    #[error("Failed to register file destination '{name}'. {error}")]
    #[diagnostic(
        code(aqueducts::destination::register_file_error),
        help(
            "Check the file destination configuration:\n\
              • Verify the file path is valid\n\
              • Ensure you have write permissions\n\
              • Check that the parent directory exists"
        )
    )]
    RegisterFile {
        name: String,
        #[source]
        error: datafusion::error::DataFusionError,
    },

    #[error("Failed to write to file destination '{name}'. {error}")]
    #[diagnostic(
        code(aqueducts::destination::write_file_error),
        help(
            "File writing failed. Common issues:\n\
              • Insufficient disk space\n\
              • Invalid file format specified\n\
              • Permission denied on target directory\n\
              • Network issues for remote file systems"
        )
    )]
    WriteFile {
        name: String,
        #[source]
        error: datafusion::error::DataFusionError,
    },

    #[cfg(feature = "odbc")]
    #[error("Failed to register ODBC destination '{name}'. {error}")]
    #[diagnostic(
        code(aqueducts::destination::register_odbc_error),
        help(
            "Check your ODBC configuration:\n\
              • Verify the connection string is correct\n\
              • Ensure the ODBC driver is installed\n\
              • Check database server is accessible\n\
              • Verify authentication credentials"
        )
    )]
    RegisterOdbc {
        name: String,
        #[source]
        error: aqueducts_odbc::error::OdbcError,
    },

    #[cfg(feature = "odbc")]
    #[error("Failed to write to ODBC destination '{name}'. {error}")]
    #[diagnostic(
        code(aqueducts::destination::write_odbc_error),
        help(
            "ODBC write failed. Common issues:\n\
              • Table doesn't exist or lacks write permissions\n\
              • Data type incompatibilities with target schema\n\
              • Connection timeout or network issues"
        )
    )]
    WriteOdbc {
        name: String,
        #[source]
        error: aqueducts_odbc::error::OdbcError,
    },

    #[cfg(feature = "delta")]
    #[error("Failed to register Delta destination '{name}'. {error}")]
    #[diagnostic(
        code(aqueducts::destination::register_delta_error),
        help(
            "Check your Delta Lake configuration:\n\
              • Verify the storage location is accessible\n\
              • Ensure proper permissions for Delta operations\n\
              • Check that the schema is valid\n\
              • Verify partition columns exist in the schema"
        )
    )]
    RegisterDelta {
        name: String,
        #[source]
        error: aqueducts_delta::error::DeltaError,
    },

    #[cfg(feature = "delta")]
    #[error("Failed to write to Delta destination {name}")]
    #[diagnostic(
        code(aqueducts::destination::write_delta_error),
        help(
            "Delta write failed. Common issues:\n\
              • Schema mismatch with existing Delta table\n\
              • Invalid partition column values\n\
              • Storage permissions insufficient\n\
              • Concurrent write conflicts"
        )
    )]
    WriteDelta {
        name: String,
        #[source]
        error: aqueducts_delta::error::DeltaError,
    },

    #[error("Destination {name} of type {tpe} is unsupported")]
    #[diagnostic(
        code(aqueducts::destination::unsupported_type),
        help(
            "The destination type '{tpe}' requires enabling the corresponding feature flag.\n\
              \n\
              To enable this destination type, rebuild with:\n\
              • For ODBC: --features odbc\n\
              • For Delta Lake: --features delta"
        )
    )]
    Unsupported { name: String, tpe: String },
}

/// Creates a `Destination`
#[instrument(skip(ctx, destination))]
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
                .map(crate::schema_transform::field_to_arrow)
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
#[instrument(skip(ctx, destination, data))]
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
                .map(crate::schema_transform::field_to_arrow)
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
