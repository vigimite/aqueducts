use aqueducts_schemas::Destination;
use datafusion::{dataframe::DataFrame, datasource::MemTable, execution::context::SessionContext};
use std::sync::Arc;
use tracing::{debug, instrument};

use crate::error::{AqueductsError, Result};
use crate::store::register_object_store;

pub mod file;

/// Creates a `Destination`
#[instrument(skip(ctx, destination), err)]
pub async fn register_destination(
    ctx: Arc<SessionContext>,
    destination: &Destination,
) -> Result<()> {
    match destination {
        Destination::InMemory(_) => Ok(()),
        Destination::File(file_def) => {
            register_object_store(ctx, &file_def.location, &file_def.storage_config)?;
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
            .map_err(|e| {
                AqueductsError::destination(&odbc_dest.name, format!("ODBC destination error: {e}"))
            })?;
            Ok(())
        }
        #[cfg(feature = "delta")]
        Destination::Delta(delta_dest) => {
            debug!("Preparing Delta destination '{}'", delta_dest.name);

            let arrow_fields: Result<Vec<_>> = delta_dest
                .schema
                .iter()
                .map(|field| {
                    crate::schema_transform::field_to_arrow(field).map_err(|e| {
                        AqueductsError::schema_validation(format!("Schema conversion error: {e}"))
                    })
                })
                .collect();
            let arrow_fields = arrow_fields?;

            aqueducts_delta::prepare_delta_destination(
                &delta_dest.name,
                delta_dest.location.as_str(),
                &delta_dest.storage_config,
                &delta_dest.partition_columns,
                &delta_dest.table_properties,
                &arrow_fields,
            )
            .await
            .map_err(|e| {
                AqueductsError::destination(
                    &delta_dest.name,
                    format!("Delta destination error: {e}"),
                )
            })?;
            Ok(())
        }
        #[cfg(not(feature = "odbc"))]
        Destination::Odbc(dest) => Err(AqueductsError::unsupported(
            &dest.name,
            "ODBC support not enabled. Enable 'odbc' feature",
        )),
        #[cfg(not(feature = "delta"))]
        Destination::Delta(dest) => Err(AqueductsError::unsupported(
            &dest.name,
            "Delta support not enabled. Enable 'delta' feature",
        )),
    }
}

/// Write a `DataFrame` to an Aqueduct `Destination`
#[instrument(skip(ctx, destination, data), err)]
pub async fn write_to_destination(
    ctx: Arc<SessionContext>,
    destination: &Destination,
    data: DataFrame,
) -> Result<()> {
    match destination {
        Destination::InMemory(mem_def) => {
            debug!("Writing data to in-memory table '{}'", mem_def.name);

            let schema = data.schema().clone();
            let partitioned = data.collect_partitioned().await?;
            let table = MemTable::try_new(Arc::new(schema.as_arrow().clone()), partitioned)?;

            ctx.register_table(mem_def.name.as_str(), Arc::new(table))?;

            Ok(())
        }
        Destination::File(file_def) => {
            debug!("Writing data to file at location '{}'", file_def.location);
            file::write(file_def, data).await?;

            Ok(())
        }
        #[cfg(feature = "odbc")]
        Destination::Odbc(odbc_dest) => {
            debug!("Writing data to ODBC destination '{}'", odbc_dest.name);

            let schema = data.schema().as_arrow().clone();
            let batches = data.collect().await?;

            aqueducts_odbc::write_arrow_batches(
                &odbc_dest.connection_string,
                &odbc_dest.name, // Using name as table name
                odbc_dest.write_mode.clone(),
                batches,
                Arc::new(schema),
                odbc_dest.batch_size,
            )
            .await
            .map_err(|e| {
                AqueductsError::destination(&odbc_dest.name, format!("ODBC destination error: {e}"))
            })?;
            Ok(())
        }
        #[cfg(feature = "delta")]
        Destination::Delta(delta_dest) => {
            debug!("Writing data to Delta destination '{}'", delta_dest.name);

            let arrow_fields: Result<Vec<_>> = delta_dest
                .schema
                .iter()
                .map(|field| {
                    crate::schema_transform::field_to_arrow(field).map_err(|e| {
                        AqueductsError::schema_validation(format!("Schema conversion error: {e}"))
                    })
                })
                .collect();
            let arrow_schema = datafusion::arrow::datatypes::Schema::new(arrow_fields?);

            aqueducts_delta::write_to_delta_destination(
                &delta_dest.name,
                delta_dest.location.as_str(),
                &arrow_schema,
                &delta_dest.storage_config,
                &delta_dest.write_mode,
                data,
            )
            .await
            .map_err(|e| {
                AqueductsError::destination(
                    &delta_dest.name,
                    format!("Delta destination error: {e}"),
                )
            })?;
            Ok(())
        }
        #[cfg(not(feature = "odbc"))]
        Destination::Odbc(dest) => Err(AqueductsError::unsupported(
            &dest.name,
            "ODBC support not enabled. Enable 'odbc' feature",
        )),
        #[cfg(not(feature = "delta"))]
        Destination::Delta(dest) => Err(AqueductsError::unsupported(
            &dest.name,
            "Delta support not enabled. Enable 'delta' feature",
        )),
    }
}
