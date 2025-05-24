use crate::store::register_object_store;
use aqueducts_schemas::Destination;
use datafusion::{dataframe::DataFrame, datasource::MemTable, execution::context::SessionContext};
use std::sync::Arc;
use tracing::{debug, instrument};

pub mod file;

/// Creates a `Destination`
#[instrument(skip(ctx, destination), err)]
pub async fn register_destination(
    ctx: Arc<SessionContext>,
    destination: &Destination,
) -> crate::error::Result<()> {
    match destination {
        Destination::InMemory(_) => Ok(()),
        Destination::File(file_def) => {
            register_object_store(ctx, &file_def.location, &file_def.storage_config)?;
            Ok(())
        }
        #[cfg(not(feature = "odbc"))]
        Destination::Odbc(_) => Err(crate::error::AqueductsError::unsupported(
            "ODBC destination",
            "ODBC support not enabled. Enable 'odbc' feature",
        )),
        #[cfg(not(feature = "delta"))]
        Destination::Delta(_) => Err(crate::error::AqueductsError::unsupported(
            "Delta destination",
            "Delta support not enabled. Enable 'delta' feature",
        )),
        #[cfg(feature = "odbc")]
        Destination::Odbc(odbc_dest) => {
            debug!("Preparing ODBC destination '{}'", odbc_dest.name);
            aqueducts_odbc::register_odbc_destination(
                &odbc_dest.connection_string,
                &odbc_dest.name,
            )
            .await
            .map_err(|e| {
                crate::error::AqueductsError::destination(
                    &odbc_dest.name,
                    format!("ODBC destination error: {}", e),
                )
            })?;
            Ok(())
        }
        #[cfg(feature = "delta")]
        Destination::Delta(delta_dest) => {
            debug!("Preparing Delta destination '{}'", delta_dest.name);

            // Convert aqueducts Fields to Arrow Fields using schema transformation
            let arrow_fields: crate::error::Result<Vec<_>> = delta_dest
                .schema
                .iter()
                .map(|field| {
                    crate::schema_transform::field_to_arrow(field).map_err(|e| {
                        crate::error::AqueductsError::schema_validation(format!(
                            "Schema conversion error: {}",
                            e
                        ))
                    })
                })
                .collect();
            let arrow_fields = arrow_fields?;

            aqueducts_delta::destination::prepare_delta_destination(
                ctx,
                &delta_dest.name,
                delta_dest.location.as_str(),
                &delta_dest.storage_config,
                &delta_dest.partition_columns,
                &delta_dest.table_properties,
                &arrow_fields,
            )
            .await
            .map_err(|e| {
                crate::error::AqueductsError::destination(
                    &delta_dest.name,
                    format!("Delta destination error: {}", e),
                )
            })?;
            Ok(())
        }
    }
}

/// Write a `DataFrame` to an Aqueduct `Destination`
#[instrument(skip(ctx, destination, data), err)]
pub async fn write_to_destination(
    ctx: Arc<SessionContext>,
    destination: &Destination,
    data: DataFrame,
) -> crate::error::Result<()> {
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
        #[cfg(not(feature = "odbc"))]
        Destination::Odbc(_) => Err(crate::error::AqueductsError::unsupported(
            "ODBC destination",
            "ODBC support not enabled. Enable 'odbc' feature",
        )),
        #[cfg(not(feature = "delta"))]
        Destination::Delta(_) => Err(crate::error::AqueductsError::unsupported(
            "Delta destination",
            "Delta support not enabled. Enable 'delta' feature",
        )),
        #[cfg(feature = "odbc")]
        Destination::Odbc(odbc_dest) => {
            debug!("Writing data to ODBC destination '{}'", odbc_dest.name);

            // Get schema before collecting
            let schema = data.schema().as_arrow().clone();
            // Collect the DataFrame into Arrow batches
            let batches = data.collect().await?;

            // Write the batches to the ODBC destination
            aqueducts_odbc::write_arrow_batches(
                &odbc_dest.connection_string,
                &odbc_dest.name, // Using name as table name
                batches,
                Arc::new(schema),
                odbc_dest.batch_size,
            )
            .await
            .map_err(|e| {
                crate::error::AqueductsError::destination(
                    &odbc_dest.name,
                    format!("ODBC destination error: {}", e),
                )
            })?;
            Ok(())
        }
        #[cfg(feature = "delta")]
        Destination::Delta(delta_dest) => {
            debug!("Writing data to Delta destination '{}'", delta_dest.name);
            aqueducts_delta::destination::write_to_delta_destination(
                ctx,
                &delta_dest.name,
                delta_dest.location.as_str(),
                &delta_dest.storage_config,
                &delta_dest.write_mode,
                data,
            )
            .await
            .map_err(|e| {
                crate::error::AqueductsError::destination(
                    &delta_dest.name,
                    format!("Delta destination error: {}", e),
                )
            })?;
            Ok(())
        }
    }
}
