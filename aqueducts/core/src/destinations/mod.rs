use aqueducts_utils::store::register_object_store;
use datafusion::{dataframe::DataFrame, datasource::MemTable, execution::context::SessionContext};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::{debug, instrument};

pub mod delta;
pub mod file;
#[cfg(feature = "odbc")]
pub mod odbc;

pub(crate) mod error;
pub(crate) type Result<T> = core::result::Result<T, error::Error>;

/// Target output for the Aqueduct table
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "schema_gen", derive(schemars::JsonSchema))]
#[serde(tag = "type")]
pub enum Destination {
    /// An in-memory destination
    InMemory(InMemoryDestination),
    /// A delta table destination
    Delta(delta::DeltaDestination),
    /// A file output destination
    File(file::FileDestination),
    #[cfg(feature = "odbc")]
    /// An ODBC insert query to write to a DB table
    Odbc(odbc::OdbcDestination),
}

/// An in-memory table destination
#[derive(Debug, Clone, Serialize, Deserialize, derive_new::new)]
#[cfg_attr(feature = "schema_gen", derive(schemars::JsonSchema))]
pub struct InMemoryDestination {
    /// Name to register the table with in the provided `SessionContext`
    pub name: String,
}

/// Creates a `Destination`
#[instrument(skip(ctx, destination), err)]
pub async fn register_destination(
    ctx: Arc<SessionContext>,
    destination: &Destination,
) -> Result<()> {
    match destination {
        Destination::InMemory(_) => Ok(()),
        Destination::Delta(table_def) => {
            debug!(
                "Creating delta table  (if it doesn't exist yet) '{}' at location '{}'",
                table_def.name, table_def.location
            );

            let _ = delta::create(table_def).await?;
            Ok(())
        }
        Destination::File(file_def) => {
            register_object_store(ctx, &file_def.location, &file_def.storage_options)?;
            Ok(())
        }
        #[cfg(feature = "odbc")]
        Destination::Odbc(odbc_def) => {
            aqueducts_odbc::register_odbc_destination(
                odbc_def.connection_string.as_str(),
                odbc_def.name.as_str(),
            )
            .await?;
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
        Destination::Delta(table_def) => {
            debug!(
                "Writing data to delta table '{}' at location '{}'",
                table_def.name, table_def.location
            );
            let _ = delta::write(table_def, data).await?;

            Ok(())
        }
        Destination::File(file_def) => {
            debug!("Writing data to file at location '{}'", file_def.location);
            file::write(file_def, data).await?;

            Ok(())
        }
        #[cfg(feature = "odbc")]
        Destination::Odbc(odbc_def) => {
            odbc::write(odbc_def, data).await?;

            Ok(())
        }
    }
}
