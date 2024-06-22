use aqueducts_utils::store::register_object_store;
use datafusion::{dataframe::DataFrame, execution::context::SessionContext};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::{info, instrument};

pub mod delta;
pub mod file;

pub(crate) mod error;
pub(crate) type Result<T> = core::result::Result<T, error::Error>;

/// Target output for the Aqueduct table
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(tag = "type")]
pub enum Destination {
    /// A delta table destination
    Delta(delta::DeltaDestination),
    /// A file output destination
    File(file::FileDestination),
}

/// Creates a `Destination`
#[instrument(skip(ctx, destination), err)]
pub async fn create_destination(ctx: Arc<SessionContext>, destination: &Destination) -> Result<()> {
    match destination {
        Destination::Delta(table_def) => {
            info!(
                "Creating delta table  (if it doesn't exist yet) '{}' at location '{}'",
                table_def.name, table_def.location
            );

            let _ = delta::create(table_def).await?;
            Ok(())
        }
        Destination::File(file_dest) => {
            register_object_store(ctx, &file_dest.location, &file_dest.storage_options)?;
            Ok(())
        }
    }
}

/// Write a `DataFrame` to an Aqueduct `Destination`
#[instrument(skip(destination, data), err)]
pub async fn write_to_destination(destination: &Destination, data: DataFrame) -> Result<()> {
    match destination {
        Destination::Delta(table_def) => {
            info!(
                "Writing data to delta table '{}' at location '{}'",
                table_def.name, table_def.location
            );
            let _ = delta::write(table_def, data).await?;

            Ok(())
        }
        Destination::File(file_def) => {
            info!("Writing data to file at location '{}'", file_def.location);
            file::write(file_def, data).await?;

            Ok(())
        }
    }
}
