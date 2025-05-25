use thiserror::Error;

/// Error types for Delta Lake operations.
#[derive(Error, Debug)]
pub enum DeltaError {
    /// Delta table operation failed.
    #[error("Delta table operation failed: {0}")]
    DeltaTable(#[from] deltalake::DeltaTableError),

    /// DataFusion error occurred.
    #[error("DataFusion error: {0}")]
    DataFusion(#[from] datafusion::error::DataFusionError),
}
