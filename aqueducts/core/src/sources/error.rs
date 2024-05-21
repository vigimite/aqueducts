#[derive(Debug, thiserror::Error)]
pub enum Error {
    // -- External
    #[error("ArrowError({0})")]
    ArrowError(#[from] datafusion::arrow::error::ArrowError),
    #[error("DataFusionError({0})")]
    DataFusionError(#[from] datafusion::error::DataFusionError),
    #[error("DeltaTableError({0})")]
    DeltaTableError(#[from] deltalake::errors::DeltaTableError),
}
