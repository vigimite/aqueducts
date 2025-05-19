use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Delta Lake error: {0}")]
    DeltaLake(#[from] deltalake::errors::DeltaTableError),

    #[error("DataFusion error: {0}")]
    DataFusion(#[from] datafusion::error::DataFusionError),

    #[error("Arrow error: {0}")]
    Arrow(#[from] datafusion::arrow::error::ArrowError),

    #[error("Storage error: {0}")]
    Storage(#[from] aqueducts_storage::Error),

    #[error("I/O error: {0}")]
    IO(#[from] std::io::Error),

    #[error("{0}")]
    Other(String),
}
