use thiserror::Error;

/// Error types for ODBC operations with security-conscious error messages.
///
/// IMPORTANT: This type never includes connection strings or other sensitive
/// information in error messages to prevent password leakage.
#[derive(Error, Debug)]
pub enum OdbcError {
    #[error(transparent)]
    ArrowOdbc(#[from] arrow_odbc::Error),

    #[error(transparent)]
    Writer(#[from] arrow_odbc::WriterError),

    #[error(transparent)]
    OdbcApi(#[from] arrow_odbc::odbc_api::Error),

    #[error(transparent)]
    Arrow(#[from] datafusion::arrow::error::ArrowError),

    #[error(transparent)]
    DataFusion(#[from] datafusion::error::DataFusionError),
}
