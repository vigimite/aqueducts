#[allow(clippy::enum_variant_names)]
#[derive(Debug, thiserror::Error)]
pub enum Error {
    // -- External
    #[error("ArrowError({0})")]
    ArrowError(#[from] datafusion::arrow::error::ArrowError),
    #[error("DataFusionError({0})")]
    DataFusionError(#[from] datafusion::error::DataFusionError),
    #[error("OdbcError({0})")]
    OdbcError(#[from] arrow_odbc::Error),
    #[error("OdbcApiError({0})")]
    OdbcApiError(#[from] arrow_odbc::odbc_api::Error),
    #[error("OdbcWriterError({0})")]
    OdbcWriterError(#[from] arrow_odbc::WriterError),
}
