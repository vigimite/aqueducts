#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("To use ODBC type you need to enable the odbc feature")]
    OdbcFeatureDisabled,

    // -- Modules
    #[cfg(feature = "odbc")]
    #[error("OdbcError({0})")]
    OdbcError(#[from] aqueducts_odbc::error::Error),

    // -- External
    #[error("ArrowError({0})")]
    ArrowError(#[from] datafusion::arrow::error::ArrowError),
    #[error("DataFusionError({0})")]
    DataFusionError(#[from] datafusion::error::DataFusionError),
    #[error("DeltaTableError({0})")]
    DeltaTableError(#[from] deltalake::errors::DeltaTableError),
}
