#[allow(clippy::enum_variant_names)]
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Unknown parquet config key: {0}")]
    UnknownConfigOption(String),

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
