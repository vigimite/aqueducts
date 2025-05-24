use thiserror::Error;

/// Error types for Delta Lake operations.
#[derive(Error, Debug)]
pub enum DeltaError {
    /// Delta table not found.
    #[error("Delta table not found: {table}")]
    TableNotFound { table: String },

    /// Delta merge conflict.
    #[error("Delta merge conflict: {message}")]
    MergeConflict { message: String },

    /// Delta transaction failed.
    #[error("Delta transaction failed: {message}")]
    TransactionFailed { message: String },

    /// Delta checkpoint operation failed.
    #[error("Delta checkpoint operation failed: {message}")]
    CheckpointFailed { message: String },

    /// Delta vacuum operation failed.
    #[error("Delta vacuum operation failed: {message}")]
    VacuumFailed { message: String },

    /// Delta table operation failed.
    #[error("Delta table operation failed: {0}")]
    DeltaTable(#[from] deltalake::DeltaTableError),

    /// DataFusion error occurred.
    #[error("DataFusion error: {0}")]
    DataFusion(#[from] datafusion::error::DataFusionError),

    /// Arrow error occurred.
    #[error("Arrow error: {0}")]
    Arrow(#[from] datafusion::arrow::error::ArrowError),

    /// URL parsing error.
    #[error("Invalid URL: {0}")]
    InvalidUrl(#[from] url::ParseError),

    /// Configuration error.
    #[error("Configuration error: {message}")]
    Config { message: String },

    /// Schema validation error.
    #[error("Schema validation failed: {message}")]
    SchemaValidation { message: String },

    /// Write operation failed.
    #[error("Write operation failed: {message}")]
    WriteFailed { message: String },

    /// Generic delta error.
    #[error("Delta error: {message}")]
    Generic { message: String },
}

impl DeltaError {
    /// Create a table not found error.
    pub fn table_not_found(table: impl Into<String>) -> Self {
        Self::TableNotFound {
            table: table.into(),
        }
    }

    /// Create a merge conflict error.
    pub fn merge_conflict(message: impl Into<String>) -> Self {
        Self::MergeConflict {
            message: message.into(),
        }
    }

    /// Create a transaction failed error.
    pub fn transaction_failed(message: impl Into<String>) -> Self {
        Self::TransactionFailed {
            message: message.into(),
        }
    }

    /// Create a checkpoint failed error.
    pub fn checkpoint_failed(message: impl Into<String>) -> Self {
        Self::CheckpointFailed {
            message: message.into(),
        }
    }

    /// Create a vacuum failed error.
    pub fn vacuum_failed(message: impl Into<String>) -> Self {
        Self::VacuumFailed {
            message: message.into(),
        }
    }

    /// Create a configuration error.
    pub fn config(message: impl Into<String>) -> Self {
        Self::Config {
            message: message.into(),
        }
    }

    /// Create a schema validation error.
    pub fn schema_validation(message: impl Into<String>) -> Self {
        Self::SchemaValidation {
            message: message.into(),
        }
    }

    /// Create a write failed error.
    pub fn write_failed(message: impl Into<String>) -> Self {
        Self::WriteFailed {
            message: message.into(),
        }
    }

    /// Create a generic error.
    pub fn generic(message: impl Into<String>) -> Self {
        Self::Generic {
            message: message.into(),
        }
    }
}

/// Convenience result type for Delta operations.
pub type Result<T> = std::result::Result<T, DeltaError>;
