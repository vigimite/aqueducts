use thiserror::Error;

/// Error types for ODBC operations with security-conscious error messages.
///
/// IMPORTANT: This type never includes connection strings or other sensitive
/// information in error messages to prevent password leakage.
#[derive(Error, Debug)]
pub enum OdbcError {
    /// ODBC connection failed (no sensitive details exposed).
    #[error("ODBC connection failed to data source")]
    ConnectionFailed,

    /// ODBC query execution failed.
    #[error("ODBC query execution failed: {message}")]
    QueryFailed { message: String },

    /// ODBC write operation failed.
    #[error("ODBC write operation failed: {message}")]
    WriteFailed { message: String },

    /// ODBC driver or environment setup error.
    #[error("ODBC driver error: {message}")]
    DriverError { message: String },

    /// Arrow error occurred.
    #[error("Arrow error: {0}")]
    Arrow(#[from] datafusion::arrow::error::ArrowError),

    /// DataFusion error occurred.
    #[error("DataFusion error: {0}")]
    DataFusion(#[from] datafusion::error::DataFusionError),
}

impl OdbcError {
    /// Create a connection failed error.
    pub fn connection_failed() -> Self {
        Self::ConnectionFailed
    }

    /// Create a query failed error.
    pub fn query_failed(message: impl Into<String>) -> Self {
        Self::QueryFailed {
            message: message.into(),
        }
    }

    /// Create a write failed error.
    pub fn write_failed(message: impl Into<String>) -> Self {
        Self::WriteFailed {
            message: message.into(),
        }
    }

    /// Create a driver error.
    pub fn driver_error(message: impl Into<String>) -> Self {
        Self::DriverError {
            message: message.into(),
        }
    }
}

// External error mappings with security considerations
impl From<arrow_odbc::Error> for OdbcError {
    fn from(err: arrow_odbc::Error) -> Self {
        // Don't expose details that might contain sensitive information
        let err_str = err.to_string().to_lowercase();
        if err_str.contains("connection")
            || err_str.contains("login")
            || err_str.contains("authentication")
        {
            Self::ConnectionFailed
        } else {
            Self::DriverError {
                message: "ODBC operation failed".to_string(),
            }
        }
    }
}

impl From<arrow_odbc::odbc_api::Error> for OdbcError {
    fn from(err: arrow_odbc::odbc_api::Error) -> Self {
        // Check if this is a connection-related error without exposing details
        let err_str = err.to_string().to_lowercase();
        if err_str.contains("connection")
            || err_str.contains("login")
            || err_str.contains("authentication")
        {
            Self::ConnectionFailed
        } else {
            Self::DriverError {
                message: "ODBC API error".to_string(),
            }
        }
    }
}

impl From<arrow_odbc::WriterError> for OdbcError {
    fn from(_err: arrow_odbc::WriterError) -> Self {
        Self::WriteFailed {
            message: "ODBC write operation failed".to_string(),
        }
    }
}

/// Convenience result type for ODBC operations.
pub type Result<T> = std::result::Result<T, OdbcError>;
