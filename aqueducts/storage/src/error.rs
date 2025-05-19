//! Error types for storage operations

use thiserror::Error;

/// Errors that can occur during storage operations
#[derive(Debug, Error)]
pub enum Error {
    /// Object store creation failed
    #[error("Failed to create object store: {0}")]
    ObjectStoreCreation(String),

    /// URL scheme not supported
    #[error("URL scheme not supported: {0}")]
    UnsupportedScheme(String),

    /// Object store error
    #[error("Object store error: {0}")]
    ObjectStore(#[from] object_store::Error),

    /// Object store configuration error
    #[error("Object store configuration error: {0}")]
    ObjectStoreConfiguration(String),

    /// Error registering object store with DataFusion
    #[error("Error registering object store with DataFusion: {0}")]
    DataFusionObjectStoreRegistration(String),
}
