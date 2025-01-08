use std::collections::HashSet;

pub type Result<T> = core::result::Result<T, Box<dyn std::error::Error>>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Missing template parameters: {0:?}")]
    MissingParams(HashSet<String>),

    // -- Modules
    #[error("Failed to register source: {0}")]
    SourceError(#[from] super::sources::error::Error),
    #[error("Failed to process stage: {0}")]
    StageError(#[from] super::stages::error::Error),
    #[error("Failed to write data to destination: {0}")]
    DestinationError(#[from] super::destinations::error::Error),

    // -- External
    #[error("Failed to read definition file: {0}")]
    IoError(#[from] std::io::Error),
    #[cfg(feature = "json")]
    #[error("Failed to deserialize definition file: {0}")]
    JsonError(#[from] serde_json::Error),
    #[cfg(feature = "toml")]
    #[error("Failed to deserialize definition file: {0}")]
    TomlDeserializationError(#[from] toml::de::Error),
    #[cfg(feature = "toml")]
    #[error("Failed to serialize definition file: {0}")]
    TomlSerializationError(#[from] toml::ser::Error),
    #[cfg(feature = "yaml")]
    #[error("Failed to deserialize definition file: {0}")]
    YmlError(#[from] serde_yml::Error),
    #[error("Failed to build regex: {0}")]
    RegexError(#[from] regex::Error),
    #[error("Failed to read output table: {0}")]
    ReadTableError(#[from] datafusion::error::DataFusionError),
}
