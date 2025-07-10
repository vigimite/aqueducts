use std::{collections::HashSet, path::PathBuf};

use crate::templating::TemplateFormat;

pub type Result<T> = core::result::Result<T, AqueductsError>;

#[derive(Debug, thiserror::Error)]
pub enum AqueductsError {
    // === Configuration & Setup Errors ===
    #[error("Unsupported operation: {operation} for {context}")]
    Unsupported { operation: String, context: String },

    // === Data Processing Errors ===
    #[error("Schema validation failed: {message}")]
    SchemaValidation { message: String },

    #[error("Data processing failed: {message}")]
    DataProcessing { message: String },

    // === I/O & Storage Errors ===
    #[error("Storage operation failed: {operation} at {location}")]
    Storage { operation: String, location: String },

    #[error("File operation failed: {message}")]
    FileOperation { message: String },

    // === Pipeline Execution Errors ===
    #[error("Source '{name}' failed: {message}")]
    Source { name: String, message: String },

    #[error("Stage '{name}' failed: {message}")]
    Stage { name: String, message: String },

    #[error("Destination '{name}' failed: {message}")]
    Destination { name: String, message: String },

    // === Template & Parsing Errors ===
    #[error("Template error: {message}")]
    Template { message: String },

    #[error("Parse error: {message}")]
    Parse { message: String },

    // === Resource Management ===
    #[error("Resource not found: {resource} at {location}")]
    NotFound { resource: String, location: String },
}

impl AqueductsError {
    // === Templating ===
    pub fn unsupported(operation: impl Into<String>, context: impl Into<String>) -> Self {
        Self::Unsupported {
            operation: operation.into(),
            context: context.into(),
        }
    }

    // === Data Processing ===
    pub fn schema_validation(message: impl Into<String>) -> Self {
        Self::SchemaValidation {
            message: message.into(),
        }
    }

    pub fn data_processing(message: impl Into<String>) -> Self {
        Self::DataProcessing {
            message: message.into(),
        }
    }

    // === I/O & Storage ===
    pub fn storage(operation: impl Into<String>, location: impl Into<String>) -> Self {
        Self::Storage {
            operation: operation.into(),
            location: location.into(),
        }
    }

    pub fn file_operation(message: impl Into<String>) -> Self {
        Self::FileOperation {
            message: message.into(),
        }
    }

    // === Pipeline Execution ===
    pub fn source(name: impl Into<String>, message: impl Into<String>) -> Self {
        Self::Source {
            name: name.into(),
            message: message.into(),
        }
    }

    pub fn stage(name: impl Into<String>, message: impl Into<String>) -> Self {
        Self::Stage {
            name: name.into(),
            message: message.into(),
        }
    }

    pub fn destination(name: impl Into<String>, message: impl Into<String>) -> Self {
        Self::Destination {
            name: name.into(),
            message: message.into(),
        }
    }

    // === Template & Parsing ===
    pub fn template(message: impl Into<String>) -> Self {
        Self::Template {
            message: message.into(),
        }
    }

    pub fn parse(message: impl Into<String>) -> Self {
        Self::Parse {
            message: message.into(),
        }
    }

    // === Resource Management ===
    pub fn not_found(resource: impl Into<String>, location: impl Into<String>) -> Self {
        Self::NotFound {
            resource: resource.into(),
            location: location.into(),
        }
    }
}

// === External Error Conversions ===

impl From<std::io::Error> for AqueductsError {
    fn from(err: std::io::Error) -> Self {
        Self::FileOperation {
            message: err.to_string(),
        }
    }
}

impl From<datafusion::error::DataFusionError> for AqueductsError {
    fn from(err: datafusion::error::DataFusionError) -> Self {
        use datafusion::error::DataFusionError as DF;
        match err {
            DF::SchemaError(_, _) => Self::SchemaValidation {
                message: err.to_string(),
            },
            DF::ArrowError(_, _) => Self::DataProcessing {
                message: err.to_string(),
            },
            DF::IoError(_) => Self::FileOperation {
                message: err.to_string(),
            },
            _ => Self::DataProcessing {
                message: err.to_string(),
            },
        }
    }
}

impl From<datafusion::arrow::error::ArrowError> for AqueductsError {
    fn from(err: datafusion::arrow::error::ArrowError) -> Self {
        use datafusion::arrow::error::ArrowError as AE;
        match err {
            AE::SchemaError(_) => Self::SchemaValidation {
                message: err.to_string(),
            },
            AE::ComputeError(_) => Self::DataProcessing {
                message: err.to_string(),
            },
            AE::IoError(_, _) => Self::FileOperation {
                message: err.to_string(),
            },
            AE::ParseError(_) => Self::Parse {
                message: err.to_string(),
            },
            _ => Self::DataProcessing {
                message: err.to_string(),
            },
        }
    }
}

impl From<object_store::Error> for AqueductsError {
    fn from(err: object_store::Error) -> Self {
        Self::Storage {
            operation: "object_store".to_string(),
            location: err.to_string(),
        }
    }
}

impl From<regex::Error> for AqueductsError {
    fn from(err: regex::Error) -> Self {
        Self::Parse {
            message: format!("Regex error: {err}"),
        }
    }
}

#[cfg(feature = "json")]
impl From<serde_json::Error> for AqueductsError {
    fn from(err: serde_json::Error) -> Self {
        Self::Parse {
            message: format!("JSON error: {err}"),
        }
    }
}

#[cfg(feature = "toml")]
impl From<toml::de::Error> for AqueductsError {
    fn from(err: toml::de::Error) -> Self {
        Self::Parse {
            message: format!("TOML deserialization error: {err}"),
        }
    }
}

#[cfg(feature = "toml")]
impl From<toml::ser::Error> for AqueductsError {
    fn from(err: toml::ser::Error) -> Self {
        Self::Parse {
            message: format!("TOML serialization error: {err}"),
        }
    }
}

#[cfg(feature = "yaml")]
impl From<serde_yml::Error> for AqueductsError {
    fn from(err: serde_yml::Error) -> Self {
        Self::Parse {
            message: format!("YAML error: {err}"),
        }
    }
}

// === Legacy Support for Template-Specific Errors ===

impl From<HashSet<String>> for AqueductsError {
    fn from(missing_params: HashSet<String>) -> Self {
        Self::Template {
            message: format!("Missing template parameters: {missing_params:?}"),
        }
    }
}

impl From<(PathBuf, &'static str)> for AqueductsError {
    fn from((path, context): (PathBuf, &'static str)) -> Self {
        Self::Template {
            message: format!("{context}: {path:?}"),
        }
    }
}

impl From<TemplateFormat> for AqueductsError {
    fn from(format: TemplateFormat) -> Self {
        Self::Unsupported {
            operation: "template format".to_string(),
            context: format!(
                "{format:?} support is not enabled in this build. Enable the corresponding feature flag"
            ),
        }
    }
}
