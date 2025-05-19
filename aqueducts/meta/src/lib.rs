//! Aqueducts - A framework to build ETL data pipelines declaratively
//!
//! This crate is a meta-package that re-exports the most commonly used
//! types and functions from the Aqueducts ecosystem. It provides a convenient
//! way to include all the necessary functionality in your project.
//!
//! # Features
//!
//! - **core**: Core functionality (enabled by default)
//! - **storage**: Storage backends (enabled by default)
//! - **s3**: Amazon S3 integration
//! - **gcs**: Google Cloud Storage integration
//! - **azure**: Azure Blob Storage integration
//! - **formats**: File formats support
//! - **yaml**: YAML support (enabled by default)
//! - **json**: JSON support
//! - **toml**: TOML support
//! - **parquet**: Parquet format support
//! - **csv**: CSV format support
//! - **json_format**: JSON format support
//! - **avro**: Avro format support
//! - **delta**: Delta Lake integration
//! - **odbc**: ODBC integration
//! - **protocol**: Communication protocol definitions
//! - **full**: All features
//!
//! # Examples
//!
//! ```no_run
//! use aqueducts::prelude::*;
//!
//! // Create an Aqueduct pipeline
//! let pipeline = Aqueduct::builder()
//!     .source(Source::InMemory(InMemorySource::new("source".into())))
//!     .stage(Stage::new("transform".into(), "SELECT * FROM source".into(), vec![], None))
//!     .destination(Destination::InMemory(InMemoryDestination::new("destination".into())))
//!     .build();
//!
//! // Run the pipeline
//! # async fn run() -> aqueducts::Result<()> {
//! let result = run_pipeline(pipeline).await?;
//! # Ok(())
//! # }
//! ```

// Re-exports from core
#[cfg(feature = "core")]
pub use aqueducts_core::{self as core, prelude as core_prelude};

#[cfg(feature = "core")]
pub use aqueducts_core::{
    error, execution, model, pipeline,
    pipeline::progress_tracker::{LoggingProgressTracker, ProgressEvent, ProgressTracker},
    pipeline::run_pipeline,
    register_handlers, Aqueduct, AqueductBuilder, Destination, OutputType, Result, Source, Stage,
};

// Re-exports from delta
#[cfg(feature = "delta")]
pub use aqueducts_delta::{self as delta, prelude as delta_prelude};

// Re-exports from formats
#[cfg(feature = "formats")]
pub use aqueducts_formats::{self as formats, prelude as formats_prelude};

// Re-exports from odbc
#[cfg(feature = "odbc")]
pub use aqueducts_odbc as odbc;

// Re-exports from protocol
#[cfg(feature = "protocol")]
pub use aqueducts_protocol::{self as protocol, prelude as protocol_prelude};

// Re-exports from storage
#[cfg(feature = "storage")]
pub use aqueducts_storage::{self as storage, prelude as storage_prelude};

/// Prelude module that exports commonly used types and functions.
///
/// This module provides a convenient way to import all the necessary
/// components with a single `use aqueducts::prelude::*;` statement.
pub mod prelude {
    // Re-export types from core
    #[cfg(feature = "core")]
    pub use aqueducts_core::{
        pipeline::progress_tracker::{ProgressEvent, ProgressTracker},
        pipeline::run_pipeline,
        Aqueduct, AqueductBuilder, Destination, OutputType, Result, Source, Stage,
    };

    // Re-export prelude modules from all crates
    #[cfg(feature = "core")]
    pub use aqueducts_core::prelude::*;

    #[cfg(feature = "delta")]
    pub use aqueducts_delta::prelude::*;

    #[cfg(feature = "formats")]
    pub use aqueducts_formats::prelude::*;

    #[cfg(feature = "protocol")]
    pub use aqueducts_protocol::prelude::*;

    #[cfg(feature = "storage")]
    pub use aqueducts_storage::prelude::*;
}

/// A simpler API for common use cases
#[cfg(feature = "core")]
pub mod api {
    use super::*;
    use std::collections::HashMap;
    use std::path::Path;

    /// Run a pipeline from a file
    ///
    /// This is a convenience function that loads a pipeline from a file
    /// and runs it. The file format is determined by the file extension.
    #[cfg(any(feature = "yaml", feature = "json", feature = "toml"))]
    pub async fn run_pipeline_from_file<P: AsRef<Path>>(
        path: P,
        params: HashMap<String, String>,
    ) -> core::Result<()> {
        let path_ref = path.as_ref();
        let extension = path_ref
            .extension()
            .and_then(|ext| ext.to_str())
            .ok_or_else(|| {
                core::error::Error::Other(format!("Invalid file extension: {:?}", path_ref))
            })?;

        let pipeline = match extension.to_lowercase().as_str() {
            #[cfg(feature = "yaml")]
            "yml" | "yaml" => core::Aqueduct::try_from_yml(path, params)?,
            #[cfg(feature = "json")]
            "json" => core::Aqueduct::try_from_json(path, params)?,
            #[cfg(feature = "toml")]
            "toml" => core::Aqueduct::try_from_toml(path, params)?,
            _ => {
                return Err(core::error::Error::Other(format!(
                    "Unsupported file extension: {}",
                    extension
                )))
            }
        };

        let ctx = std::sync::Arc::new(datafusion::prelude::SessionContext::new());
        core::pipeline::run_pipeline(ctx, pipeline, None).await?;
        Ok(())
    }
}
