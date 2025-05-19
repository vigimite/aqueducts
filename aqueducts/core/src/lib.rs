//! # Aqueducts Core
//!
//! The core crate for the Aqueducts data pipeline framework.
//!
//! This crate provides the core abstractions and functionality for defining and executing
//! data pipelines in Aqueducts. It includes:
//!
//! - Pipeline definition and execution
//! - Data sources and destinations
//! - Processing stages
//! - Progress tracking
//!
//! ## Example
//!
//! ```no_run
//! use aqueducts_core::prelude::*;
//!
//! // Create an Aqueduct pipeline
//! # async fn example() -> aqueducts_core::Result<()> {
//! let pipeline = Aqueduct::builder()
//!     .source(Source::InMemory(InMemorySource { name: "source".into() }))
//!     .stage(Stage::new("transform".into(), "SELECT * FROM source".into(), None, false, false, false))
//!     .destination(Destination::InMemory(InMemoryDestination::new("destination".into())))
//!     .build();
//!
//! // Run the pipeline
//! let result = run_pipeline(pipeline).await?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Features
//!
//! The crate includes several feature flags to enable different functionality:
//!
//! - `s3`: Amazon S3 integration
//! - `gcs`: Google Cloud Storage integration
//! - `azure`: Azure Blob Storage integration
//! - `odbc`: ODBC database integration
//! - `yaml`: YAML file format support (enabled by default)
//! - `json`: JSON file format support
//! - `toml`: TOML file format support
//! - `schema_gen`: Schema generation support
//! - `full`: All features

use std::{collections::HashMap, path::Path};

use serde::{Deserialize, Serialize};

// Core modules
pub mod error;
pub mod execution; // Runtime execution behavior
pub mod model; // Data models and serialization
pub mod pipeline; // Pipeline definition and execution logic
pub mod prelude; // Common imports

// Re-exports for convenience
pub use model::destinations::Destination;
pub use model::sources::Source;
pub use model::stages::OutputType;
pub use model::stages::Stage;
pub use pipeline::progress_tracker::ProgressEvent;
pub use pipeline::progress_tracker::ProgressTracker;
pub use pipeline::run_pipeline;

pub type Result<T> = core::result::Result<T, error::Error>;

/// Definition for an `Aqueduct` data pipeline
#[derive(Debug, Clone, Serialize, Deserialize, derive_new::new)]
#[cfg_attr(feature = "schema_gen", derive(schemars::JsonSchema))]
pub struct Aqueduct {
    /// Definition of the data sources for this pipeline
    pub sources: Vec<Source>,

    /// A sequential list of transformations to execute within the context of this pipeline
    /// Nested stages are executed in parallel
    pub stages: Vec<Vec<Stage>>,

    /// Destination for the final step of the `Aqueduct`
    /// takes the last stage as input for the write operation
    pub destination: Option<Destination>,
}

impl Aqueduct {
    /// Builder for an Aqueduct pipeline
    pub fn builder() -> AqueductBuilder {
        AqueductBuilder::default()
    }

    // Format loading functions should be implemented in the aqueducts-formats crate
    // These are temporary implementations until that crate is fully implemented
    #[cfg(feature = "json")]
    pub fn try_from_json<P>(path: P, params: HashMap<String, String>) -> Result<Self>
    where
        P: AsRef<Path>,
    {
        let raw = std::fs::read_to_string(path)?;
        let parsed = serde_json::from_str::<serde_json::Value>(raw.as_str())?;
        let parsed = serde_json::to_string(&parsed)?;
        let definition = Self::substitute_params(parsed.as_str(), params)?;
        let aqueduct = serde_json::from_str::<Aqueduct>(definition.as_str())?;

        Ok(aqueduct)
    }

    #[cfg(feature = "toml")]
    pub fn try_from_toml<P>(path: P, params: HashMap<String, String>) -> Result<Self>
    where
        P: AsRef<Path>,
    {
        let raw = std::fs::read_to_string(path)?;
        let parsed = toml::from_str::<toml::Value>(raw.as_str())?;
        let parsed = toml::to_string(&parsed)?;
        let definition = Self::substitute_params(parsed.as_str(), params)?;
        let aqueduct = toml::from_str::<Aqueduct>(definition.as_str())?;

        Ok(aqueduct)
    }

    #[cfg(feature = "yaml")]
    pub fn try_from_yml<P>(path: P, params: HashMap<String, String>) -> Result<Self>
    where
        P: AsRef<Path>,
    {
        let raw = std::fs::read_to_string(path)?;
        let parsed = serde_yml::from_str::<serde_yml::Value>(raw.as_str())?;
        let parsed = serde_yml::to_string(&parsed)?;
        let definition = Self::substitute_params(parsed.as_str(), params)?;
        let aqueduct = serde_yml::from_str::<Aqueduct>(definition.as_str())?;

        Ok(aqueduct)
    }

    #[cfg(feature = "json")]
    pub fn try_from_json_str(contents: &str, params: HashMap<String, String>) -> Result<Self> {
        let parsed = serde_json::from_str::<serde_json::Value>(contents)?;
        let parsed = serde_json::to_string(&parsed)?;
        let definition = Self::substitute_params(parsed.as_str(), params)?;
        let aqueduct = serde_json::from_str::<Aqueduct>(definition.as_str())?;

        Ok(aqueduct)
    }

    #[cfg(feature = "toml")]
    pub fn try_from_toml_str(contents: &str, params: HashMap<String, String>) -> Result<Self> {
        let parsed = toml::from_str::<toml::Value>(contents)?;
        let parsed = toml::to_string(&parsed)?;
        let definition = Self::substitute_params(parsed.as_str(), params)?;
        let aqueduct = toml::from_str::<Aqueduct>(definition.as_str())?;

        Ok(aqueduct)
    }

    #[cfg(feature = "yaml")]
    pub fn try_from_yml_str(contents: &str, params: HashMap<String, String>) -> Result<Self> {
        let parsed = serde_yml::from_str::<serde_yml::Value>(contents)?;
        let parsed = serde_yml::to_string(&parsed)?;
        let definition = Self::substitute_params(parsed.as_str(), params)?;
        let aqueduct = serde_yml::from_str::<Aqueduct>(definition.as_str())?;

        Ok(aqueduct)
    }

    fn substitute_params(raw: &str, params: HashMap<String, String>) -> Result<String> {
        let mut definition = raw.to_string();

        // Static regex to avoid recreating it
        static PARAM_REGEX_INSTANCE: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
        let param_regex = PARAM_REGEX_INSTANCE
            .get_or_init(|| regex::Regex::new("\\$\\{([a-zA-Z0-9_]+)\\}").expect("invalid regex"));

        params.into_iter().for_each(|(name, value)| {
            let template = format!("${{{name}}}");
            definition = definition.replace(template.as_str(), value.as_str());
        });

        let captures = param_regex.captures_iter(definition.as_str());

        let missing_params = captures
            .map(|capture| {
                let param = capture
                    .get(1)
                    .expect("no capture group found")
                    .as_str()
                    .to_string();
                param
            })
            .collect::<std::collections::HashSet<String>>();

        if !missing_params.is_empty() {
            let error = error::Error::MissingParams(missing_params);
            return Err(error);
        }

        Ok(definition)
    }
}

/// Builder for an Aqueduct pipeline
#[derive(Debug, Clone, Default, Serialize, Deserialize, derive_new::new)]
pub struct AqueductBuilder {
    sources: Vec<Source>,
    stages: Vec<Vec<Stage>>,
    destination: Option<Destination>,
}

impl AqueductBuilder {
    /// Add source to builder
    pub fn source(mut self, source: Source) -> Self {
        self.sources.push(source);
        self
    }

    /// Add stage to builder
    pub fn stage(mut self, stage: Stage) -> Self {
        self.stages.push(vec![stage]);
        self
    }

    /// Set destination to builder
    pub fn destination(mut self, destination: Destination) -> Self {
        self.destination = Some(destination);
        self
    }

    /// Build Aqueduct pipeline
    pub fn build(self) -> Aqueduct {
        Aqueduct::new(self.sources, self.stages, self.destination)
    }
}

/// Register handlers for the object stores enabled by the selected feature flags (s3, gcs, azure)
/// Stores can alternatively be provided by a custom context passed to `run_pipeline`
///
/// This function should be called before running pipelines that use object store sources or destinations.
/// It will register the handlers for all object stores enabled by feature flags.
pub fn register_handlers() {
    // Forward to storage module if available
    aqueducts_storage::register_handlers();
}
