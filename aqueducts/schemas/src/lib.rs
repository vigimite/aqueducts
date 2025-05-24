//! # Aqueducts Schemas
//!
//! This crate contains all the schema definitions and configuration types used
//! throughout the aqueducts ecosystem. By centralizing these types here, we avoid
//! circular dependencies between core, delta, ODBC, and other provider crates.

use bon::Builder;
use serde::{Deserialize, Serialize};

pub mod data_types;
pub mod destinations;
pub mod location;
pub mod progress;
pub mod sources;
pub mod stages;

mod serde_helpers;

#[cfg(feature = "protocol")]
pub mod protocol;

// Re-export the main types for convenience
pub use data_types::{DataType, Field, IntervalUnit, TimeUnit, UnionMode};
pub use destinations::{
    CsvDestinationOptions, CsvDestinationOptionsBuilder, DeltaWriteMode,
    FileType as DestinationFileType, ReplaceCondition,
};
pub use destinations::{
    DeltaDestination, Destination, FileDestination, InMemoryDestination, OdbcDestination,
};
pub use destinations::{
    DeltaDestinationBuilder, FileDestinationBuilder, InMemoryDestinationBuilder,
    OdbcDestinationBuilder,
};
pub use location::Location;
pub use sources::{
    CsvSourceOptions, FileType as SourceFileType, JsonSourceOptions, ParquetSourceOptions,
};
pub use sources::{CsvSourceOptionsBuilder, JsonSourceOptionsBuilder, ParquetSourceOptionsBuilder};
pub use sources::{DeltaSource, DirSource, FileSource, InMemorySource, OdbcSource, Source};
pub use sources::{
    DeltaSourceBuilder, DirSourceBuilder, FileSourceBuilder, InMemorySourceBuilder,
    OdbcSourceBuilder,
};

pub use progress::{OutputType, ProgressEvent};
pub use stages::{Stage, StageBuilder};

#[cfg(feature = "protocol")]
pub use protocol::*;

fn current_version() -> String {
    "v2".to_string()
}

/// Definition for an `Aqueduct` data pipeline.
///
/// An aqueduct defines a complete data processing pipeline with sources, transformation stages,
/// and an optional destination. Most configuration uses sensible defaults to minimize verbosity.
///
/// # Examples
///
/// ```
/// use aqueducts_schemas::{Aqueduct, Source, FileSource, SourceFileType, CsvSourceOptions, Stage};
///
/// // Complete pipeline with defaults - version defaults to "v2"
/// let pipeline = Aqueduct::builder()
///     .sources(vec![
///         Source::File(
///             FileSource::builder()
///                 .name("sales".to_string())
///                 .format(SourceFileType::Csv(CsvSourceOptions::default()))
///                 .location("./sales.csv".try_into().unwrap())
///                 .build()
///         )
///     ])
///     .stages(vec![vec![
///         Stage::builder()
///             .name("totals".to_string())
///             .query("SELECT region, SUM(amount) as total FROM sales GROUP BY region".to_string())
///             .build()
///     ]])
///     .build();
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, Builder)]
#[cfg_attr(feature = "schema_gen", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub struct Aqueduct {
    /// Schema version for migration compatibility
    #[serde(default = "current_version")]
    #[builder(default = current_version())]
    pub version: String,

    /// Definition of the data sources for this pipeline
    pub sources: Vec<Source>,

    /// A sequential list of transformations to execute within the context of this pipeline
    /// Nested stages are executed in parallel
    pub stages: Vec<Vec<Stage>>,

    /// Destination for the final step of the `Aqueduct`
    /// takes the last stage as input for the write operation
    pub destination: Option<Destination>,
}
