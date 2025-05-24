//! # Aqueducts Delta Lake Integration
//!
//! This crate provides Delta Lake support for Aqueducts pipelines, enabling both reading from
//! and writing to Delta tables with full transaction support and ACID guarantees.
//!
//! ## Features
//!
//! - **DeltaSource**: Read from Delta tables with optional time travel and filtering
//! - **DeltaDestination**: Write to Delta tables with various modes (append, overwrite, upsert)
//! - **Object Store Integration**: Automatic registration of object stores for cloud storage
//! - **Schema Evolution**: Automatic schema merging and evolution support
//!
//! ## Usage
//!
//! This crate is typically used through the main `aqueducts` meta-crate with the `delta` feature:
//!
//! ```toml
//! [dependencies]
//! aqueducts = { version = "0.9", features = ["delta"] }
//! ```
//!
//! The Delta integration is automatically registered when the feature is enabled.
//! Configure Delta sources and destinations in your pipeline YAML/JSON/TOML files:
//!
//! ```yaml
//! sources:
//!   - name: delta_source
//!     delta:
//!       path: s3://my-bucket/delta-table/
//!       # Optional time travel
//!       version: 10
//!
//! destination:
//!   delta:
//!     path: s3://my-bucket/output-table/
//!     mode: upsert
//!     upsert_condition: target.id = source.id
//! ```

// Internal implementation modules - not part of public API
#[doc(hidden)]
pub mod destination;
mod error;
#[doc(hidden)]
pub mod source;

// Public registration function
pub mod handlers;
pub use handlers::register_handlers;

// Re-export error types through main error system
pub use error::{DeltaError, Result};

// Re-export configuration schema types for user convenience
pub use aqueducts_schemas::{DeltaDestination, DeltaSource, DeltaWriteMode, ReplaceCondition};

// Re-export commonly used Delta Lake types for advanced usage
pub use deltalake::{
    kernel::{StructField, StructType},
    protocol::SaveMode,
    DeltaTable, DeltaTableBuilder,
};
