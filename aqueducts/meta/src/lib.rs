//! # Aqueducts - Data Pipeline Framework
//!
//! Aqueducts is a declarative framework for building ETL (Extract, Transform, Load) data pipelines.
//! It allows you to define complex data processing workflows using configuration files in JSON, YAML, or TOML formats.
//!
//! ## Features
//!
//! This crate provides a unified interface to all Aqueducts functionality through feature flags:
//!
//! ### Format Support
//! - **`json`** - Enable JSON configuration file support
//! - **`toml`** - Enable TOML configuration file support  
//! - **`yaml`** - Enable YAML configuration file support (enabled by default)
//!
//! ### Cloud Storage Providers
//! - **`s3`** - Amazon S3 and S3-compatible storage support (enabled by default)
//! - **`gcs`** - Google Cloud Storage support (enabled by default)
//! - **`azure`** - Azure Blob Storage support (enabled by default)
//!
//! ### Database Connectivity
//! - **`odbc`** - ODBC database connectivity for sources and destinations
//! - **`delta`** - Delta Lake table support for advanced analytics workloads
//!
//! ### Development Features
//! - **`schema_gen`** - JSON schema generation for configuration validation
//! - **`protocol`** - WebSocket protocol support for distributed execution
//!
//! ## Quick Start
//!
//! ```rust,no_run
//! use aqueducts::prelude::*;
//! use datafusion::prelude::SessionContext;
//! use std::sync::Arc;
//!
//! #[tokio::main]
//! async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
//!     // Load pipeline configuration
//!     let pipeline = Aqueduct::from_file("pipeline.yml", Default::default())?;
//!     
//!     // Create DataFusion context
//!     let ctx = Arc::new(SessionContext::new());
//!     
//!     // Execute pipeline
//!     let _result_ctx = run_pipeline(ctx, pipeline, None).await?;
//!     
//!     Ok(())
//! }
//! ```
//!
//! ## Configuration Examples
//!
//! ### Basic File Processing Pipeline
//!
//! ```yaml
//! version: "v2"
//! sources:
//!   - type: file
//!     name: sales_data
//!     format:
//!       type: csv
//!       options:
//!         has_header: true
//!         delimiter: ","
//!     location: "s3://my-bucket/sales.csv"
//!
//! stages:
//!   - - name: process_sales
//!       query: |
//!         SELECT
//!           product_id,
//!           SUM(quantity) as total_quantity,
//!           SUM(amount) as total_amount
//!         FROM sales_data
//!         GROUP BY product_id
//!
//! destination:
//!   type: file
//!   name: processed_sales
//!   format:
//!     type: parquet
//!     options: {}
//!   location: "s3://my-bucket/processed/output.parquet"
//! ```
//!
//! ### Working with Delta Tables
//!
//! ```yaml
//! version: "v2"
//! sources:
//!   - type: delta
//!     name: events
//!     location: "s3://data-lake/events/"
//!     storage_config: {}
//!       
//! stages:
//!   - - name: daily_summary
//!       query: |
//!         SELECT
//!           DATE(timestamp) as date,
//!           event_type,
//!           COUNT(*) as event_count
//!         FROM events
//!         WHERE DATE(timestamp) = CURRENT_DATE
//!         GROUP BY DATE(timestamp), event_type
//!
//! destination:
//!   type: delta
//!   name: daily_metrics
//!   location: "s3://data-lake/metrics/"
//!   storage_config: {}
//!   table_properties: {}
//!   write_mode:
//!     operation: append
//! ```
//!
//! ## Feature Flag Guide
//!
//! When using Aqueducts in your `Cargo.toml`, enable only the features you need:
//!
//! ```toml
//! [dependencies]
//! # Minimal setup with just local file processing
//! aqueducts = { version = "0.9", default-features = false, features = ["yaml"] }
//!
//! # Cloud data processing with S3 and Delta Lake
//! aqueducts = { version = "0.9", features = ["yaml", "s3", "delta"] }
//!
//! # Full-featured setup with all storage providers and formats
//! aqueducts = { version = "0.9", features = ["json", "toml", "yaml", "s3", "gcs", "azure", "odbc", "delta"] }
//! ```
//!
//! ## Error Handling
//!
//! All operations return semantic errors through the unified [`AqueductsError`] type:
//!
//! ```rust
//! use aqueducts::prelude::*;
//! use datafusion::prelude::SessionContext;
//! use std::sync::Arc;
//!
//! async fn example() -> Result<()> {
//!     let pipeline = Aqueduct::from_file("pipeline.yml", Default::default())?;
//!     let ctx = Arc::new(SessionContext::new());
//!     
//!     match run_pipeline(ctx, pipeline, None).await {
//!         Ok(result) => println!("Pipeline executed successfully"),
//!         Err(AqueductsError::Source { name, message }) => {
//!             eprintln!("Source '{}' failed: {}", name, message);
//!         }
//!         Err(AqueductsError::SchemaValidation { message }) => {
//!             eprintln!("Schema validation error: {}", message);
//!         }
//!         Err(err) => eprintln!("Pipeline error: {}", err),
//!     }
//!     Ok(())
//! }
//! ```

// Core functionality re-exports
pub use aqueducts_core::{
    error::{AqueductsError, Result},
    progress_tracker::{LoggingProgressTracker, ProgressTracker},
    run_pipeline,
    templating::{TemplateFormat, TemplateLoader},
};

// Optional crate re-exports
#[cfg(feature = "odbc")]
pub use aqueducts_odbc as odbc;

#[cfg(feature = "delta")]
pub use aqueducts_delta as delta;

/// Prelude module for convenient imports
///
/// This module re-exports the most commonly used types and functions for pipeline development.
/// Import this module to get access to all the essential Aqueducts functionality.
///
/// ```rust
/// use aqueducts::prelude::*;
/// ```
pub mod prelude {
    // Core pipeline functionality
    pub use crate::{run_pipeline, AqueductsError, Result};

    // Progress tracking
    pub use crate::{LoggingProgressTracker, ProgressTracker};
    pub use aqueducts_schemas::{OutputType, ProgressEvent};

    // Template loading
    pub use crate::{TemplateFormat, TemplateLoader};

    // Schema types - all pipeline configuration types
    pub use aqueducts_schemas::*;

    // DataFusion essentials that users typically need
    pub use datafusion::prelude::SessionContext;

    // Common async runtime
    pub use tokio;

    // Logging
    pub use tracing::{debug, error, info, warn};
}
