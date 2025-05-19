//! Prelude module that exports commonly used types and functions.
//!
//! This module provides a convenient way to import all the necessary
//! components with a single `use aqueducts_core::prelude::*;` statement.

// Core types
pub use crate::{Aqueduct, AqueductBuilder, Result};

// Progress tracking
pub use crate::pipeline::progress_tracker::{
    LoggingProgressTracker, ProgressEvent, ProgressTracker,
};

// Pipeline execution
pub use crate::pipeline::run_pipeline;

// Model types
pub use crate::model::destinations::{self as model_destinations, Destination};
pub use crate::model::sources::{self as model_sources, Source};
pub use crate::model::stages::{self as model_stages, OutputType, Stage};

// Execution functionality
pub use crate::execution::destinations::{
    self as execution_destinations, register_destination, write_to_destination,
};
pub use crate::execution::sources::{self as execution_sources, register_source};
pub use crate::execution::stages::{self as execution_stages, execute_stage};

// Trait definitions
pub use crate::execution::traits::{DestinationProvider, SourceProvider, StageProvider};

// Nothing else to add for legacy compatibility
#[doc(hidden)]
pub mod legacy {
    // Legacy types were removed during restructuring
}
