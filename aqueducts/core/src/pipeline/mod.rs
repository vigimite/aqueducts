//! Pipeline definitions and execution logic
//!
//! This module contains the core functionality for executing Aqueducts pipelines.
//! It includes:
//!
//! - Pipeline execution through the `run_pipeline` function
//! - Progress tracking through the `ProgressTracker` trait
//! - Event definitions for pipeline execution stages

use crate::error;

pub mod progress_tracker;
pub use progress_tracker::{ProgressEvent, ProgressTracker};

pub mod run;
pub use run::run_pipeline;

pub type Result<T> = core::result::Result<T, error::Error>;
