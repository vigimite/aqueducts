//! Prelude module that exports commonly used types and functions.
//!
//! This module provides a convenient way to import all the necessary
//! components with a single `use aqueducts_formats::prelude::*;` statement.

// Core re-exports
pub use crate::{write_file, Error, FileType, Result};

// Format-specific exports
#[cfg(feature = "parquet")]
pub use crate::parquet::write_parquet;

#[cfg(feature = "csv")]
pub use crate::csv::{write_csv, CsvOptions};

#[cfg(feature = "json")]
pub use crate::json::write_json;
