//! Prelude module that exports commonly used types and functions.
//!
//! This module provides a convenient way to import all the necessary
//! components with a single `use aqueducts_storage::prelude::*;` statement.

// Core re-exports
pub use crate::{register_handlers, register_object_store, Error, Result};

// Serde utilities
pub use crate::serde::deserialize_file_location;
