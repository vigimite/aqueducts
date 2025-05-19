//! Prelude module that exports commonly used types and functions.
//!
//! This module provides a convenient way to import all the necessary
//! components with a single `use aqueducts_delta::prelude::*;` statement.

// Core re-exports
pub use crate::{DeltaDestination, Error, ReplaceCondition, Result, WriteMode};

// Function re-exports
pub use crate::{create, register_destination, write};
