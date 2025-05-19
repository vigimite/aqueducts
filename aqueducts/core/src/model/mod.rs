//! Data models for pipeline definitions
//!
//! This module contains the data models used for defining Aqueducts pipelines.
//! These models are serializable and deserializable, allowing them to be defined
//! in configuration files (YAML, JSON, TOML) and loaded at runtime.
//!
//! The module includes:
//!
//! - Source definitions for data inputs
//! - Destination definitions for data outputs
//! - Stage definitions for data transformations

pub mod destinations;
pub mod sources;
pub mod stages;
