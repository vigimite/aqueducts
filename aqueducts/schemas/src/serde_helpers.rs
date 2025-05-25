//! Shared serde helper functions for deserialization and default values.
//!
//! This module consolidates common serde helpers used across the schema types

use crate::data_types::DataType;
use serde::{Deserialize, Deserializer};
use std::str::FromStr;

// =============================================================================
// Default value functions
// =============================================================================

/// Default value for boolean fields that should be true
pub fn default_true() -> bool {
    true
}

/// Default comma delimiter for CSV files
pub fn default_comma() -> char {
    ','
}

/// Default batch size for ODBC operations
pub fn default_batch_size() -> usize {
    1000
}

// =============================================================================
// Custom deserializers
// =============================================================================

/// Custom deserializer that handles string representations of DataType
pub fn deserialize_data_type<'de, D>(deserializer: D) -> Result<DataType, D::Error>
where
    D: Deserializer<'de>,
{
    use serde::de::Error;

    // Deserialize as a string
    let s = String::deserialize(deserializer)?;
    DataType::from_str(&s).map_err(|e| D::Error::custom(format!("Invalid data type: {}", e)))
}

/// Custom deserializer for partition columns that handles both tuple and object formats
pub fn deserialize_partition_columns<'de, D>(
    deserializer: D,
) -> Result<Vec<(String, DataType)>, D::Error>
where
    D: Deserializer<'de>,
{
    use serde::de::Error;

    #[derive(Deserialize)]
    #[serde(untagged)]
    enum PartitionColumn {
        Tuple(String, String),                      // (name, type_string)
        Object { name: String, data_type: String }, // {name: "col", data_type: "int32"}
    }

    let columns: Vec<PartitionColumn> = Vec::deserialize(deserializer)?;

    columns
        .into_iter()
        .map(|col| match col {
            PartitionColumn::Tuple(name, type_str) => {
                let data_type = DataType::from_str(&type_str).map_err(|e| {
                    D::Error::custom(format!("Invalid data type in partition column: {}", e))
                })?;
                Ok((name, data_type))
            }
            PartitionColumn::Object {
                name,
                data_type: type_str,
            } => {
                let data_type = DataType::from_str(&type_str).map_err(|e| {
                    D::Error::custom(format!("Invalid data type in partition column: {}", e))
                })?;
                Ok((name, data_type))
            }
        })
        .collect()
}
