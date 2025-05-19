//! File format handling for Aqueducts
//!
//! This crate provides utilities for handling various file formats in the Aqueducts framework.
//! It includes support for:
//!
//! - Parquet files
//! - CSV files
//! - JSON files
//! - Avro files (when the 'avro' feature is enabled)
//!
//! Each format is conditionally compiled based on feature flags, allowing for flexible
//! dependency management and minimal build size.

use datafusion::dataframe::{DataFrame, DataFrameWriteOptions};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use url::Url;

mod error;
pub use error::Error;
pub type Result<T> = std::result::Result<T, Error>;

/// Prelude module with commonly used types and functions
pub mod prelude;

#[cfg(feature = "parquet")]
pub mod parquet;

#[cfg(feature = "csv")]
pub mod csv;

#[cfg(feature = "json")]
pub mod json;

/// File type and options
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "schema_gen", derive(schemars::JsonSchema))]
#[serde(tag = "type", content = "options")]
pub enum FileType {
    /// Parquet options map, please refer to <https://docs.rs/datafusion-common/latest/datafusion_common/config/struct.TableParquetOptions.html> for possible options
    #[cfg(feature = "parquet")]
    Parquet(#[serde(default)] HashMap<String, String>),

    /// CSV options
    #[cfg(feature = "csv")]
    Csv(csv::CsvOptions),

    /// JSON destination
    #[cfg(feature = "json")]
    Json,
}

/// Write dataframe to a file with the specified file type
pub async fn write_file(
    location: &Url,
    data: DataFrame,
    file_type: &FileType,
    single_file: bool,
    partition_cols: Vec<String>,
) -> Result<()> {
    let write_options = DataFrameWriteOptions::default()
        .with_partition_by(partition_cols)
        .with_single_file_output(single_file);

    match file_type {
        #[cfg(feature = "parquet")]
        FileType::Parquet(options) => {
            parquet::write_parquet(location, data, write_options, options).await?;
        }
        #[cfg(feature = "csv")]
        FileType::Csv(options) => {
            csv::write_csv(location, data, write_options, options).await?;
        }
        #[cfg(feature = "json")]
        FileType::Json => {
            json::write_json(location, data, write_options).await?;
        }
    }

    Ok(())
}
