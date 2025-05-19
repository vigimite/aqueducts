use datafusion::{
    config::CsvOptions as DataFusionCsvOptions,
    dataframe::{DataFrame, DataFrameWriteOptions},
};
use serde::{Deserialize, Serialize};
use url::Url;

use crate::Result;

/// CSV format options
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[cfg_attr(feature = "schema_gen", derive(schemars::JsonSchema))]
pub struct CsvOptions {
    /// Defaults to true, sets a header for the CSV file
    pub has_header: Option<bool>,

    /// Defaults to `,`, sets the delimiter char for the CSV file
    pub delimiter: Option<char>,
}

impl CsvOptions {
    /// Create new CSV options
    pub fn new(has_header: Option<bool>, delimiter: Option<char>) -> Self {
        Self {
            has_header,
            delimiter,
        }
    }

    /// Convert to DataFusion CSV options
    pub fn to_datafusion_options(&self) -> DataFusionCsvOptions {
        DataFusionCsvOptions::default()
            .with_has_header(self.has_header.unwrap_or(true))
            .with_delimiter(self.delimiter.unwrap_or(',') as u8)
    }
}

/// Write a dataframe to CSV
pub async fn write_csv(
    location: &Url,
    data: DataFrame,
    write_options: DataFrameWriteOptions,
    csv_options: &CsvOptions,
) -> Result<()> {
    let options = csv_options.to_datafusion_options();

    data.write_csv(location.as_str(), write_options, Some(options))
        .await?;

    Ok(())
}
