use datafusion::{
    config::{ConfigField, TableParquetOptions},
    dataframe::{DataFrame, DataFrameWriteOptions},
};
use std::collections::HashMap;
use url::Url;

use crate::{Error, Result};

/// Write a dataframe to Parquet
pub async fn write_parquet(
    location: &Url,
    data: DataFrame,
    write_options: DataFrameWriteOptions,
    options: &HashMap<String, String>,
) -> Result<()> {
    let mut parquet_options = TableParquetOptions::default();

    for (k, v) in options {
        parquet_options
            .set(k.as_str(), v.as_str())
            .map_err(|_| Error::UnknownOption(k.clone()))?;
    }

    data.write_parquet(location.as_str(), write_options, Some(parquet_options))
        .await?;

    Ok(())
}
