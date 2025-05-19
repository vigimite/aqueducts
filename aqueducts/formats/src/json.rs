use datafusion::dataframe::{DataFrame, DataFrameWriteOptions};
use url::Url;

use crate::Result;

/// Write a dataframe to JSON
pub async fn write_json(
    location: &Url,
    data: DataFrame,
    write_options: DataFrameWriteOptions,
) -> Result<()> {
    data.write_json(location.as_str(), write_options, None)
        .await?;

    Ok(())
}
