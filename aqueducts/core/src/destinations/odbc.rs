use datafusion::dataframe::DataFrame;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use super::Result;

/// A file output destination
#[derive(Debug, Clone, Serialize, Deserialize, derive_new::new)]
#[cfg_attr(feature = "schema_gen", derive(schemars::JsonSchema))]
pub struct OdbcDestination {
    /// Name of the ODBC destination
    pub name: String,

    /// Query to execute when writing data to the ODBC connection
    pub query: String,

    /// ODBC connection string
    /// Please reference the respective database connection string syntax (e.g. <https://www.connectionstrings.com/postgresql-odbc-driver-psqlodbc/>)
    pub connection_string: String,

    /// batch size (rows) to use when inserting data
    pub batch_size: usize,
}

pub(super) async fn write(odbc_def: &OdbcDestination, data: DataFrame) -> Result<()> {
    let schema = data.schema().as_arrow().clone();
    let batches = data.collect().await?;

    aqueducts_odbc::write_arrow_batches(
        odbc_def.connection_string.as_str(),
        odbc_def.name.as_str(),
        batches,
        Arc::new(schema),
        odbc_def.batch_size,
    )
    .await?;

    Ok(())
}
