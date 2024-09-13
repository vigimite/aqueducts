use super::Result;
use datafusion::dataframe::DataFrame;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// An ODBC output destination
#[derive(Debug, Clone, Serialize, Deserialize, derive_new::new)]
#[cfg_attr(feature = "schema_gen", derive(schemars::JsonSchema))]
pub struct OdbcDestination {
    /// Name of the ODBC destination
    pub name: String,

    /// ODBC connection string
    /// Please reference the respective database connection string syntax (e.g. <https://www.connectionstrings.com/postgresql-odbc-driver-psqlodbc/>)
    pub connection_string: String,

    /// Strategy for performing ODBC write operation
    pub write_mode: WriteMode,

    /// batch size (rows) to use when inserting data
    pub batch_size: usize,
}

/// Write modes for the `Destination` output.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "schema_gen", derive(schemars::JsonSchema))]
#[serde(tag = "operation", content = "transaction")]
pub enum WriteMode {
    /// `Append`: appends data to the `Destination`
    Append,

    /// `Custom`: Inserts data with a prepared stament. Option to perform any number of (non-insert) preliminary statements
    Custom(CustomStatements),
}

/// SQL statements for `Custom` write mode.
#[derive(Debug, Clone, Serialize, Deserialize, Default, derive_new::new)]
#[cfg_attr(feature = "schema_gen", derive(schemars::JsonSchema))]
pub struct CustomStatements {
    /// Optional (non-insert) preliminary statement
    pre_insert: Option<String>,

    /// Insert prepared statement
    insert: String,
}

pub(super) async fn write(odbc_def: &OdbcDestination, data: DataFrame) -> Result<()> {
    let schema = data.schema().as_arrow().clone();
    let batches = data.collect().await?;

    match &odbc_def.write_mode {
        WriteMode::Append => {
            aqueducts_odbc::write_arrow_batches(
                odbc_def.connection_string.as_str(),
                odbc_def.name.as_str(),
                batches,
                Arc::new(schema),
                odbc_def.batch_size,
            )
            .await?
        }
        WriteMode::Custom(custom_statements) => {
            aqueducts_odbc::custom(
                odbc_def.connection_string.as_str(),
                custom_statements.pre_insert.clone(),
                custom_statements.insert.as_str(),
                batches,
                Arc::new(schema),
                odbc_def.batch_size,
            )
            .await?
        }
    }

    Ok(())
}
