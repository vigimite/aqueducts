//! ODBC source implementation.

use std::sync::Arc;

use arrow_odbc::odbc_api::{ConnectionOptions, Environment};
use arrow_odbc::OdbcReaderBuilder;
use datafusion::arrow::{array::RecordBatch, error::ArrowError};
use datafusion::datasource::MemTable;
use datafusion::execution::context::SessionContext;

use crate::error;

/// Register a table via ODBC using [arrow-odbc](https://docs.rs/arrow-odbc)
pub async fn register_odbc_source(
    ctx: Arc<SessionContext>,
    connection_string: &str,
    query: &str,
    source_name: &str,
) -> error::Result<()> {
    let odbc_environment = Environment::new().unwrap();

    let connection = odbc_environment
        .connect_with_connection_string(connection_string, ConnectionOptions::default())?;

    let parameters = ();

    let cursor = connection
        .execute(query, parameters, None)?
        .expect("SELECT statement must produce a cursor");

    let reader = OdbcReaderBuilder::new().build(cursor)?;

    let batches = reader
        .into_iter()
        .collect::<std::result::Result<Vec<RecordBatch>, ArrowError>>()?;

    let df = ctx.read_batches(batches)?;

    let schema = df.schema().clone();
    let partitioned = df.collect_partitioned().await?;
    let table = MemTable::try_new(Arc::new(schema.as_arrow().clone()), partitioned)?;

    ctx.register_table(source_name, Arc::new(table))?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use aqueducts_schemas::OdbcSource;

    #[test]
    fn test_odbc_source_config_serialization() {
        let config = OdbcSource {
            name: "test_source".to_string(),
            query: "SELECT * FROM test_table".to_string(),
            connection_string: "Driver={Test};Server=localhost".to_string(),
        };

        let serialized = serde_json::to_string(&config).unwrap();
        let deserialized: OdbcSource = serde_json::from_str(&serialized).unwrap();

        assert_eq!(config.name, deserialized.name);
        assert_eq!(config.query, deserialized.query);
        assert_eq!(config.connection_string, deserialized.connection_string);
    }
}
