//! ODBC destination implementation.

use std::sync::Arc;

use arrow_odbc::odbc_api::{ConnectionOptions, Environment};
use arrow_odbc::{insert_into_table, OdbcWriter};
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::array::RecordBatchIterator;
use datafusion::arrow::compute::concat_batches;
use datafusion::arrow::datatypes::Schema;
use tracing::error;

use crate::error;

/// Checks if the provided table for the destination exists
/// will try to query one record from the provided table name
pub async fn register_odbc_destination(
    connection_string: &str,
    destination_name: &str,
) -> error::Result<()> {
    let odbc_environment = Environment::new().unwrap();

    let connection = odbc_environment
        .connect_with_connection_string(connection_string, ConnectionOptions::default())?;

    let parameters = ();

    let query = format!("SELECT * FROM {destination_name} LIMIT 1");
    connection
        .execute(query.as_str(), parameters, None)?
        .expect("SELECT statement must produce a cursor");

    Ok(())
}

/// Write arrow batches to a table via ODBC using [arrow-odbc](https://docs.rs/arrow-odbc)
pub async fn write_arrow_batches(
    connection_string: &str,
    destination_name: &str,
    batches: Vec<RecordBatch>,
    schema: Arc<Schema>,
    batch_size: usize,
) -> error::Result<()> {
    let odbc_environment = Environment::new().unwrap();

    let connection = odbc_environment
        .connect_with_connection_string(connection_string, ConnectionOptions::default())?;

    let batches = [concat_batches(&schema, batches.iter())?];
    let mut record_batch_iterator = RecordBatchIterator::new(batches.into_iter().map(Ok), schema);

    insert_into_table(
        &connection,
        &mut record_batch_iterator,
        destination_name,
        batch_size,
    )?;

    Ok(())
}

/// Performs an insert with a prepared statement provided.
/// Optionally, it can execute preliminary statements (such as `delete from ...`).
/// All statements are executed within the same transaction and it gets rolled back
/// in case of any errors.
pub async fn custom(
    connection_string: &str,
    pre_insert: Option<String>,
    insert: &str,
    batches: Vec<RecordBatch>,
    schema: Arc<Schema>,
    batch_size: usize,
) -> error::Result<()> {
    let odbc_environment = Environment::new()?;

    let connection = odbc_environment
        .connect_with_connection_string(connection_string, ConnectionOptions::default())?;

    let batches = [concat_batches(&schema, batches.iter())?];
    let record_batch_iterator =
        RecordBatchIterator::new(batches.into_iter().map(Ok), schema.clone());

    let mut writer = OdbcWriter::new(batch_size, &schema, connection.prepare(insert)?)?;

    let _ = connection.set_autocommit(false);

    let result = || -> error::Result<()> {
        if let Some(stmt) = pre_insert {
            connection.execute(&stmt, (), None)?;
        }
        writer.write_all(record_batch_iterator)?;

        Ok(())
    };

    match result() {
        Ok(_) => {
            connection.commit()?;
            Ok(())
        }
        Err(err) => {
            connection.rollback()?;
            error!("ROLLBACK transaction: {err:?}");
            Err(err)
        }
    }
}

#[cfg(test)]
mod tests {
    use aqueducts_schemas::OdbcDestination;

    #[test]
    fn test_odbc_destination_config_serialization() {
        let config = OdbcDestination {
            name: "test_destination".to_string(),
            connection_string: "Driver={Test};Server=localhost".to_string(),
            query: "INSERT INTO test_table VALUES (?, ?)".to_string(),
            batch_size: 500,
        };

        let serialized = serde_json::to_string(&config).unwrap();
        let deserialized: OdbcDestination = serde_json::from_str(&serialized).unwrap();

        assert_eq!(config.name, deserialized.name);
        assert_eq!(config.connection_string, deserialized.connection_string);
        assert_eq!(config.query, deserialized.query);
        assert_eq!(config.batch_size, deserialized.batch_size);
    }
}
