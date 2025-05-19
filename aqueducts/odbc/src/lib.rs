use std::sync::Arc;

use arrow_odbc::odbc_api::{ConnectionOptions, Environment};
use arrow_odbc::{insert_into_table, OdbcReaderBuilder, OdbcWriter};
use datafusion::arrow::array::RecordBatchIterator;
use datafusion::arrow::compute::concat_batches;
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::{array::RecordBatch, error::ArrowError};
use datafusion::datasource::MemTable;
use datafusion::execution::context::SessionContext;
use tracing::error;

pub mod error;

pub type Result<T> = core::result::Result<T, error::Error>;

/// Register a table via ODBC using [arrow-odbc](https://docs.rs/arrow-odbc)
/// ```rust,ignore
/// use datafusion::prelude::SessionContext;
/// use arrow_odbc::odbc_api::ConnectionOptions;
///
/// let connection_string: &str = "\
///     Driver={PostgreSQL Unicode};\
///     Server=localhost;\
///     UID=postgres;\
///     PWD=postgres;\
/// ";
///
/// // query to request data from the ODBC source
/// // make sure to constrain this query to a dataset that is manageble in memory
/// let query = "SELECT * FROM my_table WHERE date > '2024-01-01'";
///
/// let ctx = SessionContext::new();
///
/// register_odbc_source(&ctx, query, connection_string, "my_table_name").await.unwrap();
///
/// let df = ctx.sql("SELECT * FROM my_table_name").await.unwrap();
/// df.show().await.unwrap();
/// ```
pub async fn register_odbc_source(
    ctx: Arc<SessionContext>,
    connection_string: &str,
    query: &str,
    source_name: &str,
) -> Result<()> {
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

/// Checks if the provided table for the destination exists
/// will try to query one record from the provided table name
pub async fn register_odbc_destination(
    connection_string: &str,
    destination_name: &str,
) -> Result<()> {
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
/// ```rust,ignore
/// use datafusion::prelude::SessionContext;
/// use arrow_odbc::odbc_api::ConnectionOptions;
/// use std::sync::Arc;
///
/// let connection_string: &str = "\
///     Driver={PostgreSQL Unicode};\
///     Server=localhost;\
///     UID=postgres;\
///     PWD=postgres;\
/// ";
///
/// let query = "SELECT * FROM my_table WHERE date > '2024-01-01'";
/// let ctx = SessionContext::new();
/// register_odbc_source(&ctx, query, connection_string, "my_table_name").await.unwrap();
///
/// //check if table exists
/// register_odbc_destination(connection_string, "another_table").await.unwrap();
///
/// let df = ctx.sql("SELECT * FROM my_table_name").await.unwrap();
/// let schema = df.schema().as_arrow().clone();
/// let batches = df.collect().await.unwrap();
///
/// write_arrow_batches(connection_string, "another_table", batches, Arc::new(schema), 1000).await.unwrap();
/// ```
pub async fn write_arrow_batches(
    connection_string: &str,
    destination_name: &str,
    batches: Vec<RecordBatch>,
    schema: Arc<Schema>,
    batch_size: usize,
) -> Result<()> {
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

/// Write a DataFrame to an ODBC table
/// ```rust,ignore
/// use datafusion::prelude::*;
///
/// let ctx = SessionContext::new();
/// let df = ctx.sql("SELECT * FROM my_source").await.unwrap();
///
/// write_to_table(
///    "Driver={PostgreSQL};Server=localhost;UID=user;PWD=password",
///    "destination_table",
///    df
/// ).await.unwrap();
/// ```
pub async fn write_to_table(
    connection_string: &str,
    destination_name: &str,
    df: datafusion::dataframe::DataFrame,
) -> Result<()> {
    let schema = df.schema().as_arrow().clone();
    let batches = df.collect().await?;

    write_arrow_batches(
        connection_string,
        destination_name,
        batches,
        Arc::new(schema),
        1000, // default batch size
    )
    .await
}

/// Performs an insert with a prepared statement provided.
/// Optionally, it can execute preliminary statements (such as `delete from ...`).
/// All statemets are executed within the same transaction and it gets rolled back
/// in case of any errors.
pub async fn custom(
    connection_string: &str,
    pre_insert: Option<String>,
    insert: &str,
    batches: Vec<RecordBatch>,
    schema: Arc<Schema>,
    batch_size: usize,
) -> Result<()> {
    let odbc_environment = Environment::new()?;

    let connection = odbc_environment
        .connect_with_connection_string(connection_string, ConnectionOptions::default())?;

    let batches = [concat_batches(&schema, batches.iter())?];
    let record_batch_iterator =
        RecordBatchIterator::new(batches.into_iter().map(Ok), schema.clone());

    let mut writer = OdbcWriter::new(batch_size, &schema, connection.prepare(insert)?)?;

    let _ = connection.set_autocommit(false);

    let result = || -> Result<()> {
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

#[cfg(all(test, feature = "odbc_tests"))]
mod tests {
    use datafusion::arrow::array::*;
    use datafusion::{assert_batches_eq, prelude::*};
    use std::sync::Arc;

    use super::*;

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_register_odbc_source_ok() {
        let connection_string: &str = "\
            Driver={PostgreSQL Unicode};\
            Server=localhost;\
            UID=postgres;\
            PWD=postgres;\
        ";

        let ctx = Arc::new(SessionContext::new());

        register_odbc_source(
            ctx.clone(),
            connection_string,
            "SELECT * FROM temp_readings WHERE timestamp::date BETWEEN '2024-01-01' AND '2024-01-31'",
            "my_table",
        )
        .await
        .unwrap();

        let result = ctx
            .sql("SELECT count(*) num_rows FROM my_table")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        assert_batches_eq!(
            &[
                "+----------+",
                "| num_rows |",
                "+----------+",
                "| 1000     |",
                "+----------+",
            ],
            result.as_slice()
        );
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_register_odbc_destination_ok() {
        let connection_string: &str = "\
            Driver={PostgreSQL Unicode};\
            Server=localhost;\
            UID=postgres;\
            PWD=postgres;\
        ";

        let result = register_odbc_destination(connection_string, "temp_readings_empty").await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_write_arrow_batches_ok() {
        let connection_string: &str = "\
            Driver={PostgreSQL Unicode};\
            Server=localhost;\
            UID=postgres;\
            PWD=postgres;\
        ";

        let locations = (0..1000).collect::<Vec<i32>>();
        let timestamps = (1704067200..1704068200).collect::<Vec<i64>>();
        let temperatures = (0..1000).map(|i| i as f64).collect::<Vec<f64>>();
        let humidity = (0..1000).map(|i| i as f64).collect::<Vec<f64>>();
        let conditions = (0..1000)
            .map(|i| format!("CONDITION_{i}"))
            .collect::<Vec<String>>();

        let a: ArrayRef = Arc::new(Int32Array::from(locations));
        let b: ArrayRef = Arc::new(TimestampSecondArray::from(timestamps));
        let c: ArrayRef = Arc::new(Float64Array::from(temperatures));
        let d: ArrayRef = Arc::new(Float64Array::from(humidity));
        let e: ArrayRef = Arc::new(StringArray::from(conditions));

        let record_batch = RecordBatch::try_from_iter(vec![
            ("location_id", a),
            ("timestamp", b),
            ("temperature_c", c),
            ("humidity", d),
            ("weather_condition", e),
        ])
        .unwrap();
        let schema = record_batch.schema();

        let result = write_arrow_batches(
            connection_string,
            "temp_readings_empty",
            vec![record_batch],
            schema,
            100,
        )
        .await;

        assert!(result.is_ok());
    }

    /// Tests a trasaction with a delete and an insert
    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_custom_delete_insert_ok() {
        let odbc_environment = Environment::new().unwrap();
        let connection_string: &str = "\
            Driver={PostgreSQL Unicode};\
            Server=localhost;\
            UID=postgres;\
            PWD=postgres;\
        ";
        let connection = odbc_environment
            .connect_with_connection_string(connection_string, ConnectionOptions::default())
            .unwrap();
        let _ = connection
            .execute("truncate test_custom_delete_insert_ok", (), None)
            .unwrap();

        let record_batch = RecordBatch::try_from_iter(vec![
            ("id", Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef),
            (
                "value",
                Arc::new(StringArray::from(vec!["original", "original"])) as ArrayRef,
            ),
        ])
        .unwrap();
        let schema = record_batch.schema();

        let _ = write_arrow_batches(
            connection_string,
            "test_custom_delete_insert_ok",
            vec![record_batch],
            schema.clone(),
            100,
        )
        .await;

        let new_batch = RecordBatch::try_from_iter(vec![
            ("id", Arc::new(Int32Array::from(vec![1])) as ArrayRef),
            (
                "value",
                Arc::new(StringArray::from(vec!["updated"])) as ArrayRef,
            ),
        ])
        .unwrap();

        custom(
            connection_string,
            Some("delete from test_custom_delete_insert_ok where id = 1".to_string()),
            "insert into test_custom_delete_insert_ok values (?, ?)",
            vec![new_batch],
            schema,
            50,
        )
        .await
        .unwrap();

        let cursor = connection
            .execute(
                "select * from test_custom_delete_insert_ok order by id",
                (),
                None,
            )
            .unwrap()
            .unwrap();
        let result = OdbcReaderBuilder::new().build(cursor).unwrap();
        for batch in result {
            assert_batches_eq!(
                [
                    "+----+----------+",
                    "| id | value    |",
                    "+----+----------+",
                    "| 1  | updated  |",
                    "| 2  | original |",
                    "+----+----------+",
                ],
                &[batch.unwrap()]
            );
        }
    }

    /// Checks transaction is rolled back in case of error
    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_custom_delete_insert_failed() {
        let odbc_environment = Environment::new().unwrap();
        let connection_string: &str = "\
            Driver={PostgreSQL Unicode};\
            Server=localhost;\
            UID=postgres;\
            PWD=postgres;\
        ";
        let connection = odbc_environment
            .connect_with_connection_string(connection_string, ConnectionOptions::default())
            .unwrap();
        let _ = connection
            .execute("truncate test_custom_delete_insert_failed", (), None)
            .unwrap();

        let record_batch = RecordBatch::try_from_iter(vec![
            ("id", Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef),
            (
                "value",
                Arc::new(StringArray::from(vec!["original", "original"])) as ArrayRef,
            ),
        ])
        .unwrap();
        let schema = record_batch.schema();

        let _ = write_arrow_batches(
            connection_string,
            "test_custom_delete_insert_failed",
            vec![record_batch],
            schema.clone(),
            100,
        )
        .await;

        let new_batch = RecordBatch::try_from_iter(vec![
            ("id", Arc::new(Int32Array::from(vec![1])) as ArrayRef),
            (
                "value",
                Arc::new(StringArray::from(vec!["updated"])) as ArrayRef,
            ),
        ])
        .unwrap();

        custom(
            connection_string,
            Some("delete from test_custom_delete_insert_failed where id = 1".to_string()),
            "insert into WRONG_TABLE values (?, ?)",
            vec![new_batch],
            schema,
            50,
        )
        .await
        .ok();

        let cursor = connection
            .execute(
                "select * from test_custom_delete_insert_failed order by id",
                (),
                None,
            )
            .unwrap()
            .unwrap();
        let result = OdbcReaderBuilder::new().build(cursor).unwrap();
        for batch in result {
            assert_batches_eq!(
                [
                    "+----+----------+",
                    "| id | value    |",
                    "+----+----------+",
                    "| 1  | original |",
                    "| 2  | original |",
                    "+----+----------+",
                ],
                &[batch.unwrap()]
            );
        }
    }
}
