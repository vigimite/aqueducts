use std::sync::Arc;

use arrow_odbc::odbc_api::{ConnectionOptions, Environment};
use arrow_odbc::{insert_into_table, OdbcReaderBuilder};
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::{array::RecordBatch, error::ArrowError};
use datafusion::execution::context::SessionContext;
use deltalake::arrow::array::RecordBatchIterator;

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
        .execute(query, parameters)?
        .expect("SELECT statement must produce a cursor");

    let reader = OdbcReaderBuilder::new().build(cursor)?;

    let batches = reader
        .into_iter()
        .collect::<std::result::Result<Vec<RecordBatch>, ArrowError>>()?;

    let df = ctx.read_batches(batches)?;
    let schema: Arc<Schema> = Arc::new(df.schema().into());
    let batch = deltalake::arrow::compute::concat_batches(&schema, df.collect().await?.iter())?;

    ctx.register_batch(source_name, batch)?;

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
        .execute(query.as_str(), parameters)?
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

    let mut record_batch_iterator = RecordBatchIterator::new(batches.into_iter().map(Ok), schema);

    insert_into_table(
        &connection,
        &mut record_batch_iterator,
        destination_name,
        batch_size,
    )?;

    Ok(())
}

#[cfg(test)]
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
}
