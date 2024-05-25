/// custom serde
pub mod serde {
    use serde::{Deserialize, Deserializer};
    use std::path::Path;
    use url::{ParseError, Url};

    /// try to deserialize URL
    /// if URL deserialization fails due to it being a relative path this function will fallback to using the `std::path` API to create a canonical representation of the given path and then parse as a URL
    pub fn deserialize_file_location<'de, D>(deserializer: D) -> core::result::Result<Url, D::Error>
    where
        D: Deserializer<'de>,
    {
        let buf = String::deserialize(deserializer)?;

        let url = match Url::parse(buf.as_str()) {
            Err(ParseError::RelativeUrlWithoutBase)
            | Err(ParseError::RelativeUrlWithCannotBeABaseBase)
                if buf.ends_with('/') || buf.ends_with('\\') =>
            {
                let canonicalized = Path::new(buf.as_str()).canonicalize().map_err(|e| {
                    serde::de::Error::custom(format!("error canonicalizing dir path: {e:?}"))
                })?;

                let url = Url::from_directory_path(canonicalized).map_err(|e| {
                    serde::de::Error::custom(format!("error parsing directory path as url: {e:?}"))
                })?;

                Ok(url)
            }
            Err(ParseError::RelativeUrlWithoutBase)
            | Err(ParseError::RelativeUrlWithCannotBeABaseBase) => {
                let (path, file_name) = buf
                    .contains('/')
                    .then_some('/')
                    .or(buf.contains('\\').then_some('\\'))
                    .and_then(|split_char| buf.as_str().rsplit_once(split_char))
                    .ok_or_else(|| {
                        serde::de::Error::custom(
                            "relative paths cannot only contain the file name".to_string(),
                        )
                    })?;

                // file might not exist in the output case
                let canonicalized = {
                    let mut path = Path::new(path).canonicalize().map_err(|e| {
                        serde::de::Error::custom(format!(
                            "error canonicalizing file path '{buf}': {e:?}"
                        ))
                    })?;

                    path.push(file_name);
                    path
                };

                let url = Url::from_file_path(canonicalized).map_err(|e| {
                    serde::de::Error::custom(format!(
                        "error parsing file path as url '{buf}': {e:?}"
                    ))
                })?;

                Ok(url)
            }
            Err(err) => Err(serde::de::Error::custom(format!(
                "error parsing location: {err:?}"
            ))),
            Ok(url) => Ok(url),
        }?;

        Ok(url)
    }
}

/// object store handlers
pub mod store {
    use deltalake::{
        datafusion::prelude::SessionContext, storage::StorageOptions, DeltaTableError,
    };
    use std::{collections::HashMap, sync::Arc};
    use url::Url;

    /// register deltalake object store handlers
    #[allow(dead_code)]
    pub fn register_handlers() {
        #[cfg(any(feature = "s3", rust_analyzer))]
        {
            deltalake_aws::register_handlers(None);
        }

        #[cfg(any(feature = "gcs", rust_analyzer))]
        {
            deltalake_gcp::register_handlers(None);
        }

        #[cfg(any(feature = "azure", rust_analyzer))]
        {
            deltalake_azure::register_handlers(None);
        }
    }

    pub fn register_object_store(
        ctx: &SessionContext,
        location: &Url,
        storage_options: &HashMap<String, String>,
    ) -> Result<(), DeltaTableError> {
        if location.scheme() == "file" || location.scheme() == "memory" {
            return Ok(());
        }

        let scheme = Url::parse(&format!("{}://", location.scheme())).unwrap();
        if let Some(factory) = deltalake::storage::factories().get(&scheme) {
            let (store, _prefix) =
                factory.parse_url_opts(location, &StorageOptions(storage_options.clone()))?;
            let _ = ctx
                .runtime_env()
                .register_object_store(location, Arc::new(store));

            Ok(())
        } else {
            Err(DeltaTableError::InvalidTableLocation(
                location.clone().into(),
            ))
        }
    }
}

/// odbc functionality
#[cfg(any(feature = "odbc", rust_analyzer))]
pub mod odbc {
    use std::sync::Arc;

    use arrow_odbc::odbc_api::{ConnectionOptions, Environment};
    use arrow_odbc::OdbcReaderBuilder;
    use datafusion::arrow::datatypes::Schema;
    use datafusion::arrow::{array::RecordBatch, error::ArrowError};
    use datafusion::execution::context::SessionContext;

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
        ctx: &SessionContext,
        connection_string: &str,
        query: &str,
        source_name: &str,
    ) -> Result<(), ArrowError> {
        let odbc_environment = Environment::new().unwrap();

        let connection = odbc_environment
            .connect_with_connection_string(connection_string, ConnectionOptions::default())
            .expect("failed to connect to ODBC source");

        let parameters = ();

        let cursor = connection
            .execute(query, parameters)
            .expect("failed to execute SQL statement statement")
            .expect("SELECT statement must produce a cursor");

        let reader = OdbcReaderBuilder::new()
            .with_max_bytes_per_batch(256 * 1024 * 1024)
            .build(cursor)
            .expect("failed to build OdbcReaderBuilder");

        let batches = reader
            .into_iter()
            .collect::<Result<Vec<RecordBatch>, ArrowError>>()?;

        let df = ctx.read_batches(batches)?;
        let schema: Arc<Schema> = Arc::new(df.schema().into());
        let batch = deltalake::arrow::compute::concat_batches(&schema, df.collect().await?.iter())?;

        ctx.register_batch(source_name, batch)?;

        Ok(())
    }
}

#[cfg(all(test, feature = "odbc"))]
mod odbc_tests {
    use super::odbc::*;
    use datafusion::{assert_batches_eq, prelude::*};

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_register_odbc_source_ok() {
        let connection_string: &str = "\
            Driver={PostgreSQL Unicode};\
            Server=localhost;\
            UID=postgres;\
            PWD=postgres;\
        ";

        let ctx = SessionContext::new();

        register_odbc_source(
            &ctx,
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
}
