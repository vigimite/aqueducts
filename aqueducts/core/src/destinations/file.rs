use aqueducts_utils::serde::deserialize_file_location;
use datafusion::config::{ConfigField, CsvOptions, TableParquetOptions};
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use url::Url;

use super::Result;

#[derive(Debug, Clone, Serialize, Deserialize, derive_new::new)]
pub struct FileDestination {
    ///  Name of the file to write
    pub name: String,

    /// Location of the file as a URL e.g. file:///tmp/output.csv, s3://bucket_name/prefix/output.parquet, s3:://bucket_name/prefix
    #[serde(deserialize_with = "deserialize_file_location")]
    pub location: Url,

    /// File type, supported types are Parquet and CSV
    pub file_type: FileType,

    /// Describes whether to write a single file (can be used to overwrite destination file)
    #[serde(default)]
    pub single_file: bool,

    /// Columns to partition table by
    #[serde(default)]
    pub partition_cols: Vec<String>,

    /// Object store storage options
    #[serde(default)]
    pub storage_options: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "options")]
pub enum FileType {
    Parquet(ParquetDestinationOptions),
    Csv(CsvDestinationOptions),
}

/// Parquet options, please refer to `https://docs.rs/datafusion-common/latest/datafusion_common/config/struct.TableParquetOptions.html` for possible options
#[derive(Debug, Clone, Serialize, Deserialize, Default, derive_new::new)]
pub struct ParquetDestinationOptions {
    #[serde(default)]
    parquet_options: HashMap<String, String>,
}

/// Csv options
#[derive(Debug, Clone, Serialize, Deserialize, Default, derive_new::new)]
pub struct CsvDestinationOptions {
    has_header: Option<bool>,
    delimiter: Option<char>,
}

pub(super) async fn write(file_def: &FileDestination, data: DataFrame) -> Result<()> {
    let write_options = DataFrameWriteOptions::default()
        .with_partition_by(file_def.partition_cols.clone())
        .with_single_file_output(file_def.single_file);

    let _ = match &file_def.file_type {
        FileType::Parquet(config) => {
            let mut parquet_options = TableParquetOptions::default();

            config
                .parquet_options
                .iter()
                .try_for_each(|(k, v)| parquet_options.set(k.as_str(), v.as_str()))?;

            data.write_parquet(
                file_def.location.as_str(),
                write_options,
                Some(parquet_options),
            )
            .await?
        }
        FileType::Csv(csv_options) => {
            let csv_options = CsvOptions::default()
                .with_has_header(csv_options.has_header.unwrap_or(true))
                .with_delimiter(csv_options.delimiter.unwrap_or(',') as u8);

            data.write_csv(file_def.location.as_str(), write_options, Some(csv_options))
                .await?
        }
    };

    Ok(())
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::RecordBatch;
    use datafusion::arrow::array::{ArrayRef, BooleanArray, Int32Array, StringArray};
    use datafusion::assert_batches_eq;
    use datafusion::datasource::file_format::parquet::ParquetFormat;
    use datafusion::datasource::listing::{
        ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
    };
    use deltalake::arrow::datatypes::DataType;
    use std::{path::Path, sync::Arc};

    use super::*;

    fn generate_test_file_path(file_name: &str) -> Url {
        let local_path = Path::new(".")
            .canonicalize()
            .unwrap()
            .into_os_string()
            .into_string()
            .unwrap();

        let file_path = format!("file://{local_path}/tests/output/test_file/{file_name}");

        Url::parse(file_path.as_str()).unwrap()
    }

    #[tokio::test]
    async fn test_write_csv_ok() {
        let ctx = SessionContext::new();

        let path = generate_test_file_path("csv/write.csv");
        let definition = FileDestination::new(
            "write".into(),
            path.clone(),
            FileType::Csv(CsvDestinationOptions::new(Some(true), None)),
            true,
            vec![],
            Default::default(),
        );

        // Insert records into table
        let col_1 = Arc::new(StringArray::from(vec!["a", "b", "c", "d"])) as ArrayRef;
        let col_2 = Arc::new(Int32Array::from(vec![1, 2, 3, 4])) as ArrayRef;
        let col_3 = Arc::new(BooleanArray::from(vec![true, true, false, false])) as ArrayRef;
        let col_4 = Arc::new(Int32Array::from(vec![10, 20, 30, 40])) as ArrayRef;

        let batch = RecordBatch::try_from_iter(vec![
            ("col_1", col_1),
            ("col_2", col_2),
            ("col_3", col_3),
            ("col_4", col_4),
        ])
        .unwrap();
        let df = ctx.read_batch(batch).unwrap();
        let _ = write(&definition, df).await.unwrap();

        let df = ctx
            .read_csv(path.as_str(), CsvReadOptions::default())
            .await
            .unwrap()
            .select_columns(&["col_1", "col_2", "col_3", "col_4"])
            .unwrap()
            .sort(vec![col("col_1").sort(true, false)])
            .unwrap();

        let batches = df.collect().await.unwrap();

        assert_batches_eq!(
            [
                "+-------+-------+-------+-------+",
                "| col_1 | col_2 | col_3 | col_4 |",
                "+-------+-------+-------+-------+",
                "| a     | 1     | true  | 10    |",
                "| b     | 2     | true  | 20    |",
                "| c     | 3     | false | 30    |",
                "| d     | 4     | false | 40    |",
                "+-------+-------+-------+-------+",
            ],
            batches.as_slice()
        );
    }

    #[tokio::test]
    async fn test_write_parquet_ok() {
        let ctx = SessionContext::new();

        let path = generate_test_file_path("parquet/write.parquet");
        let definition = FileDestination::new(
            "write".into(),
            path.clone(),
            FileType::Parquet(ParquetDestinationOptions::default()),
            true,
            vec![],
            Default::default(),
        );

        // Insert records into table
        let col_1 = Arc::new(StringArray::from(vec!["a", "b", "c", "d"])) as ArrayRef;
        let col_2 = Arc::new(Int32Array::from(vec![1, 2, 3, 4])) as ArrayRef;
        let col_3 = Arc::new(BooleanArray::from(vec![true, true, false, false])) as ArrayRef;
        let col_4 = Arc::new(Int32Array::from(vec![10, 20, 30, 40])) as ArrayRef;

        let batch = RecordBatch::try_from_iter(vec![
            ("col_1", col_1),
            ("col_2", col_2),
            ("col_3", col_3),
            ("col_4", col_4),
        ])
        .unwrap();
        let df = ctx.read_batch(batch).unwrap();
        let _ = write(&definition, df).await.unwrap();

        let df = ctx
            .read_parquet(path.as_str(), ParquetReadOptions::default())
            .await
            .unwrap()
            .select_columns(&["col_1", "col_2", "col_3", "col_4"])
            .unwrap()
            .sort(vec![col("col_1").sort(true, false)])
            .unwrap();

        let batches = df.collect().await.unwrap();

        assert_batches_eq!(
            [
                "+-------+-------+-------+-------+",
                "| col_1 | col_2 | col_3 | col_4 |",
                "+-------+-------+-------+-------+",
                "| a     | 1     | true  | 10    |",
                "| b     | 2     | true  | 20    |",
                "| c     | 3     | false | 30    |",
                "| d     | 4     | false | 40    |",
                "+-------+-------+-------+-------+",
            ],
            batches.as_slice()
        );
    }

    #[tokio::test]
    async fn test_write_parquet_partitioned_ok() {
        // Setup
        let ctx = SessionContext::new();

        let suffix = format!("partitioned/{}/", rand::random::<usize>());
        let path = generate_test_file_path(suffix.as_str());
        let definition = FileDestination::new(
            "write".into(),
            path.clone(),
            FileType::Parquet(ParquetDestinationOptions::default()),
            false,
            vec!["year".into()],
            Default::default(),
        );

        // Insert records into table
        let year = Arc::new(StringArray::from(vec!["2023", "2023", "2024", "2024"])) as ArrayRef;
        let col_1 = Arc::new(StringArray::from(vec!["a", "b", "c", "d"])) as ArrayRef;
        let col_2 = Arc::new(Int32Array::from(vec![1, 2, 3, 4])) as ArrayRef;
        let col_3 = Arc::new(BooleanArray::from(vec![true, true, false, false])) as ArrayRef;
        let col_4 = Arc::new(Int32Array::from(vec![10, 20, 30, 40])) as ArrayRef;

        let batch = RecordBatch::try_from_iter(vec![
            ("year", year),
            ("col_1", col_1),
            ("col_2", col_2),
            ("col_3", col_3),
            ("col_4", col_4),
        ])
        .unwrap();
        let df = ctx.read_batch(batch).unwrap();

        // Test
        let _ = write(&definition, df).await.unwrap();

        // Assert
        let session_state = ctx.state();
        let listing_table_url = ListingTableUrl::parse(path).unwrap();
        let listing_options = ListingOptions::new(Arc::new(ParquetFormat::default()))
            .with_file_extension(".parquet")
            .with_table_partition_cols(vec![("year".into(), DataType::Utf8)]);
        let resolved_schema = listing_options
            .infer_schema(&session_state, &listing_table_url)
            .await
            .unwrap();

        let config = ListingTableConfig::new(listing_table_url.clone())
            .with_listing_options(listing_options)
            .with_schema(resolved_schema.clone());

        let provider = Arc::new(ListingTable::try_new(config).unwrap());

        let df = ctx
            .read_table(provider)
            .unwrap()
            .select_columns(&["year", "col_1", "col_2", "col_3", "col_4"])
            .unwrap()
            .sort(vec![
                col("year").sort(true, false),
                col("col_1").sort(true, false),
            ])
            .unwrap();

        let batches = df.collect().await.unwrap();

        assert_batches_eq!(
            [
                "+------+-------+-------+-------+-------+",
                "| year | col_1 | col_2 | col_3 | col_4 |",
                "+------+-------+-------+-------+-------+",
                "| 2023 | a     | 1     | true  | 10    |",
                "| 2023 | b     | 2     | true  | 20    |",
                "| 2024 | c     | 3     | false | 30    |",
                "| 2024 | d     | 4     | false | 40    |",
                "+------+-------+-------+-------+-------+",
            ],
            batches.as_slice()
        );
    }
}
