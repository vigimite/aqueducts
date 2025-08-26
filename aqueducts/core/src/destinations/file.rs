use aqueducts_schemas::{destinations::DestinationFileType, FileDestination};
use datafusion::config::{ConfigField, CsvOptions, TableParquetOptions};
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::prelude::*;

use super::DestinationError;

pub(super) async fn write(
    file_def: &FileDestination,
    data: DataFrame,
) -> Result<(), DestinationError> {
    let write_options = DataFrameWriteOptions::default()
        .with_partition_by(file_def.partition_columns.clone())
        .with_single_file_output(file_def.single_file);

    let _ = match &file_def.format {
        DestinationFileType::Parquet(options) => {
            let mut parquet_options = TableParquetOptions::default();

            options
                .iter()
                .try_for_each(|(k, v)| parquet_options.set(k.as_str(), v.as_str()))
                .expect("failed to set parquet options");

            data.write_parquet(
                file_def.location.as_str(),
                write_options,
                Some(parquet_options),
            )
            .await
            .map_err(|error| DestinationError::WriteFile {
                name: file_def.name.clone(),
                error,
            })?
        }
        DestinationFileType::Csv(csv_options) => {
            let csv_options = CsvOptions::default()
                .with_has_header(csv_options.has_header)
                .with_delimiter(csv_options.delimiter as u8);

            data.write_csv(file_def.location.as_str(), write_options, Some(csv_options))
                .await
                .map_err(|error| DestinationError::WriteFile {
                    name: file_def.name.clone(),
                    error,
                })?
        }
        DestinationFileType::Json => data
            .write_json(file_def.location.as_str(), write_options, None)
            .await
            .map_err(|error| DestinationError::WriteFile {
                name: file_def.name.clone(),
                error,
            })?,
    };

    Ok(())
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{ArrayRef, BooleanArray, Int32Array, StringArray};
    use datafusion::arrow::datatypes::DataType;
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::assert_batches_eq;
    use datafusion::datasource::listing::{
        ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
    };
    use datafusion::datasource::physical_plan::parquet::ParquetFormat;
    use datafusion::prelude::*;
    use std::{collections::HashMap, path::Path, sync::Arc};

    use super::*;
    use aqueducts_schemas::destinations::{CsvDestinationOptions, DestinationFileType};
    use aqueducts_schemas::Location;

    fn create_test_record_batch() -> RecordBatch {
        let col_str = Arc::new(StringArray::from(vec!["a", "b", "c", "d"])) as ArrayRef;
        let col_int = Arc::new(Int32Array::from(vec![1, 2, 3, 4])) as ArrayRef;
        let col_bool = Arc::new(BooleanArray::from(vec![true, true, false, false])) as ArrayRef;
        let col_val = Arc::new(Int32Array::from(vec![10, 20, 30, 40])) as ArrayRef;

        RecordBatch::try_from_iter(vec![
            ("col_str", col_str),
            ("col_int", col_int),
            ("col_bool", col_bool),
            ("col_val", col_val),
        ])
        .unwrap()
    }

    fn generate_test_file_path(file_name: &str) -> url::Url {
        let local_path = Path::new(".")
            .canonicalize()
            .unwrap()
            .into_os_string()
            .into_string()
            .unwrap();

        let file_path = format!("file://{local_path}/tests/output/test_file/{file_name}");
        url::Url::parse(&file_path).unwrap()
    }

    #[tokio::test]
    async fn test_write_csv_ok() {
        let ctx = SessionContext::new();

        let path = generate_test_file_path("csv/write.csv");
        let definition = FileDestination::builder()
            .name("write".to_string())
            .location(Location(path.clone()))
            .format(DestinationFileType::Csv(CsvDestinationOptions::default()))
            .single_file(true)
            .build();

        // Create test data using helper
        let batch = create_test_record_batch();
        let df = ctx.read_batch(batch).unwrap();
        write(&definition, df).await.unwrap();

        let df = ctx
            .read_csv(path.as_str(), CsvReadOptions::default())
            .await
            .unwrap()
            .select_columns(&["col_str", "col_int", "col_bool", "col_val"])
            .unwrap()
            .sort(vec![col("col_str").sort(true, false)])
            .unwrap();

        let batches = df.collect().await.unwrap();

        assert_batches_eq!(
            [
                "+---------+---------+----------+---------+",
                "| col_str | col_int | col_bool | col_val |",
                "+---------+---------+----------+---------+",
                "| a       | 1       | true     | 10      |",
                "| b       | 2       | true     | 20      |",
                "| c       | 3       | false    | 30      |",
                "| d       | 4       | false    | 40      |",
                "+---------+---------+----------+---------+",
            ],
            batches.as_slice()
        );
    }

    #[tokio::test]
    async fn test_write_parquet_ok() {
        let ctx = SessionContext::new();

        let path = generate_test_file_path("parquet/write.parquet");
        let definition = FileDestination::builder()
            .name("write".to_string())
            .location(Location(path.clone()))
            .format(DestinationFileType::Parquet(HashMap::default()))
            .single_file(true)
            .build();

        // Create test data using helper
        let batch = create_test_record_batch();
        let df = ctx.read_batch(batch).unwrap();
        write(&definition, df).await.unwrap();

        let df = ctx
            .read_parquet(path.as_str(), ParquetReadOptions::default())
            .await
            .unwrap()
            .select_columns(&["col_str", "col_int", "col_bool", "col_val"])
            .unwrap()
            .sort(vec![col("col_str").sort(true, false)])
            .unwrap();

        let batches = df.collect().await.unwrap();

        assert_batches_eq!(
            [
                "+---------+---------+----------+---------+",
                "| col_str | col_int | col_bool | col_val |",
                "+---------+---------+----------+---------+",
                "| a       | 1       | true     | 10      |",
                "| b       | 2       | true     | 20      |",
                "| c       | 3       | false    | 30      |",
                "| d       | 4       | false    | 40      |",
                "+---------+---------+----------+---------+",
            ],
            batches.as_slice()
        );
    }

    #[tokio::test]
    async fn test_write_parquet_partitioned_ok() {
        // Setup
        let ctx = SessionContext::new();

        let suffix = format!("partitioned/{}/", rand::random::<u64>());
        let path = generate_test_file_path(suffix.as_str());
        let definition = FileDestination::builder()
            .name("write".into())
            .location(Location(path.clone()))
            .format(DestinationFileType::Parquet(HashMap::default()))
            .single_file(false)
            .partition_columns(vec!["year".into()])
            .storage_config(HashMap::default())
            .build();

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
        write(&definition, df).await.unwrap();

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
