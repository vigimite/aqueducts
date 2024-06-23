use aqueducts_utils::serde::deserialize_file_location;
use datafusion::dataframe::DataFrame;
use deltalake::{
    arrow::datatypes::Schema,
    kernel::{StructField, StructType},
    protocol::SaveMode,
    DeltaOps, DeltaTable,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use url::Url;

use super::Result;

/// A delta table destination
#[derive(Debug, Clone, Serialize, Deserialize, derive_new::new, schemars::JsonSchema)]
pub struct DeltaDestination {
    /// Name of the table
    pub name: String,

    /// Location of the table as a URL e.g. file:///tmp/delta_table/, s3://bucket_name/delta_table
    #[serde(deserialize_with = "deserialize_file_location")]
    pub location: Url,

    /// DeltaTable storage options
    #[serde(default)]
    pub storage_options: HashMap<String, String>,

    /// DeltaTable table properties: <https://docs.delta.io/latest/table-properties.html>
    pub table_properties: HashMap<String, Option<String>>,

    /// Columns that will be used to determine uniqueness during merge operation
    /// Supported types: All primitive types and lists of primitive types
    pub write_mode: WriteMode,

    /// Columns to partition table by
    pub partition_cols: Vec<String>,

    /// Table schema definition `deltalake_core::models::schema::StructField`
    #[schemars(skip)]
    pub schema: Vec<StructField>,
}

/// Write modes for the `Destination` output.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(tag = "operation", content = "params")]
pub enum WriteMode {
    /// `Append`: appends data to the `Destination`
    Append,

    /// `Upsert`: upserts data to the `Destination` using the specified merge columns
    Upsert(Vec<String>),

    /// `Replace`: replaces data to the `Destination` using the specified `ReplaceCondition`s
    Replace(Vec<ReplaceCondition>),
}

/// Condition used to build a predicate by which data should be replaced in a `Destination`
/// Expression built is checking equality for the given `value` of a field with `field_name`
#[derive(Debug, Clone, Serialize, Deserialize, derive_new::new, schemars::JsonSchema)]
pub struct ReplaceCondition {
    column: String,
    value: String,
}

/// Create a deltatable and apply schema migrations by merging the schema when it diverges from the specified schema
/// Schema migration only supports adding new non-nullable columns and doesn't support modifying the table partitioning
/// A create is always executed with `deltalake::protocol::SaveMode::Ignore` which won't fail if the table already exists
pub(super) async fn create(table_def: &DeltaDestination) -> Result<DeltaTable> {
    let table = DeltaOps::try_from_uri_with_storage_options(
        table_def.location.as_str(),
        table_def.storage_options.clone(),
    )
    .await?
    .create()
    .with_save_mode(deltalake::protocol::SaveMode::Ignore)
    .with_configuration(table_def.table_properties.clone())
    .with_columns(table_def.schema.clone())
    .with_partition_columns(table_def.partition_cols.clone())
    .await?;

    Ok(table)
}

pub(super) async fn write(table_def: &DeltaDestination, data: DataFrame) -> Result<DeltaTable> {
    let table_schema = StructType::new(table_def.schema.clone());
    let table_schema = TryInto::<Schema>::try_into(&table_schema)?;
    let data = validate_schema(table_schema.clone(), data)?;

    let ops = DeltaOps::try_from_uri_with_storage_options(
        table_def.location.clone(),
        table_def.storage_options.clone(),
    )
    .await?;

    let table = match &table_def.write_mode {
        WriteMode::Append => {
            let batches = data.collect().await?;
            ops.write(batches).with_save_mode(SaveMode::Append).await?
        }
        WriteMode::Upsert(merge_cols) => merge(ops, table_schema, merge_cols.clone(), data).await?,
        WriteMode::Replace(conditions) => {
            let batches = data.collect().await?;

            ops.write(batches)
                .with_schema_mode(deltalake::operations::write::SchemaMode::Merge)
                .with_save_mode(SaveMode::Overwrite)
                .with_replace_where(build_expression(conditions.clone()))
                .await?
        }
    };

    Ok(table)
}

/// Merge a dataframe with a deltatable
/// This merge behaves like an upsert where the merge columns are used as the unique keys and every other column is updated to the new values provided by the dataframe
/// Additionally this merge can check for equality on lists of primitive types where equality is determined by the contained elements but NOT on the element order
async fn merge(
    ops: DeltaOps,
    table_schema: Schema,
    merge_cols: Vec<String>,
    data: DataFrame,
) -> Result<DeltaTable> {
    use datafusion::arrow::datatypes::DataType;
    use datafusion::prelude::{array_empty, array_has_all, col, Expr};

    let merge_predicate = merge_cols
        .iter()
        .map(|column_name| {
            let field = table_schema
                .field_with_name(column_name.as_str())
                .expect("field not found in schema");

            match field.data_type() {
                DataType::Struct(_)
                | DataType::Union(_, _)
                | DataType::Dictionary(_, _)
                | DataType::Map(_, _)
                | DataType::Binary
                | DataType::LargeBinary
                | DataType::FixedSizeBinary(_) => {
                    unimplemented!("unsupported column type for merge")
                }
                DataType::List(_) => array_has_all(
                    col(format!("old.{column_name}")),
                    col(format!("new.{column_name}")),
                )
                .and(array_has_all(
                    col(format!("new.{column_name}")),
                    col(format!("old.{column_name}")),
                ))
                .or(array_empty(col(format!("old.{column_name}")))
                    .and(array_empty(col(format!("old.{column_name}")))))
                .or(col(format!("old.{column_name}"))
                    .is_null()
                    .and(col(format!("new.{column_name}")).is_null())),
                _ => col(format!("old.{column_name}"))
                    .eq(col(format!("new.{column_name}")))
                    .or(col(format!("old.{column_name}"))
                        .is_null()
                        .and(col(format!("new.{column_name}")).is_null())),
            }
        })
        .collect::<Vec<Expr>>();

    let update_columns = table_schema
        .fields
        .iter()
        .filter_map(|column| {
            let column_name = column.name();
            if merge_cols.contains(column_name) {
                None
            } else {
                Some(column_name.clone())
            }
        })
        .collect::<Vec<String>>();

    let (table, _) = ops
        .merge(
            data,
            merge_predicate
                .into_iter()
                .reduce(|acc, e| acc.and(e))
                .expect("merge predicate is empty"),
        )
        .with_target_alias("old")
        .with_source_alias("new")
        .when_not_matched_insert(|insert| {
            table_schema
                .fields
                .iter()
                .map(|field| field.name().clone())
                .fold(insert, |acc, column_name| {
                    acc.set(column_name.as_str(), col(format!("new.{column_name}")))
                })
        })?
        .when_matched_update(|update| {
            update_columns.into_iter().fold(update, |acc, column_name| {
                acc.update(column_name.as_str(), col(format!("new.{column_name}")))
            })
        })?
        .await?;

    Ok(table)
}

/// Validate if the table schema matches the data that is about to be written (casts the dataframe to the output schema)
fn validate_schema(schema: Schema, data: DataFrame) -> Result<DataFrame> {
    use datafusion::prelude::{cast, col, Expr};

    let columns = schema
        .fields
        .into_iter()
        .map(|field| cast(col(field.name()), field.data_type().clone()).alias(field.name()))
        .collect::<Vec<Expr>>();

    let result = data.select(columns)?;
    assert!(result.schema().matches_arrow_schema(&schema));

    Ok(result)
}

/// Build expression to replace values matching the `ReplaceCondition`s that was defined
fn build_expression(conditions: Vec<ReplaceCondition>) -> datafusion::logical_expr::Expr {
    use datafusion::prelude::{col, lit};

    conditions
        .into_iter()
        .map(
            |ReplaceCondition {
                 column: field_name,
                 value,
             }| col(field_name).eq(lit(value)),
        )
        .fold(lit(true), |agg, cond| agg.and(cond))
}

#[cfg(test)]
mod tests {
    use datafusion::prelude::*;
    use datafusion::{
        arrow::{
            array::{ArrayRef, BooleanArray, Int32Array, StringArray},
            datatypes::Schema,
            record_batch::RecordBatch,
        },
        assert_batches_eq,
    };
    use deltalake::kernel::{DataType, PrimitiveType, StructField, StructType};
    use std::path::Path;
    use std::{collections::HashMap, sync::Arc};

    use super::*;

    fn generate_test_table_path() -> Url {
        let local_path = Path::new(".")
            .canonicalize()
            .unwrap()
            .into_os_string()
            .into_string()
            .unwrap();

        let run_id = rand::random::<usize>();
        let table_path = format!("file://{local_path}/tests/output/test_delta/{run_id}/test_table");

        Url::parse(table_path.as_str()).unwrap()
    }

    #[tokio::test]
    async fn test_create_table_ok() {
        let ctx = SessionContext::new();

        // Define table
        let schema = vec![
            StructField::new("col_1", DataType::Primitive(PrimitiveType::String), false),
            StructField::new("col_2", DataType::Primitive(PrimitiveType::Integer), false),
            StructField::new("col_3", DataType::Primitive(PrimitiveType::Boolean), false),
            StructField::new("col_4", DataType::Primitive(PrimitiveType::Integer), false),
        ];

        let location = generate_test_table_path();
        let definition = DeltaDestination::new(
            "test_table".into(),
            location,
            HashMap::default(),
            HashMap::default(),
            WriteMode::Upsert(vec![String::from("col_1")]),
            vec!["col_1".into()],
            schema.clone(),
        );

        // Create the table
        let table = create(&definition).await.unwrap();

        let result = ctx
            .read_table(Arc::new(table.clone()))
            .unwrap()
            .select_columns(&["col_1", "col_2", "col_3", "col_4"])
            .unwrap();

        let expected_schema = TryInto::<Schema>::try_into(&StructType::new(schema)).unwrap();

        assert!(result.schema().matches_arrow_schema(&expected_schema));
    }

    #[tokio::test]
    async fn test_merge_dataframe_ok() {
        let ctx = SessionContext::new();
        let location = generate_test_table_path();

        // Define table
        let schema = vec![
            StructField::new("col_1", DataType::Primitive(PrimitiveType::String), false),
            StructField::new("col_2", DataType::Primitive(PrimitiveType::Integer), false),
            StructField::new("col_3", DataType::Primitive(PrimitiveType::Boolean), false),
            StructField::new("col_4", DataType::Primitive(PrimitiveType::Integer), false),
        ];
        let definition = DeltaDestination::new(
            "test_table".into(),
            location,
            HashMap::default(),
            HashMap::default(),
            WriteMode::Upsert(vec![String::from("col_1")]),
            vec!["col_1".into()],
            schema.clone(),
        );

        // Create the table
        let _ = create(&definition).await.unwrap();

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
        let delta_table = write(&definition, df).await.unwrap();

        let df = ctx
            .read_table(Arc::new(delta_table))
            .unwrap()
            .select_columns(&["col_1", "col_2", "col_3", "col_4"])
            .unwrap()
            .sort(vec![col("col_1").sort(true, false)])
            .unwrap();

        let initial = df.clone().collect().await.unwrap();

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
            initial.as_slice()
        );

        let updated = df
            .select(vec![
                col("col_1"),
                (col("col_2") * lit(2)).alias("col_2"),
                col("col_3").not().alias("col_3"),
                (col("col_4") * lit(10)).alias("col_4"),
            ])
            .unwrap();

        let delta_table = write(&definition, updated).await.unwrap();

        let df = ctx
            .read_table(Arc::new(delta_table))
            .unwrap()
            .select_columns(&["col_1", "col_2", "col_3", "col_4"])
            .unwrap()
            .sort(vec![col("col_1").sort(true, false)])
            .unwrap();

        let initial = df.clone().collect().await.unwrap();

        assert_batches_eq!(
            [
                "+-------+-------+-------+-------+",
                "| col_1 | col_2 | col_3 | col_4 |",
                "+-------+-------+-------+-------+",
                "| a     | 2     | false | 100   |",
                "| b     | 4     | false | 200   |",
                "| c     | 6     | true  | 300   |",
                "| d     | 8     | true  | 400   |",
                "+-------+-------+-------+-------+",
            ],
            initial.as_slice()
        );
    }
}
