//! Destination registration and behavior implementations

use aqueducts_storage::register_object_store;
use datafusion::{dataframe::DataFrame, datasource::MemTable, execution::context::SessionContext};
use std::sync::Arc;
use tracing::{debug, instrument};

use crate::model::destinations::{error, DeltaDestination, Destination, FileDestination, FileType};

pub(crate) type Result<T> = core::result::Result<T, error::Error>;

/// Creates a `Destination`
#[instrument(skip(ctx, destination), err)]
pub async fn register_destination(
    ctx: Arc<SessionContext>,
    destination: &Destination,
) -> Result<()> {
    match destination {
        Destination::InMemory(_) => Ok(()),
        Destination::Delta(table_def) => {
            debug!(
                "Creating delta table  (if it doesn't exist yet) '{}' at location '{}'",
                table_def.name, table_def.location
            );

            let _ = create_delta(table_def).await?;
            Ok(())
        }
        Destination::File(file_def) => {
            register_object_store(ctx, &file_def.location, &file_def.storage_options)?;
            Ok(())
        }
        #[cfg(feature = "odbc")]
        Destination::Odbc(odbc_def) => {
            aqueducts_odbc::register_odbc_destination(
                odbc_def.connection_string.as_str(),
                odbc_def.name.as_str(),
            )
            .await?;
            Ok(())
        }
    }
}

/// Write a `DataFrame` to an Aqueduct `Destination`
#[instrument(skip(ctx, destination, data), err)]
pub async fn write_to_destination(
    ctx: Arc<SessionContext>,
    destination: &Destination,
    data: DataFrame,
) -> Result<()> {
    match destination {
        Destination::InMemory(mem_def) => {
            debug!("Writing data to in-memory table '{}'", mem_def.name);

            let schema = data.schema().clone();
            let partitioned = data.collect_partitioned().await?;
            let table = MemTable::try_new(Arc::new(schema.as_arrow().clone()), partitioned)?;

            ctx.register_table(mem_def.name.as_str(), Arc::new(table))?;

            Ok(())
        }
        Destination::Delta(table_def) => {
            debug!(
                "Writing data to delta table '{}' at location '{}'",
                table_def.name, table_def.location
            );
            let _ = write_delta(table_def, data).await?;

            Ok(())
        }
        Destination::File(file_def) => {
            debug!("Writing data to file at location '{}'", file_def.location);
            write_file(file_def, data).await?;

            Ok(())
        }
        #[cfg(feature = "odbc")]
        Destination::Odbc(odbc_def) => {
            write_odbc(odbc_def, data).await?;

            Ok(())
        }
    }
}

/// Create a deltatable and apply schema migrations by merging the schema when it diverges from the specified schema
/// Schema migration only supports adding new non-nullable columns and doesn't support modifying the table partitioning
/// A create is always executed with `deltalake::protocol::SaveMode::Ignore` which won't fail if the table already exists
pub(crate) async fn create_delta(table_def: &DeltaDestination) -> Result<deltalake::DeltaTable> {
    let table = deltalake::DeltaOps::try_from_uri_with_storage_options(
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

pub(crate) async fn write_delta(
    table_def: &DeltaDestination,
    data: DataFrame,
) -> Result<deltalake::DeltaTable> {
    use crate::model::destinations::WriteMode;
    use datafusion::arrow::datatypes::DataType;
    use datafusion::prelude::{array_empty, array_has_all, col, Expr};

    let table_schema = deltalake::kernel::StructType::new(table_def.schema.clone());
    let table_schema = TryInto::<deltalake::arrow::datatypes::Schema>::try_into(&table_schema)?;
    let data = validate_schema(table_schema.clone(), data)?;

    let ops = deltalake::DeltaOps::try_from_uri_with_storage_options(
        table_def.location.clone(),
        table_def.storage_options.clone(),
    )
    .await?;

    let table = match &table_def.write_mode {
        WriteMode::Append => {
            let batches = data.collect().await?;
            ops.write(batches)
                .with_save_mode(deltalake::protocol::SaveMode::Append)
                .await?
        }
        WriteMode::Upsert(merge_cols) => {
            // Implementation for Upsert using Delta Lake merge operation
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

            table
        }
        WriteMode::Replace(conditions) => {
            // Implementation for Replace using Delta Lake replace condition
            let batches = data.collect().await?;

            let replace_expr = build_delta_replace_expression(conditions);

            ops.write(batches)
                .with_schema_mode(deltalake::operations::write::SchemaMode::Overwrite)
                .with_save_mode(deltalake::protocol::SaveMode::Overwrite)
                .with_replace_where(replace_expr)
                .await?
        }
    };

    Ok(table)
}

/// Validate if the table schema matches the data that is about to be written (casts the dataframe to the output schema)
fn validate_schema(
    schema: deltalake::arrow::datatypes::Schema,
    data: DataFrame,
) -> Result<DataFrame> {
    use datafusion::arrow::datatypes::DataType;
    use datafusion::prelude::{cast, col, Expr};

    let columns = schema
        .fields
        .into_iter()
        .map(|field| {
            let has_input_type = |data_type: DataType| {
                data.schema()
                    .field_with_unqualified_name(field.name())
                    .map(|f| f.data_type().equals_datatype(&data_type))
                    .unwrap_or_default()
            };

            match field.data_type() {
                DataType::Utf8 if has_input_type(DataType::LargeUtf8) => {
                    cast(col(field.name()), DataType::LargeUtf8).alias(field.name())
                }
                deltalake::arrow::datatypes::DataType::Binary
                    if has_input_type(DataType::LargeBinary) =>
                {
                    cast(col(field.name()), DataType::LargeBinary).alias(field.name())
                }
                _ => cast(col(field.name()), field.data_type().clone()).alias(field.name()),
            }
        })
        .collect::<Vec<Expr>>();

    let result = data.select(columns)?;
    assert!(result.schema().matches_arrow_schema(&schema));

    Ok(result)
}

/// Build expression to replace values matching the `ReplaceCondition`s that was defined
fn build_delta_replace_expression(
    conditions: &[crate::model::destinations::ReplaceCondition],
) -> datafusion::logical_expr::Expr {
    use datafusion::prelude::{col, lit};

    conditions
        .iter()
        .map(
            |crate::model::destinations::ReplaceCondition {
                 column: field_name,
                 value,
             }| col(field_name).eq(lit(value.clone())),
        )
        .fold(lit(true), |agg, cond| agg.and(cond))
}

pub(crate) async fn write_file(file_def: &FileDestination, data: DataFrame) -> Result<()> {
    use datafusion::config::{ConfigField, TableParquetOptions};
    use datafusion::dataframe::DataFrameWriteOptions;

    let write_options = DataFrameWriteOptions::default()
        .with_partition_by(file_def.partition_cols.clone())
        .with_single_file_output(file_def.single_file);

    let _ = match &file_def.file_type {
        FileType::Parquet(options) => {
            let mut parquet_options = TableParquetOptions::default();

            options
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
            use datafusion::config::CsvOptions;

            let csv_options = CsvOptions::default()
                .with_has_header(csv_options.has_header.unwrap_or(true))
                .with_delimiter(csv_options.delimiter.unwrap_or(',') as u8);

            data.write_csv(file_def.location.as_str(), write_options, Some(csv_options))
                .await?
        }
        FileType::Json => {
            data.write_json(file_def.location.as_str(), write_options, None)
                .await?
        }
    };

    Ok(())
}

#[cfg(feature = "odbc")]
pub(crate) async fn write_odbc(
    odbc_def: &crate::model::destinations::OdbcDestination,
    data: DataFrame,
) -> Result<()> {
    aqueducts_odbc::write_to_table(
        odbc_def.connection_string.as_str(),
        odbc_def.name.as_str(),
        data,
    )
    .await
    .map_err(error::Error::OdbcError)
}
