//! # Aqueducts Delta Lake Integration
//!
//! This crate provides Delta Lake support for Aqueducts pipelines, enabling both reading from
//! and writing to Delta tables with full transaction support and ACID guarantees.
//!
//! ## Features
//!
//! - **DeltaSource**: Read from Delta tables with optional time travel and filtering
//! - **DeltaDestination**: Write to Delta tables with various modes (append, overwrite, upsert)
//! - **Object Store Integration**: Automatic registration of object stores for cloud storage
//! - **Schema Evolution**: Automatic schema merging and evolution support
//!
//! ## Usage
//!
//! This crate is typically used through the main `aqueducts` meta-crate with the `delta` feature:
//!
//! ```toml
//! [dependencies]
//! aqueducts = { version = "0.10", features = ["delta"] }
//! ```
//!
//! The Delta integration is automatically registered when the feature is enabled.
//! Configure Delta sources and destinations in your pipeline YAML/JSON/TOML files:

use aqueducts_schemas::{DeltaSource, DeltaWriteMode, ReplaceCondition};
use datafusion::{execution::context::SessionContext, prelude::DataFrame};
use deltalake::{
    arrow::datatypes::Schema, protocol::SaveMode, DeltaOps, DeltaTable, DeltaTableBuilder,
};
use error::DeltaError;
use std::sync::Arc;

use crate::handlers::register_handlers;

pub mod error;

pub mod handlers;

/// Register a Delta table as a source in the SessionContext.
///
/// This function allows reading from Delta tables with optional time travel
/// and automatic object store registration for cloud storage.
#[doc(hidden)]
pub async fn register_delta_source(
    ctx: Arc<SessionContext>,
    config: &DeltaSource,
) -> std::result::Result<(), DeltaError> {
    // Ensure Delta Lake handlers are registered
    register_handlers();

    tracing::debug!(
        "Registering delta source '{}' at location '{}'",
        config.name,
        config.location
    );

    let builder = DeltaTableBuilder::from_valid_uri(config.location.as_str())?
        .with_storage_options(config.storage_config.clone());

    let table = if let Some(version) = config.version {
        builder.with_version(version).load().await?
    } else if let Some(timestamp) = &config.timestamp {
        builder.with_timestamp(*timestamp).load().await?
    } else {
        builder.load().await?
    };

    ctx.register_table(&config.name, Arc::new(table))?;

    tracing::debug!("Successfully registered delta source '{}'", config.name);

    Ok(())
}
/// Prepare a Delta Lake destination for writing.
///
/// This function creates the Delta table if it doesn't exist and sets up
/// the necessary configuration for subsequent write operations.
pub async fn prepare_delta_destination(
    name: &str,
    location: &str,
    storage_config: &std::collections::HashMap<String, String>,
    partition_columns: &[String],
    table_properties: &std::collections::HashMap<String, Option<String>>,
    arrow_schema_fields: &[datafusion::arrow::datatypes::Field],
) -> std::result::Result<(), DeltaError> {
    // Ensure Delta Lake handlers are registered
    register_handlers();

    tracing::debug!(
        "Preparing delta destination '{}' at location '{}'",
        name,
        location
    );

    create_delta_table(
        location,
        storage_config,
        partition_columns,
        table_properties,
        arrow_schema_fields,
    )
    .await?;

    tracing::debug!("Successfully prepared delta destination '{}'", name);

    Ok(())
}

/// Write data to a Delta Lake destination.
///
/// This function handles various write modes including append, upsert,
/// and conditional replace operations based on the destination configuration.
pub async fn write_to_delta_destination(
    name: &str,
    location: &str,
    schema: &Schema,
    storage_config: &std::collections::HashMap<String, String>,
    write_mode: &DeltaWriteMode,
    data: DataFrame,
) -> std::result::Result<(), DeltaError> {
    // Ensure Delta Lake handlers are registered
    register_handlers();

    tracing::debug!(
        "Writing data to delta destination '{}' at location '{}'",
        name,
        location
    );

    write_delta_table(location, schema, storage_config, write_mode, data).await?;

    tracing::debug!("Successfully wrote data to delta destination '{}'", name);

    Ok(())
}

/// Create a deltatable and apply schema migrations by merging the schema when it diverges from the specified schema
/// Schema migration only supports adding new non-nullable columns and doesn't support modifying the table partitioning
/// A create is always executed with `deltalake::protocol::SaveMode::Ignore` which won't fail if the table already exists
async fn create_delta_table(
    location: &str,
    storage_config: &std::collections::HashMap<String, String>,
    partition_columns: &[String],
    table_properties: &std::collections::HashMap<String, Option<String>>,
    arrow_schema_fields: &[datafusion::arrow::datatypes::Field],
) -> std::result::Result<DeltaTable, DeltaError> {
    let table = DeltaOps::try_from_uri_with_storage_options(location, storage_config.clone())
        .await?
        .create()
        .with_save_mode(SaveMode::Ignore)
        .with_configuration(table_properties.clone())
        .with_columns(
            arrow_schema_fields
                .iter()
                .map(|field| deltalake::kernel::StructField::try_from(field).unwrap())
                .collect::<Vec<_>>(),
        )
        .with_partition_columns(partition_columns.to_vec())
        .await?;

    Ok(table)
}

async fn write_delta_table(
    location: &str,
    table_schema: &Schema,
    storage_config: &std::collections::HashMap<String, String>,
    write_mode: &DeltaWriteMode,
    data: DataFrame,
) -> std::result::Result<DeltaTable, DeltaError> {
    let data_schema = data.schema().as_arrow().clone();
    let validated_data = validate_schema(table_schema, data)?;

    let ops = DeltaOps::try_from_uri_with_storage_options(location, storage_config.clone()).await?;

    let table = match write_mode {
        DeltaWriteMode::Append => {
            let batches = validated_data.collect().await?;
            let batches = batches
                .into_iter()
                .map(|b| {
                    b.schema()
                        .fields()
                        .iter()
                        .map(|f| table_schema.field_with_name(f.name()).cloned())
                        .collect::<Result<Vec<_>, _>>()
                        .and_then(|fields| b.with_schema(Arc::new(Schema::new(fields))))
                })
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| {
                    DeltaError::DataFusion(datafusion::error::DataFusionError::ArrowError(e, None))
                })?;

            ops.write(batches).with_save_mode(SaveMode::Append).await?
        }
        DeltaWriteMode::Upsert(merge_cols) => {
            merge(ops, data_schema, merge_cols.clone(), validated_data).await?
        }
        DeltaWriteMode::Replace(conditions) => {
            let batches = validated_data.collect().await?;
            let batches = batches
                .into_iter()
                .map(|b| {
                    b.schema()
                        .fields()
                        .iter()
                        .map(|f| table_schema.field_with_name(f.name()).cloned())
                        .collect::<Result<Vec<_>, _>>()
                        .and_then(|fields| b.with_schema(Arc::new(Schema::new(fields))))
                })
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| {
                    DeltaError::DataFusion(datafusion::error::DataFusionError::ArrowError(e, None))
                })?;

            // Build replace predicate from conditions
            let replace_predicate = build_replace_predicate(conditions);

            ops.write(batches)
                .with_schema_mode(deltalake::operations::write::SchemaMode::Overwrite)
                .with_save_mode(SaveMode::Overwrite)
                .with_replace_where(replace_predicate)
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
) -> Result<DeltaTable, DeltaError> {
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
                    .and(array_empty(col(format!("new.{column_name}")))))
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
fn validate_schema(schema: &Schema, data: DataFrame) -> Result<DataFrame, DeltaError> {
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
                DataType::Binary if has_input_type(DataType::LargeBinary) => {
                    cast(col(field.name()), DataType::LargeBinary).alias(field.name())
                }
                _ => cast(col(field.name()), field.data_type().clone()).alias(field.name()),
            }
        })
        .collect::<Vec<Expr>>();

    let result = data.select(columns)?;
    assert!(result.schema().matches_arrow_schema(schema));

    Ok(result)
}

/// Build expression to replace values matching the `ReplaceCondition`s that was defined
fn build_replace_predicate(conditions: &[ReplaceCondition]) -> String {
    if conditions.is_empty() {
        return "true".to_string();
    }

    conditions
        .iter()
        .map(|condition| format!("{} = '{}'", condition.column, condition.value))
        .collect::<Vec<_>>()
        .join(" AND ")
}

#[cfg(test)]
mod tests {
    use super::*;
    use aqueducts_schemas::{DeltaDestination, DeltaWriteMode};
    use std::collections::HashMap;
    use url::Url;

    #[test]
    fn test_delta_destination_config_serialization() {
        let config = DeltaDestination {
            name: "test_table".to_string(),
            location: aqueducts_schemas::Location(Url::parse("file:///tmp/delta-table").unwrap()),
            write_mode: DeltaWriteMode::Append,
            storage_config: HashMap::new(),
            partition_columns: vec![],
            table_properties: HashMap::new(),
            metadata: HashMap::new(),
            schema: vec![aqueducts_schemas::Field {
                name: "test_col".to_string(),
                data_type: aqueducts_schemas::DataType::Utf8,
                nullable: false,
                description: None,
            }],
        };

        let serialized = serde_json::to_string(&config).unwrap();
        let deserialized: DeltaDestination = serde_json::from_str(&serialized).unwrap();

        assert_eq!(config.name, deserialized.name);
        assert_eq!(config.location, deserialized.location);
    }

    #[test]
    fn test_build_replace_predicate() {
        let conditions = vec![
            ReplaceCondition {
                column: "col1".to_string(),
                value: "value1".to_string(),
            },
            ReplaceCondition {
                column: "col2".to_string(),
                value: "value2".to_string(),
            },
        ];

        let predicate = build_replace_predicate(&conditions);
        assert_eq!(predicate, "col1 = 'value1' AND col2 = 'value2'");
    }
}
