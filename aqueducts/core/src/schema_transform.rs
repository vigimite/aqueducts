//! Schema transformation utilities for converting between aqueducts schemas and backend formats.
//!
//! This module provides functions to convert the universal Field and DataType definitions
//! from aqueducts-schemas into Arrow and Delta Lake specific schema representations.

use aqueducts_schemas::{DataType, Field, IntervalUnit, TimeUnit, UnionMode};
use datafusion::arrow::datatypes::{
    DataType as ArrowDataType, Field as ArrowField, IntervalUnit as ArrowIntervalUnit,
    Schema as ArrowSchema, TimeUnit as ArrowTimeUnit, UnionMode as ArrowUnionMode,
};
use datafusion::common::Result;
use std::sync::Arc;

/// Convert an aqueducts Field to an Arrow Field
pub fn field_to_arrow(field: &Field) -> Result<ArrowField> {
    let arrow_data_type = data_type_to_arrow(&field.data_type)?;
    Ok(ArrowField::new(
        &field.name,
        arrow_data_type,
        field.nullable,
    ))
}

/// Convert a vector of aqueducts Fields to an Arrow Schema
pub fn fields_to_arrow_schema(fields: &[Field]) -> Result<ArrowSchema> {
    let arrow_fields: Result<Vec<ArrowField>> = fields.iter().map(field_to_arrow).collect();
    Ok(ArrowSchema::new(arrow_fields?))
}

/// Convert an aqueducts DataType to an Arrow DataType
pub fn data_type_to_arrow(data_type: &DataType) -> Result<ArrowDataType> {
    match data_type {
        // Primitive types
        DataType::Boolean => Ok(ArrowDataType::Boolean),
        DataType::Int8 => Ok(ArrowDataType::Int8),
        DataType::Int16 => Ok(ArrowDataType::Int16),
        DataType::Int32 => Ok(ArrowDataType::Int32),
        DataType::Int64 => Ok(ArrowDataType::Int64),
        DataType::UInt8 => Ok(ArrowDataType::UInt8),
        DataType::UInt16 => Ok(ArrowDataType::UInt16),
        DataType::UInt32 => Ok(ArrowDataType::UInt32),
        DataType::UInt64 => Ok(ArrowDataType::UInt64),
        DataType::Float32 => Ok(ArrowDataType::Float32),
        DataType::Float64 => Ok(ArrowDataType::Float64),

        // String types
        DataType::Utf8 => Ok(ArrowDataType::Utf8),
        DataType::LargeUtf8 => Ok(ArrowDataType::LargeUtf8),

        // Binary types
        DataType::Binary => Ok(ArrowDataType::Binary),
        DataType::LargeBinary => Ok(ArrowDataType::LargeBinary),
        DataType::FixedSizeBinary(size) => Ok(ArrowDataType::FixedSizeBinary(*size)),

        // Temporal types
        DataType::Date32 => Ok(ArrowDataType::Date32),
        DataType::Date64 => Ok(ArrowDataType::Date64),
        DataType::Time32(unit) => Ok(ArrowDataType::Time32(time_unit_to_arrow(unit))),
        DataType::Time64(unit) => Ok(ArrowDataType::Time64(time_unit_to_arrow(unit))),
        DataType::Timestamp(unit, tz) => Ok(ArrowDataType::Timestamp(
            time_unit_to_arrow(unit),
            tz.as_ref().map(|s| Arc::from(s.as_str())),
        )),
        DataType::Duration(unit) => Ok(ArrowDataType::Duration(time_unit_to_arrow(unit))),
        DataType::Interval(unit) => Ok(ArrowDataType::Interval(interval_unit_to_arrow(unit))),

        // Decimal types
        DataType::Decimal128(precision, scale) => Ok(ArrowDataType::Decimal128(*precision, *scale)),
        DataType::Decimal256(precision, scale) => Ok(ArrowDataType::Decimal256(*precision, *scale)),

        // Nested types
        DataType::List(field) => {
            let arrow_field = field_to_arrow(field)?;
            Ok(ArrowDataType::List(Arc::new(arrow_field)))
        }
        DataType::LargeList(field) => {
            let arrow_field = field_to_arrow(field)?;
            Ok(ArrowDataType::LargeList(Arc::new(arrow_field)))
        }
        DataType::FixedSizeList(field, size) => {
            let arrow_field = field_to_arrow(field)?;
            Ok(ArrowDataType::FixedSizeList(Arc::new(arrow_field), *size))
        }
        DataType::Struct(fields) => {
            let arrow_fields: Result<Vec<ArrowField>> = fields.iter().map(field_to_arrow).collect();
            Ok(ArrowDataType::Struct(arrow_fields?.into()))
        }
        DataType::Map(key_field, value_field, keys_sorted) => {
            let key_arrow = field_to_arrow(key_field)?;
            let value_arrow = field_to_arrow(value_field)?;

            // Arrow Map requires a struct field containing key and value
            let map_field = ArrowField::new(
                "entries",
                ArrowDataType::Struct(vec![key_arrow, value_arrow].into()),
                false,
            );
            Ok(ArrowDataType::Map(Arc::new(map_field), *keys_sorted))
        }
        DataType::Union(fields, mode) => {
            let arrow_fields: Result<Vec<ArrowField>> = fields
                .iter()
                .map(|(_, field)| field_to_arrow(field))
                .collect();
            let type_ids: Vec<i8> = fields.iter().map(|(id, _)| *id).collect();

            use datafusion::arrow::datatypes::UnionFields;
            let union_fields = UnionFields::new(type_ids, arrow_fields?);

            Ok(ArrowDataType::Union(
                union_fields,
                union_mode_to_arrow(mode),
            ))
        }
        DataType::Dictionary(key_type, value_type) => {
            let key_arrow = data_type_to_arrow(key_type)?;
            let value_arrow = data_type_to_arrow(value_type)?;
            Ok(ArrowDataType::Dictionary(
                Box::new(key_arrow),
                Box::new(value_arrow),
            ))
        }
    }
}

fn time_unit_to_arrow(unit: &TimeUnit) -> ArrowTimeUnit {
    match unit {
        TimeUnit::Second => ArrowTimeUnit::Second,
        TimeUnit::Millisecond => ArrowTimeUnit::Millisecond,
        TimeUnit::Microsecond => ArrowTimeUnit::Microsecond,
        TimeUnit::Nanosecond => ArrowTimeUnit::Nanosecond,
    }
}

fn interval_unit_to_arrow(unit: &IntervalUnit) -> ArrowIntervalUnit {
    match unit {
        IntervalUnit::YearMonth => ArrowIntervalUnit::YearMonth,
        IntervalUnit::DayTime => ArrowIntervalUnit::DayTime,
        IntervalUnit::MonthDayNano => ArrowIntervalUnit::MonthDayNano,
    }
}

fn union_mode_to_arrow(mode: &UnionMode) -> ArrowUnionMode {
    match mode {
        UnionMode::Sparse => ArrowUnionMode::Sparse,
        UnionMode::Dense => ArrowUnionMode::Dense,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aqueducts_schemas::{DataType, Field};

    #[test]
    fn test_basic_field_conversion() {
        let field = Field {
            name: "test_field".to_string(),
            data_type: DataType::Int32,
            nullable: true,
            description: None,
        };

        let arrow_field = field_to_arrow(&field).unwrap();
        assert_eq!(arrow_field.name(), "test_field");
        assert_eq!(arrow_field.data_type(), &ArrowDataType::Int32);
        assert!(arrow_field.is_nullable());
    }

    #[test]
    fn test_complex_type_conversion() {
        let list_field = Field {
            name: "items".to_string(),
            data_type: DataType::List(Box::new(Field {
                name: "item".to_string(),
                data_type: DataType::Utf8,
                nullable: true,
                description: None,
            })),
            nullable: false,
            description: None,
        };

        let arrow_field = field_to_arrow(&list_field).unwrap();
        assert_eq!(arrow_field.name(), "items");
        assert!(!arrow_field.is_nullable());

        if let ArrowDataType::List(inner) = arrow_field.data_type() {
            assert_eq!(inner.name(), "item");
            assert_eq!(inner.data_type(), &ArrowDataType::Utf8);
        } else {
            panic!("Expected List type");
        }
    }

    #[test]
    fn test_struct_type_conversion() {
        let struct_field = Field {
            name: "person".to_string(),
            data_type: DataType::Struct(vec![
                Field {
                    name: "name".to_string(),
                    data_type: DataType::Utf8,
                    nullable: false,
                    description: None,
                },
                Field {
                    name: "age".to_string(),
                    data_type: DataType::Int32,
                    nullable: true,
                    description: None,
                },
            ]),
            nullable: false,
            description: None,
        };

        let arrow_field = field_to_arrow(&struct_field).unwrap();
        if let ArrowDataType::Struct(fields) = arrow_field.data_type() {
            assert_eq!(fields.len(), 2);
            assert_eq!(fields[0].name(), "name");
            assert_eq!(fields[0].data_type(), &ArrowDataType::Utf8);
            assert_eq!(fields[1].name(), "age");
            assert_eq!(fields[1].data_type(), &ArrowDataType::Int32);
        } else {
            panic!("Expected Struct type");
        }
    }

    #[test]
    fn test_map_type_conversion() {
        let key_field = Field {
            name: "key".to_string(),
            data_type: DataType::Utf8,
            nullable: false,
            description: None,
        };

        let value_field = Field {
            name: "value".to_string(),
            data_type: DataType::Int32,
            nullable: true,
            description: None,
        };

        let map_field = Field {
            name: "metadata".to_string(),
            data_type: DataType::Map(Box::new(key_field), Box::new(value_field), false),
            nullable: false,
            description: None,
        };

        let arrow_field = field_to_arrow(&map_field).unwrap();
        assert_eq!(arrow_field.name(), "metadata");
        assert!(!arrow_field.is_nullable());

        if let ArrowDataType::Map(entries_field, keys_sorted) = arrow_field.data_type() {
            assert!(!keys_sorted);
            assert_eq!(entries_field.name(), "entries");
            assert!(!entries_field.is_nullable());

            if let ArrowDataType::Struct(struct_fields) = entries_field.data_type() {
                assert_eq!(struct_fields.len(), 2);

                let key_arrow = &struct_fields[0];
                assert_eq!(key_arrow.name(), "key");
                assert_eq!(key_arrow.data_type(), &ArrowDataType::Utf8);
                assert!(!key_arrow.is_nullable());

                let value_arrow = &struct_fields[1];
                assert_eq!(value_arrow.name(), "value");
                assert_eq!(value_arrow.data_type(), &ArrowDataType::Int32);
                assert!(value_arrow.is_nullable());
            } else {
                panic!("Expected Struct type for Map entries");
            }
        } else {
            panic!("Expected Map type");
        }
    }

    #[test]
    fn test_schema_conversion() {
        let fields = vec![
            Field {
                name: "id".to_string(),
                data_type: DataType::Int64,
                nullable: false,
                description: None,
            },
            Field {
                name: "name".to_string(),
                data_type: DataType::Utf8,
                nullable: true,
                description: None,
            },
        ];

        let schema = fields_to_arrow_schema(&fields).unwrap();
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(1).name(), "name");
    }
}
