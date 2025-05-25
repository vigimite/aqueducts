//! Custom schema types that work universally across all aqueducts backends.
//!
//! These types provide a unified way to define data schemas that can be converted
//! to Arrow, Delta Lake, and other backend-specific schema formats.

use crate::serde_helpers::{default_true, deserialize_data_type};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

/// A field definition that can be used across all aqueducts backends.
///
/// Fields define the structure of data with a name, data type, nullable flag, and optional description.
///
/// # Examples
///
/// ```
/// use aqueducts_schemas::{Field, DataType};
/// use std::str::FromStr;
///
/// // Simple field using direct construction  
/// let field = Field {
///     name: "user_id".to_string(),
///     data_type: DataType::Int64,
///     nullable: false,
///     description: Some("Unique user identifier".to_string()),
/// };
///
/// // Field with complex type from string
/// let list_field = Field {
///     name: "tags".to_string(),
///     data_type: DataType::from_str("list<string>").unwrap(),
///     nullable: true,
///     description: None,
/// };
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema_gen", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub struct Field {
    /// The name of the field
    pub name: String,

    /// The data type of the field
    /// Can be specified as a string (e.g., "int32", "string", "`list<int32>`")
    /// or as a structured object for complex types
    #[serde(alias = "type")]
    #[serde(deserialize_with = "deserialize_data_type")]
    #[cfg_attr(
        feature = "schema_gen",
        schemars(
            with = "String",
            description = "Data type specification. Examples: 'string', 'int64', 'bool', 'list<string>', 'struct<name:string,age:int32>', 'timestamp<millisecond,UTC>', 'decimal<10,2>'"
        )
    )]
    pub data_type: DataType,

    /// Whether the field can contain null values
    #[serde(default = "default_true")]
    pub nullable: bool,

    /// Optional description of what this field represents
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

/// Universal data type that can be converted to Arrow, Delta, and other formats.
///
/// DataType supports all common data types and can be parsed from user-friendly string representations.
/// This provides a unified schema definition that works across different backends.
///
/// # Examples
///
/// ```
/// use aqueducts_schemas::DataType;
/// use std::str::FromStr;
///
/// // Parse basic types from strings
/// let int_type = DataType::from_str("int32").unwrap();
/// let string_type = DataType::from_str("string").unwrap();
///
/// // Parse complex types
/// let list_type = DataType::from_str("list<int64>").unwrap();
/// let struct_type = DataType::from_str("struct<name:string,age:int32>").unwrap();
/// let decimal_type = DataType::from_str("decimal<10,2>").unwrap();
/// let timestamp_type = DataType::from_str("timestamp<millisecond,UTC>").unwrap();
/// ```
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[cfg_attr(feature = "schema_gen", derive(schemars::JsonSchema))]
pub enum DataType {
    // Primitive types
    Boolean,
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Float32,
    Float64,

    // String types
    Utf8,
    LargeUtf8,

    // Binary types
    Binary,
    LargeBinary,
    FixedSizeBinary(i32),

    // Temporal types
    Date32,
    Date64,
    Time32(TimeUnit),
    Time64(TimeUnit),
    Timestamp(TimeUnit, Option<String>), // time unit, timezone
    Duration(TimeUnit),
    Interval(IntervalUnit),

    // Decimal type
    Decimal128(u8, i8), // precision, scale
    Decimal256(u8, i8), // precision, scale

    // Nested types
    List(Box<Field>),
    LargeList(Box<Field>),
    FixedSizeList(Box<Field>, i32), // field, size
    Struct(Vec<Field>),
    Map(Box<Field>, Box<Field>, bool),  // key, value, keys_sorted
    Union(Vec<(i8, Field)>, UnionMode), // type_ids with fields, mode

    // Dictionary type
    Dictionary(Box<DataType>, Box<DataType>), // key_type, value_type
}

/// Time unit for temporal types
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema_gen", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum TimeUnit {
    #[serde(alias = "second", alias = "s")]
    Second,
    #[serde(alias = "millisecond", alias = "ms")]
    Millisecond,
    #[serde(alias = "microsecond", alias = "us")]
    Microsecond,
    #[serde(alias = "nanosecond", alias = "ns")]
    Nanosecond,
}

/// Interval unit for interval types
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema_gen", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum IntervalUnit {
    #[serde(alias = "year_month")]
    YearMonth,
    #[serde(alias = "day_time")]
    DayTime,
    #[serde(alias = "month_day_nano")]
    MonthDayNano,
}

/// Union mode
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema_gen", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum UnionMode {
    #[serde(alias = "sparse")]
    Sparse,
    #[serde(alias = "dense")]
    Dense,
}

impl Serialize for DataType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DataType::Boolean => write!(f, "bool"),
            DataType::Int8 => write!(f, "int8"),
            DataType::Int16 => write!(f, "int16"),
            DataType::Int32 => write!(f, "int32"),
            DataType::Int64 => write!(f, "int64"),
            DataType::UInt8 => write!(f, "uint8"),
            DataType::UInt16 => write!(f, "uint16"),
            DataType::UInt32 => write!(f, "uint32"),
            DataType::UInt64 => write!(f, "uint64"),
            DataType::Float32 => write!(f, "float32"),
            DataType::Float64 => write!(f, "float64"),
            DataType::Utf8 => write!(f, "string"),
            DataType::LargeUtf8 => write!(f, "large_string"),
            DataType::Binary => write!(f, "binary"),
            DataType::LargeBinary => write!(f, "large_binary"),
            DataType::FixedSizeBinary(size) => write!(f, "fixed_binary<{}>", size),
            DataType::Date32 => write!(f, "date32"),
            DataType::Date64 => write!(f, "date64"),
            DataType::Time32(unit) => write!(f, "time32<{}>", unit_to_string(unit)),
            DataType::Time64(unit) => write!(f, "time64<{}>", unit_to_string(unit)),
            DataType::Timestamp(unit, tz) => {
                if let Some(tz) = tz {
                    write!(f, "timestamp<{},{}>", unit_to_string(unit), tz)
                } else {
                    write!(f, "timestamp<{}>", unit_to_string(unit))
                }
            }
            DataType::Duration(unit) => write!(f, "duration<{}>", unit_to_string(unit)),
            DataType::Interval(unit) => write!(f, "interval<{}>", interval_unit_to_string(unit)),
            DataType::Decimal128(precision, scale) => write!(f, "decimal<{},{}>", precision, scale),
            DataType::Decimal256(precision, scale) => {
                write!(f, "decimal256<{},{}>", precision, scale)
            }
            DataType::List(field) => write!(f, "list<{}>", field.data_type),
            DataType::LargeList(field) => write!(f, "large_list<{}>", field.data_type),
            DataType::FixedSizeList(field, size) => {
                write!(f, "fixed_list<{},{}>", field.data_type, size)
            }
            DataType::Struct(fields) => {
                write!(f, "struct<")?;
                for (i, field) in fields.iter().enumerate() {
                    if i > 0 {
                        write!(f, ",")?;
                    }
                    write!(f, "{}:{}", field.name, field.data_type)?;
                }
                write!(f, ">")
            }
            DataType::Map(key, value, _) => write!(f, "map<{},{}>", key.data_type, value.data_type),
            DataType::Union(fields, mode) => {
                write!(f, "union<")?;
                match mode {
                    UnionMode::Sparse => write!(f, "sparse,")?,
                    UnionMode::Dense => write!(f, "dense,")?,
                }
                for (i, (_, field)) in fields.iter().enumerate() {
                    if i > 0 {
                        write!(f, ",")?;
                    }
                    write!(f, "{}:{}", field.name, field.data_type)?;
                }
                write!(f, ">")
            }
            DataType::Dictionary(key, value) => write!(f, "dictionary<{},{}>", key, value),
        }
    }
}

fn unit_to_string(unit: &TimeUnit) -> &'static str {
    match unit {
        TimeUnit::Second => "second",
        TimeUnit::Millisecond => "millisecond",
        TimeUnit::Microsecond => "microsecond",
        TimeUnit::Nanosecond => "nanosecond",
    }
}

fn interval_unit_to_string(unit: &IntervalUnit) -> &'static str {
    match unit {
        IntervalUnit::YearMonth => "year_month",
        IntervalUnit::DayTime => "day_time",
        IntervalUnit::MonthDayNano => "month_day_nano",
    }
}

impl FromStr for DataType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.trim();

        // Handle simple types first
        match s {
            "bool" | "boolean" => return Ok(DataType::Boolean),
            "int8" => return Ok(DataType::Int8),
            "int16" => return Ok(DataType::Int16),
            "int32" | "int" | "integer" => return Ok(DataType::Int32), // Support old "integer" format
            "int64" | "long" => return Ok(DataType::Int64),
            "uint8" => return Ok(DataType::UInt8),
            "uint16" => return Ok(DataType::UInt16),
            "uint32" => return Ok(DataType::UInt32),
            "uint64" => return Ok(DataType::UInt64),
            "float32" | "float" => return Ok(DataType::Float32),
            "float64" | "double" => return Ok(DataType::Float64), // Support old "double" format
            "string" | "utf8" => return Ok(DataType::Utf8),
            "large_string" | "large_utf8" => return Ok(DataType::LargeUtf8),
            "binary" => return Ok(DataType::Binary),
            "large_binary" => return Ok(DataType::LargeBinary),
            "date32" | "date" => return Ok(DataType::Date32), // Support old "date" format
            "date64" => return Ok(DataType::Date64),
            _ => {}
        }

        // Handle parameterized types
        if let Some(captures) = parse_parameterized_type(s) {
            let (type_name, params) = captures;

            match type_name.as_str() {
                "list" => {
                    if params.len() != 1 {
                        return Err(format!(
                            "list type requires exactly 1 parameter, got {}",
                            params.len()
                        ));
                    }
                    let inner_type = DataType::from_str(&params[0])?;
                    let field = Field {
                        name: "item".to_string(),
                        data_type: inner_type,
                        nullable: true,
                        description: None,
                    };
                    Ok(DataType::List(Box::new(field)))
                }
                "large_list" => {
                    if params.len() != 1 {
                        return Err(format!(
                            "large_list type requires exactly 1 parameter, got {}",
                            params.len()
                        ));
                    }
                    let inner_type = DataType::from_str(&params[0])?;
                    let field = Field {
                        name: "item".to_string(),
                        data_type: inner_type,
                        nullable: true,
                        description: None,
                    };
                    Ok(DataType::LargeList(Box::new(field)))
                }
                "fixed_list" => {
                    if params.len() != 2 {
                        return Err(format!(
                            "fixed_list type requires exactly 2 parameters, got {}",
                            params.len()
                        ));
                    }
                    let inner_type = DataType::from_str(&params[0])?;
                    let size: i32 = params[1]
                        .parse()
                        .map_err(|_| format!("Invalid size for fixed_list: {}", params[1]))?;
                    let field = Field {
                        name: "item".to_string(),
                        data_type: inner_type,
                        nullable: true,
                        description: None,
                    };
                    Ok(DataType::FixedSizeList(Box::new(field), size))
                }
                "fixed_binary" => {
                    if params.len() != 1 {
                        return Err(format!(
                            "fixed_binary type requires exactly 1 parameter, got {}",
                            params.len()
                        ));
                    }
                    let size: i32 = params[0]
                        .parse()
                        .map_err(|_| format!("Invalid size for fixed_binary: {}", params[0]))?;
                    Ok(DataType::FixedSizeBinary(size))
                }
                "decimal" => {
                    if params.len() != 2 {
                        return Err(format!(
                            "decimal type requires exactly 2 parameters, got {}",
                            params.len()
                        ));
                    }
                    let precision: u8 = params[0]
                        .parse()
                        .map_err(|_| format!("Invalid precision for decimal: {}", params[0]))?;
                    let scale: i8 = params[1]
                        .parse()
                        .map_err(|_| format!("Invalid scale for decimal: {}", params[1]))?;
                    Ok(DataType::Decimal128(precision, scale))
                }
                "decimal256" => {
                    if params.len() != 2 {
                        return Err(format!(
                            "decimal256 type requires exactly 2 parameters, got {}",
                            params.len()
                        ));
                    }
                    let precision: u8 = params[0]
                        .parse()
                        .map_err(|_| format!("Invalid precision for decimal256: {}", params[0]))?;
                    let scale: i8 = params[1]
                        .parse()
                        .map_err(|_| format!("Invalid scale for decimal256: {}", params[1]))?;
                    Ok(DataType::Decimal256(precision, scale))
                }
                "timestamp" => {
                    if params.is_empty() || params.len() > 2 {
                        return Err(format!(
                            "timestamp type requires 1 or 2 parameters, got {}",
                            params.len()
                        ));
                    }
                    let unit = parse_time_unit(&params[0])?;
                    let timezone = if params.len() == 2 {
                        Some(params[1].clone())
                    } else {
                        None
                    };
                    Ok(DataType::Timestamp(unit, timezone))
                }
                "time32" => {
                    if params.len() != 1 {
                        return Err(format!(
                            "time32 type requires exactly 1 parameter, got {}",
                            params.len()
                        ));
                    }
                    let unit = parse_time_unit(&params[0])?;
                    match unit {
                        TimeUnit::Second | TimeUnit::Millisecond => Ok(DataType::Time32(unit)),
                        _ => Err("time32 only supports second and millisecond units".to_string()),
                    }
                }
                "time64" => {
                    if params.len() != 1 {
                        return Err(format!(
                            "time64 type requires exactly 1 parameter, got {}",
                            params.len()
                        ));
                    }
                    let unit = parse_time_unit(&params[0])?;
                    match unit {
                        TimeUnit::Microsecond | TimeUnit::Nanosecond => Ok(DataType::Time64(unit)),
                        _ => {
                            Err("time64 only supports microsecond and nanosecond units".to_string())
                        }
                    }
                }
                "duration" => {
                    if params.len() != 1 {
                        return Err(format!(
                            "duration type requires exactly 1 parameter, got {}",
                            params.len()
                        ));
                    }
                    let unit = parse_time_unit(&params[0])?;
                    Ok(DataType::Duration(unit))
                }
                "struct" => parse_struct_type(&params.join(",")),
                "map" => {
                    if params.len() != 2 {
                        return Err(format!(
                            "map type requires exactly 2 parameters, got {}",
                            params.len()
                        ));
                    }
                    let key_type = DataType::from_str(&params[0])?;
                    let value_type = DataType::from_str(&params[1])?;
                    let key_field = Field {
                        name: "key".to_string(),
                        data_type: key_type,
                        nullable: false,
                        description: None,
                    };
                    let value_field = Field {
                        name: "value".to_string(),
                        data_type: value_type,
                        nullable: true,
                        description: None,
                    };
                    Ok(DataType::Map(
                        Box::new(key_field),
                        Box::new(value_field),
                        false,
                    ))
                }
                _ => Err(format!("Unknown parameterized type: {}", type_name)),
            }
        } else {
            Err(format!("Unknown data type: {}", s))
        }
    }
}

fn parse_time_unit(s: &str) -> Result<TimeUnit, String> {
    match s.trim() {
        "second" | "s" => Ok(TimeUnit::Second),
        "millisecond" | "ms" => Ok(TimeUnit::Millisecond),
        "microsecond" | "us" => Ok(TimeUnit::Microsecond),
        "nanosecond" | "ns" => Ok(TimeUnit::Nanosecond),
        _ => Err(format!("Unknown time unit: {}", s)),
    }
}

fn parse_struct_type(params: &str) -> Result<DataType, String> {
    let mut fields = Vec::new();
    let mut depth = 0;
    let mut current_field = String::new();

    for ch in params.chars() {
        match ch {
            '<' => {
                depth += 1;
                current_field.push(ch);
            }
            '>' => {
                depth -= 1;
                current_field.push(ch);
            }
            ',' if depth == 0 => {
                if !current_field.trim().is_empty() {
                    fields.push(parse_struct_field(&current_field)?);
                    current_field.clear();
                }
            }
            _ => {
                current_field.push(ch);
            }
        }
    }

    if !current_field.trim().is_empty() {
        fields.push(parse_struct_field(&current_field)?);
    }

    Ok(DataType::Struct(fields))
}

fn parse_struct_field(field_def: &str) -> Result<Field, String> {
    let parts: Vec<&str> = field_def.splitn(2, ':').collect();
    if parts.len() != 2 {
        return Err(format!(
            "Invalid struct field definition: {}. Expected format: name:type",
            field_def
        ));
    }

    let name = parts[0].trim().to_string();
    let data_type = DataType::from_str(parts[1].trim())?;

    Ok(Field {
        name,
        data_type,
        nullable: true,
        description: None,
    })
}

fn parse_parameterized_type(s: &str) -> Option<(String, Vec<String>)> {
    let start = s.find('<')?;
    let end = s.rfind('>')?;

    if start >= end {
        return None;
    }

    let type_name = s[..start].trim().to_string();
    let params_str = &s[start + 1..end];

    let mut params = Vec::new();
    let mut depth = 0;
    let mut current_param = String::new();

    for ch in params_str.chars() {
        match ch {
            '<' => {
                depth += 1;
                current_param.push(ch);
            }
            '>' => {
                depth -= 1;
                current_param.push(ch);
            }
            ',' if depth == 0 => {
                if !current_param.trim().is_empty() {
                    params.push(current_param.trim().to_string());
                    current_param.clear();
                }
            }
            _ => {
                current_param.push(ch);
            }
        }
    }

    if !current_param.trim().is_empty() {
        params.push(current_param.trim().to_string());
    }

    Some((type_name, params))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_data_type_parsing() {
        // Test basic type parsing
        assert_eq!(DataType::from_str("bool").unwrap(), DataType::Boolean);
        assert_eq!(DataType::from_str("int32").unwrap(), DataType::Int32);
        assert_eq!(DataType::from_str("string").unwrap(), DataType::Utf8);

        // Test complex types
        let list_type = DataType::from_str("list<int32>").unwrap();
        if let DataType::List(field) = list_type {
            assert_eq!(field.data_type, DataType::Int32);
        } else {
            panic!("Expected List type");
        }

        let struct_type = DataType::from_str("struct<name:string,age:int32>").unwrap();
        if let DataType::Struct(fields) = struct_type {
            assert_eq!(fields.len(), 2);
            assert_eq!(fields[0].name, "name");
            assert_eq!(fields[0].data_type, DataType::Utf8);
        } else {
            panic!("Expected Struct type");
        }

        // Test parameterized types
        assert_eq!(
            DataType::from_str("decimal<10,2>").unwrap(),
            DataType::Decimal128(10, 2)
        );
        assert_eq!(
            DataType::from_str("timestamp<millisecond>").unwrap(),
            DataType::Timestamp(TimeUnit::Millisecond, None)
        );
    }

    #[test]
    fn test_field_serialization() {
        let field_json = r#"{
            "name": "user_score",
            "type": "decimal<5,2>",
            "nullable": true,
            "description": "User performance score",
            "metadata": {}
        }"#;

        // Test that Field ignores unknown fields like metadata
        let field: Field = serde_json::from_str(field_json).unwrap();
        assert_eq!(field.name, "user_score");
        assert_eq!(field.data_type, DataType::Decimal128(5, 2));
        assert!(field.nullable);

        // Test round-trip serialization
        let serialized = serde_json::to_string(&field).unwrap();
        let roundtrip: Field = serde_json::from_str(&serialized).unwrap();
        assert_eq!(field.name, roundtrip.name);
        assert_eq!(field.data_type, roundtrip.data_type);
    }
}
