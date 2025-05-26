use datafusion::arrow::array::{as_string_array, Array, ListBuilder, StringBuilder};
use datafusion::error::DataFusionError;
use datafusion::execution::FunctionRegistry;
use datafusion::logical_expr::Volatility;
use datafusion::{
    arrow::array::ArrayRef,
    arrow::datatypes::DataType,
    arrow::datatypes::Field,
    logical_expr::{create_udf, ScalarUDF},
    physical_plan::ColumnarValue,
};
use std::sync::Arc;

fn unnest_json_array_udf() -> datafusion::logical_expr::ScalarUDF {
    let fun = Arc::new(
        |args: &[ColumnarValue]| -> datafusion::error::Result<ColumnarValue> {
            assert_eq!(args.len(), 1);

            let arrays = ColumnarValue::values_to_arrays(args)?;
            let sarr = as_string_array(&arrays[0]);

            let mut builder = ListBuilder::new(StringBuilder::new());

            for i in 0..sarr.len() {
                if sarr.is_null(i) {
                    builder.append(false);
                } else {
                    let txt = sarr.value(i);
                    let v: serde_json::Value = serde_json::from_str(txt)
                        .map_err(|e| DataFusionError::Execution(e.to_string()))?;

                    if let serde_json::Value::Array(elems) = v {
                        for elem in elems {
                            let s = elem.to_string();
                            builder.values().append_value(&s);
                        }
                        builder.append(true);
                    } else {
                        return Err(DataFusionError::Execution(format!(
                            "unnest_json_array: expected JSON array, got {}",
                            v
                        )));
                    }
                }
            }

            let array = builder.finish();
            Ok(ColumnarValue::Array(Arc::new(array) as ArrayRef))
        },
    );

    create_udf(
        "unnest_json_array",
        vec![DataType::Utf8],
        DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
        Volatility::Immutable,
        fun,
    )
}

pub fn register_all(registry: &mut dyn FunctionRegistry) -> datafusion::error::Result<()> {
    let functions: Vec<Arc<ScalarUDF>> = vec![Arc::new(unnest_json_array_udf())];

    for function in functions {
        registry.register_udf(function)?;
    }

    datafusion_functions_json::register_all(registry)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::{
        arrow::array::RecordBatch, assert_batches_sorted_eq, common::DFSchema, prelude::*,
    };

    async fn prepare_df(json: &str) -> (DFSchema, Vec<RecordBatch>) {
        let ctx = SessionContext::new();
        ctx.register_udf(unnest_json_array_udf());
        let df = ctx
            .sql(&format!(
                "SELECT unnest_json_array(c) AS arr \
                 FROM (VALUES ('{}')) AS t(c)",
                json
            ))
            .await
            .unwrap();
        let schema = df.schema().clone();
        let batches = df.collect().await.unwrap();
        (schema, batches)
    }

    #[tokio::test]
    async fn test_unnest_json_array_numbers() {
        let (schema, batches) = prepare_df("[1, 2, 3]").await;

        let field = schema.field(0);
        let expected_type = DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)));
        assert_eq!(
            field.data_type(),
            &expected_type,
            "Expected return type List<item:Utf8>, got {:?}",
            field.data_type()
        );

        let expected = [
            "+-----------+",
            "| arr       |",
            "+-----------+",
            "| [1, 2, 3] |",
            "+-----------+",
        ];
        assert_batches_sorted_eq!(&expected, &batches);
    }

    #[tokio::test]
    async fn test_unnest_json_array_strings() {
        let (schema, batches) = prepare_df(r#"["foo", "bar"]"#).await;

        let field = schema.field(0);
        let expected_type = DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)));
        assert_eq!(
            field.data_type(),
            &expected_type,
            "Expected return type List<item:Utf8>, got {:?}",
            field.data_type()
        );

        let expected = [
            "+----------------+",
            "| arr            |",
            "+----------------+",
            "| [\"foo\", \"bar\"] |",
            "+----------------+",
        ];
        assert_batches_sorted_eq!(&expected, &batches);
    }

    #[tokio::test]
    async fn test_unnest_json_array_objects() {
        let (schema, batches) = prepare_df(r#"[{"x":1}, {"y":"foo"}]"#).await;

        let field = schema.field(0);
        let expected_type = DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)));
        assert_eq!(
            field.data_type(),
            &expected_type,
            "Expected return type List<item:Utf8>, got {:?}",
            field.data_type()
        );

        let expected = [
            "+------------------------+",
            "| arr                    |",
            "+------------------------+",
            "| [{\"x\":1}, {\"y\":\"foo\"}] |",
            "+------------------------+",
        ];
        assert_batches_sorted_eq!(&expected, &batches);
    }
}
