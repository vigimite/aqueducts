#[derive(
    Debug, Clone, serde::Serialize, serde::Deserialize, derive_new::new, schemars::JsonSchema,
)]
pub struct TableSchema {
    columns: Vec<TableColumn>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, derive_new::new)]
pub struct TableColumn {
    pub name: String,
    pub data_type: datafusion::arrow::datatypes::DataType,
}

impl From<TableSchema> for datafusion::arrow::datatypes::Schema {
    fn from(value: TableSchema) -> Self {
        let fields = value
            .columns
            .into_iter()
            .map(|f| datafusion::arrow::datatypes::Field::new(f.name, f.data_type, false))
            .collect::<Vec<datafusion::arrow::datatypes::Field>>();
        datafusion::arrow::datatypes::Schema::new(fields)
    }
}

impl TryFrom<TableSchema> for deltalake::kernel::StructType {
    type Error = deltalake::errors::DeltaTableError;

    fn try_from(value: TableSchema) -> Result<Self, Self::Error> {
        let mut fields = vec![];

        for field in value.columns.into_iter() {
            let data_type = deltalake::kernel::DataType::try_from(&field.data_type)?;

            fields.push(deltalake::kernel::StructField::new(
                field.name, data_type, false,
            ));
        }

        Ok(deltalake::kernel::StructType::from_iter(fields))
    }
}

impl schemars::JsonSchema for TableColumn {
    fn schema_name() -> String {
        "TableColumn".into()
    }

    fn json_schema(_gen: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
        // let discriminator = serde_json::json!({
        //     "propertyName": "item_type",
        //     "mapping": {
        //         "table": "#/components/schemas/TableItem",
        //         "chart": "#/components/schemas/ChartItem",
        //     }
        // });

        // let schema_object = schemars::schema::SchemaObject {
        //     subschemas: Some(Box::new(subschemas)),
        //     extensions: BTreeMap::from_iter(vec![("discriminator".to_owned(), discriminator)]),
        //     ..Default::default()
        // };
        // schemars::schema::Schema::Object(schema_object)
        todo!()
    }
}
// "Stage": {
//   "description": "Definition for a processing stage in an Aqueduct Pipeline",
//   "type": "object",
//   "required": [
//     "name",
//     "query"
//   ],
//   "properties": {
//     "name": {
//       "description": "Name of the stage, used as the table name for the result of this stage",
//       "type": "string"
//     },
//     "query": {
//       "description": "SQL query that is executed against a datafusion context",
//       "type": "string"
//     },
//     "show": {
//       "description": "When set to a value of up to `usize`, will print the result of this stage to the stdout limited by the number Set value to 0 to not limit the outputs",
//       "type": [
//         "integer",
//         "null"
//       ],
//       "format": "uint",
//       "minimum": 0.0
//     },
//     "explain": {
//       "description": "When set to 'true' the stage will output the query execution plan",
//       "default": false,
//       "type": "boolean"
//     },
//     "explain_analyze": {
//       "description": "When set to 'true' the stage will output the query execution plan with added execution metrics",
//       "default": false,
//       "type": "boolean"
//     },
//     "print_schema": {
//       "description": "When set to 'true' the stage will pretty print the output schema of the excuted query",
//       "default": false,
//       "type": "boolean"
//     }
//   }
// },
