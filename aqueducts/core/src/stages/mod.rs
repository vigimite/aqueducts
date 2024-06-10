use datafusion::execution::context::{SQLOptions, SessionContext};
use deltalake::arrow::datatypes::Schema;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::{info, instrument};

pub(crate) mod error;
pub(crate) type Result<T> = core::result::Result<T, error::Error>;

/// Definition for a processing stage in an Aqueduct Pipeline
#[derive(Debug, Clone, Serialize, Deserialize, derive_new::new, JsonSchema)]
pub struct Stage {
    /// Name of the stage, used as the table name for the result of this stage
    pub name: String,

    /// SQL query that is executed against a datafusion context. Check the datafusion SQL reference for more information <https://datafusion.apache.org/user-guide/sql/index.html>
    pub query: String,

    /// When set to a value of up to `usize`, will print the result of this stage to the stdout limited by the number
    /// Set value to 0 to not limit the outputs
    pub show: Option<usize>,

    /// When set to 'true' the stage will output the query execution plan
    #[serde(default)]
    pub explain: bool,

    /// When set to 'true' the stage will output the query execution plan with added execution metrics
    #[serde(default)]
    pub explain_analyze: bool,

    /// When set to 'true' the stage will pretty print the output schema of the excuted query
    #[serde(default)]
    pub print_schema: bool,
}

/// Process a stage in the Aqueduct pipeline
/// The result of the operation will be registered within the `SessionContext` as an
/// in-memory table using the stages name as the table name
/// Does not allow for ddl/dml queries or SQL statements (e.g. SET VARIABLE, CREATE TABLE, etc.)
#[instrument(skip(ctx, stage), err)]
pub async fn process_stage(ctx: &SessionContext, stage: Stage) -> Result<()> {
    info!("Running stage '{}'", stage.name);
    let options = SQLOptions::new()
        .with_allow_ddl(false)
        .with_allow_dml(false)
        .with_allow_statements(false);

    let result = ctx.sql_with_options(stage.query.as_str(), options).await?;

    if stage.explain || stage.explain_analyze {
        println!("\n*** Stage query plan: {} ***", stage.name.as_str());
        result
            .clone()
            .explain(false, stage.explain_analyze)?
            .show()
            .await?;
        println!();
    }

    match stage.show {
        Some(0) => {
            println!("\n*** Stage output data: {} ***", stage.name.as_str());
            result.clone().show().await?;
            println!();
        }
        Some(limit) => {
            println!(
                "\n*** Stage output data (limit {limit}): {} ***",
                stage.name.as_str()
            );
            result.clone().show_limit(limit).await?;
            println!();
        }
        _ => (),
    };

    if stage.print_schema {
        println!(
            "\n*** Stage output schema: {name} ***\n{schema:#?}\n",
            name = stage.name.as_str(),
            schema = result.schema()
        );
    }

    let schema: Arc<Schema> = Arc::new(result.schema().into());
    let batch = deltalake::arrow::compute::concat_batches(&schema, result.collect().await?.iter())?;

    let _ = ctx.register_batch(stage.name.as_str(), batch)?;

    Ok(())
}
