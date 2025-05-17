use datafusion::{
    datasource::MemTable,
    execution::context::{SQLOptions, SessionContext},
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::instrument;

use crate::OutputType;

pub(crate) mod error;
pub(crate) type Result<T> = core::result::Result<T, error::Error>;

/// Definition for a processing stage in an Aqueduct Pipeline
#[derive(Debug, Clone, Serialize, Deserialize, derive_new::new)]
#[cfg_attr(feature = "schema_gen", derive(schemars::JsonSchema))]
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

    /// When set to 'true' the stage will pretty print the output schema of the executed query
    #[serde(default)]
    pub print_schema: bool,
}

/// Process a stage in the Aqueduct pipeline
/// The result of the operation will be registered within the `SessionContext` as an
/// in-memory table using the stages name as the table name
/// Does not allow for ddl/dml queries or SQL statements (e.g. SET VARIABLE, CREATE TABLE, etc.)
#[instrument(skip_all, err)]
pub async fn process_stage(
    ctx: Arc<SessionContext>,
    stage: Stage,
    progress_tracker: Option<&Arc<dyn crate::ProgressTracker>>,
) -> Result<()> {
    let options = SQLOptions::new()
        .with_allow_ddl(false)
        .with_allow_dml(false)
        .with_allow_statements(false);

    let result = ctx
        .sql_with_options(stage.query.as_str(), options)
        .await?
        .cache()
        .await?;
    let schema = result.schema().clone();

    if stage.explain || stage.explain_analyze {
        let output_type = if stage.explain_analyze {
            OutputType::ExplainAnalyze
        } else {
            OutputType::Explain
        };

        let explain = result.clone().explain(false, stage.explain_analyze)?;
        let batches = explain.collect().await?;

        if let Some(tracker) = progress_tracker {
            tracker.on_output(&stage.name, output_type, &schema, &batches);
        }
    }

    match stage.show {
        Some(0) => {
            let batches = result.clone().collect().await?;
            if let Some(tracker) = progress_tracker {
                tracker.on_output(&stage.name, OutputType::Show, &schema, &batches);
            }
        }
        Some(limit) => {
            let batches = result.clone().limit(0, Some(limit))?.collect().await?;
            if let Some(tracker) = progress_tracker {
                tracker.on_output(&stage.name, OutputType::ShowLimit, &schema, &batches);
            }
        }
        _ => (),
    };

    if stage.print_schema {
        if let Some(tracker) = progress_tracker {
            let schema = result.schema();
            tracker.on_output(&stage.name, OutputType::PrintSchema, &schema, &[]);
        }
    }

    let partitioned = result.collect_partitioned().await?;
    let table = MemTable::try_new(Arc::new(schema.as_arrow().clone()), partitioned)?;

    ctx.register_table(stage.name.as_str(), Arc::new(table))?;

    Ok(())
}
