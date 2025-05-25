use aqueducts_schemas::{OutputType, Stage};
use datafusion::{
    datasource::MemTable,
    execution::context::{SQLOptions, SessionContext},
};
use std::sync::Arc;
use tracing::instrument;

use crate::error::{AqueductsError, Result};

/// Process a stage in the Aqueduct pipeline
/// The result of the operation will be registered within the `SessionContext` as an
/// in-memory table using the stages name as the table name
/// Does not allow for ddl/dml queries or SQL statements (e.g. SET VARIABLE, CREATE TABLE, etc.)
#[instrument(skip_all, err)]
pub async fn process_stage(
    ctx: Arc<SessionContext>,
    stage: Stage,
    progress_tracker: Option<Arc<dyn crate::ProgressTracker>>,
) -> Result<()> {
    let options = SQLOptions::new()
        .with_allow_ddl(false)
        .with_allow_dml(false)
        .with_allow_statements(false);

    let result = ctx
        .sql_with_options(stage.query.as_str(), options)
        .await?
        .cache()
        .await
        .map_err(|e| {
            AqueductsError::stage(
                &stage.name,
                format!("Error occured during stage execution: {e}"),
            )
        })?;
    let schema = result.schema().clone();

    if stage.explain || stage.explain_analyze {
        let output_type = if stage.explain_analyze {
            OutputType::ExplainAnalyze
        } else {
            OutputType::Explain
        };

        let explain = result.clone().explain(false, stage.explain_analyze)?;
        let batches = explain.collect().await?;

        if let Some(tracker) = &progress_tracker {
            tracker.on_output(&stage.name, output_type, &schema, &batches);
        }
    }

    match stage.show {
        Some(0) => {
            let batches = result.clone().collect().await?;
            if let Some(tracker) = &progress_tracker {
                tracker.on_output(&stage.name, OutputType::Show, &schema, &batches);
            }
        }
        Some(limit) => {
            let batches = result.clone().limit(0, Some(limit))?.collect().await?;
            if let Some(tracker) = &progress_tracker {
                tracker.on_output(&stage.name, OutputType::ShowLimit, &schema, &batches);
            }
        }
        _ => (),
    };

    if stage.print_schema {
        if let Some(tracker) = &progress_tracker {
            let schema = result.schema();
            tracker.on_output(&stage.name, OutputType::PrintSchema, schema, &[]);
        }
    }

    let partitioned = result.collect_partitioned().await?;
    let table = MemTable::try_new(Arc::new(schema.as_arrow().clone()), partitioned)?;

    ctx.register_table(stage.name.as_str(), Arc::new(table))?;

    Ok(())
}
