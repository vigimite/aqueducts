use aqueducts_schemas::{OutputType, Stage};
use datafusion::{
    datasource::MemTable,
    execution::context::{SQLOptions, SessionContext},
};
use miette::Diagnostic;
use std::sync::Arc;
use tracing::instrument;

#[derive(Debug, thiserror::Error, Diagnostic)]
pub enum StageError {
    #[error("Failed to parse SQL in stage '{name}': {error}")]
    #[diagnostic(
        code(aqueducts::stage::sql_parse_error),
        help(
            "Check your SQL syntax in '{name}' query:\n\
              • Official docs: https://datafusion.apache.org/user-guide/sql/index.html"
        )
    )]
    Sql {
        name: String,
        #[source]
        error: datafusion::error::DataFusionError,
    },

    #[error("Failed to execute stage '{name}': {error}")]
    #[diagnostic(
        code(aqueducts::stage::execution_error),
        help(
            "Stage execution failed. Common issues:\n\
              • Referenced table/source doesn't exist\n\
              • Column names don't match\n\
              • Data type incompatibilities"
        )
    )]
    Execution {
        name: String,
        #[source]
        error: datafusion::error::DataFusionError,
    },
}

/// Process a stage in the Aqueduct pipeline
/// The result of the operation will be registered within the `SessionContext` as an
/// in-memory table using the stages name as the table name
/// Does not allow for ddl/dml queries or SQL statements (e.g. SET VARIABLE, CREATE TABLE, etc.)
#[instrument(skip_all)]
pub async fn process_stage(
    ctx: Arc<SessionContext>,
    stage: Stage,
    progress_tracker: Option<Arc<dyn crate::ProgressTracker>>,
) -> Result<(), StageError> {
    let options = SQLOptions::new()
        .with_allow_ddl(false)
        .with_allow_dml(false)
        .with_allow_statements(false);

    let result = ctx
        .sql_with_options(stage.query.as_str(), options)
        .await
        .map_err(|error| StageError::Sql {
            name: stage.name.clone(),
            error,
        })?
        .cache()
        .await
        .map_err(|error| StageError::Execution {
            name: stage.name.clone(),
            error,
        })?;
    let schema = result.schema().clone();

    if stage.explain || stage.explain_analyze {
        let output_type = if stage.explain_analyze {
            OutputType::ExplainAnalyze
        } else {
            OutputType::Explain
        };

        let explain = result
            .clone()
            .explain(false, stage.explain_analyze)
            .map_err(|error| StageError::Execution {
                name: stage.name.clone(),
                error,
            })?;

        let batches = explain
            .collect()
            .await
            .map_err(|error| StageError::Execution {
                name: stage.name.clone(),
                error,
            })?;

        if let Some(tracker) = &progress_tracker {
            tracker.on_output(&stage.name, output_type, &schema, &batches);
        }
    }

    match stage.show {
        Some(0) => {
            let batches = result
                .clone()
                .limit(0, Some(500))
                .map_err(|error| StageError::Execution {
                    name: stage.name.clone(),
                    error,
                })?
                .collect()
                .await
                .map_err(|error| StageError::Execution {
                    name: stage.name.clone(),
                    error,
                })?;

            if let Some(tracker) = &progress_tracker {
                tracker.on_output(&stage.name, OutputType::Show, &schema, &batches);
            }
        }
        Some(limit) => {
            let batches = result
                .clone()
                .limit(0, Some(limit))
                .map_err(|error| StageError::Execution {
                    name: stage.name.clone(),
                    error,
                })?
                .collect()
                .await
                .map_err(|error| StageError::Execution {
                    name: stage.name.clone(),
                    error,
                })?;

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

    let partitioned =
        result
            .collect_partitioned()
            .await
            .map_err(|error| StageError::Execution {
                name: stage.name.clone(),
                error,
            })?;

    let table =
        MemTable::try_new(Arc::new(schema.as_arrow().clone()), partitioned).map_err(|error| {
            StageError::Execution {
                name: stage.name.clone(),
                error,
            }
        })?;

    ctx.register_table(stage.name.as_str(), Arc::new(table))
        .map_err(|error| StageError::Execution {
            name: stage.name.clone(),
            error,
        })?;

    Ok(())
}
