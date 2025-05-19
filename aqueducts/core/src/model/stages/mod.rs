//! Models for processing stages

use serde::{Deserialize, Serialize};

pub(crate) mod error;
pub(crate) type Result<T> = core::result::Result<T, error::Error>;

use async_trait::async_trait;
use datafusion::dataframe::DataFrame;
use datafusion::execution::context::SessionContext;
use std::sync::Arc;

use crate::execution::stages::execute_stage;
use crate::execution::traits::StageProvider;
// Removed redundant imports

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

#[async_trait]
impl StageProvider for Stage {
    async fn execute(
        &self,
        ctx: Arc<SessionContext>,
    ) -> std::result::Result<DataFrame, crate::error::Error> {
        execute_stage(ctx, self)
            .await
            .map_err(crate::error::Error::ModelStageError)
    }
}

/// Stage output types
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum OutputType {
    /// Stage outputs the full dataframe
    Show,
    /// Stage outputs up to `usize` records
    ShowLimit,
    /// Stage outputs query plan
    Explain,
    /// Stage outputs query plan with execution metrics
    ExplainAnalyze,
    /// Stage outputs the dataframe schema
    PrintSchema,
}
