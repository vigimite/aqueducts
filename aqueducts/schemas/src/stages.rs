//! Stage configuration types and schemas.
//!
//! Stages define SQL transformations that are executed as part of an aqueducts pipeline.
//! Each stage produces a named table that can be referenced by subsequent stages.

use bon::Builder;
use serde::{Deserialize, Serialize};

/// A processing stage in an aqueducts pipeline.
///
/// Stages execute SQL queries against the available data sources and previous stage results.
/// Each stage creates a named table that can be referenced by subsequent stages.
///
/// # Examples
///
/// ```
/// use aqueducts_schemas::Stage;
///
/// // Basic stage - debug fields default to false, show defaults to None
/// let stage = Stage::builder()
///     .name("aggregated_sales".to_string())
///     .query("SELECT region, SUM(amount) as total FROM sales GROUP BY region".to_string())
///     .build();
///
/// // Stage with output shown
/// let debug_stage = Stage::builder()
///     .name("debug_query".to_string())
///     .query("SELECT * FROM source LIMIT 5".to_string())
///     .show(10)
///     .build();
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, Builder)]
#[cfg_attr(feature = "schema_gen", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub struct Stage {
    /// Name of the stage, used as the table name for the result of this stage
    pub name: String,

    /// SQL query that is executed against a datafusion context. Check the datafusion SQL reference for more information <https://datafusion.apache.org/user-guide/sql/index.html>
    pub query: String,

    /// When set to a value of up to `usize`, will print the result of this stage to the stdout limited by the number
    /// Set value to 0 for unlimited output (capped at 500 rows to avoid terminal overflow)
    #[serde(default)]
    pub show: Option<usize>,

    /// When set to 'true' the stage will output the query execution plan
    #[serde(default)]
    #[builder(default)]
    pub explain: bool,

    /// When set to 'true' the stage will output the query execution plan with added execution metrics
    #[serde(default)]
    #[builder(default)]
    pub explain_analyze: bool,

    /// When set to 'true' the stage will pretty print the output schema of the executed query
    #[serde(default)]
    #[builder(default)]
    pub print_schema: bool,
}
