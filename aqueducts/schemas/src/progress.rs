//! Progress event types for tracking pipeline execution

use serde::{Deserialize, Serialize};

/// Progress events emitted during pipeline execution
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ProgressEvent {
    /// Pipeline execution started
    Started,
    /// A source has been registered
    SourceRegistered {
        /// Name of the source
        name: String,
    },
    /// A stage has started processing
    StageStarted {
        /// Name of the stage
        name: String,
        /// Position in the stages array (outer)
        position: usize,
        /// Position in the parallel stages array (inner)
        sub_position: usize,
    },
    /// A stage has completed processing
    StageCompleted {
        /// Name of the stage
        name: String,
        /// Position in the stages array (outer)
        position: usize,
        /// Position in the parallel stages array (inner)
        sub_position: usize,
        /// Duration of the stage execution
        duration_ms: u64,
    },
    /// Data has been written to the destination
    DestinationCompleted,
    /// Pipeline execution completed
    Completed {
        /// Total duration of the pipeline execution
        duration_ms: u64,
    },
}

/// Stage output types for websocket communication
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
