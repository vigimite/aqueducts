//! Protocol types for websocket communication between client and executor.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{Aqueduct, ProgressEvent};

/// Stage output sent down to clients
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum StageOutputMessage {
    /// Stage output is being streamed to client
    OutputStart {
        output_header: String,
    },
    /// Stage output content
    OutputChunk {
        /// Indicates the sequence of this chunk output
        sequence: usize,
        /// Output chunk body
        body: String,
    },
    OutputEnd {
        output_footer: String,
    },
}

/// Client websocket message
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
#[allow(clippy::large_enum_variant)]
pub enum ClientMessage {
    /// Execution requested by client
    ExecutionRequest {
        /// The aqueducts pipeline to be executed
        pipeline: Aqueduct,
    },
    /// Execution cancellation requested by client
    CancelRequest {
        /// Execution id of the pipeline execution to cancel
        execution_id: Uuid,
    },
}

/// Executor websocket message
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ExecutorMessage {
    /// Execution successfully queued
    ExecutionResponse {
        /// Execution id that identifies the queued execution
        execution_id: Uuid,
    },
    /// Execution cancellation was successful
    CancelResponse {
        /// Execution id of the cancelled pipeline
        execution_id: Uuid,
    },
    /// The queue position for the requested execution
    QueuePosition {
        /// Execution id of the queued pipeline
        execution_id: Uuid,
        /// Position of the requested execution in the queue
        position: usize,
    },
    /// Progress update event emited by a running aqueducts pipeline
    ProgressUpdate {
        /// Execution id of the running pipeline
        execution_id: Uuid,
        /// Progress percentage (0-100)
        progress: u8,
        /// Progress event payload
        event: ProgressEvent,
    },
    /// Stage output of a running pipeline
    StageOutput {
        /// Execution id of the running pipeline
        execution_id: Uuid,
        /// Stage name that is outputting
        stage_name: String,
        /// Stage output payload
        payload: StageOutputMessage,
    },
    /// Pipeline execution completet successfully
    ExecutionSucceeded {
        /// Execution id of the pipeline
        execution_id: Uuid,
    },
    ExecutionError {
        /// Execution id that produced error
        execution_id: Uuid,
        /// Error message
        message: String,
    },
}
