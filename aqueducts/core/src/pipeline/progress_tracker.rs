use datafusion::arrow::array::RecordBatch;
use datafusion::common::DFSchema;
use serde::{Deserialize, Serialize};
use tracing::{error, info, instrument};

use crate::model::stages::OutputType;

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

/// A trait for handling progress events and stage output during pipeline execution
pub trait ProgressTracker: Send + Sync {
    /// Called when a progress event occurs during pipeline execution
    fn on_progress(&self, event: ProgressEvent);

    /// Called when a stage produces output
    fn on_output(
        &self,
        stage_name: &str,
        output_type: OutputType,
        schema: &DFSchema,
        batches: &[RecordBatch],
    );
}

#[derive(Debug)]
pub struct LoggingProgressTracker;

impl ProgressTracker for LoggingProgressTracker {
    #[instrument(skip_all)]
    fn on_progress(&self, event: ProgressEvent) {
        match event {
            ProgressEvent::Started => {
                info!("🚀 Pipeline execution started");
            }
            ProgressEvent::SourceRegistered { name } => {
                info!("📚 Registered source: {}", name);
            }
            ProgressEvent::StageStarted {
                name,
                position,
                sub_position,
            } => {
                info!(
                    "⚙️  Processing stage: {} (position: {}, sub-position: {})",
                    name, position, sub_position
                );
            }
            ProgressEvent::StageCompleted {
                name,
                position: _,
                sub_position: _,
                duration_ms,
            } => {
                info!(
                    "✅ Completed stage: {} (took: {:.2}s)",
                    name,
                    duration_ms as f64 / 1000.0
                );
            }
            ProgressEvent::DestinationCompleted => {
                info!("📦 Data successfully written to destination");
            }
            ProgressEvent::Completed { duration_ms } => {
                info!(
                    "🎉 Pipeline execution completed (total time: {:.2}s)",
                    duration_ms as f64 / 1000.0
                );
            }
        }
    }

    #[instrument(skip_all)]
    fn on_output(
        &self,
        stage_name: &str,
        output_type: OutputType,
        schema: &DFSchema,
        batches: &[RecordBatch],
    ) {
        let output = datafusion::arrow::util::pretty::pretty_format_batches(batches);
        match (output_type, output){
            (OutputType::Show, Ok(output_str)) => info!(
                "\n📋 Table Data: {stage_name}\n───────────────────────────────────────\n{output_str}\n"
            ),
            (OutputType::ShowLimit, Ok(output_str)) => info!(
                "\n📋 Table Data (Preview): {stage_name}\n───────────────────────────────────────\n{output_str}\n"
            ),
            (OutputType::Explain, Ok(output_str)) => info!(
                "\n🔍 Query Plan: {stage_name}\n───────────────────────────────────────\n{output_str}\n"
            ),
            (OutputType::ExplainAnalyze, Ok(output_str)) => info!(
                "\n📊 Query Metrics: {stage_name}\n───────────────────────────────────────\n{output_str}\n"
            ),
            (OutputType::PrintSchema, Ok(_)) => info!(
                "\n🔢 Schema: {stage_name}\n───────────────────────────────────────\n{schema:#?}\n"
            ),
            _ => error!("❗\n Failed to produce stage output\n")
        }
    }
}
