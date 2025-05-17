use aqueducts::prelude::{ProgressEvent, ProgressTracker};
use aqueducts::progress_tracker::OutputType;
use aqueducts_websockets::{Outgoing, StageOutputMessage};
use itertools::Itertools;
use std::sync::atomic::AtomicUsize;
use tokio::runtime::Handle;
use tokio::sync::mpsc;
use tracing::{debug, error, info, instrument};
use uuid::Uuid;

const MAX_MESSAGE_CHARS: usize = 32_000;

/// Implementation of ProgressTracker for the executor
pub struct ExecutorProgressTracker {
    progress_tx: mpsc::Sender<Outgoing>,
    execution_id: Uuid,
    total_steps: usize,
    completed_steps: AtomicUsize,
}

impl ExecutorProgressTracker {
    pub fn new(tx: mpsc::Sender<Outgoing>, execution_id: Uuid, total_steps: usize) -> Self {
        info!(
            execution_id = %execution_id,
            total_steps = total_steps,
            "Creating executor progress tracker"
        );
        Self {
            progress_tx: tx,
            execution_id,
            total_steps,
            completed_steps: AtomicUsize::new(0),
        }
    }

    /// Calculate progress percentage based on completed steps
    fn calculate_progress(&self, current: usize) -> u8 {
        let progress = 10 + ((current as f32) / (self.total_steps as f32) * 100.0) as u8;
        debug!(
            execution_id = %self.execution_id,
            current_step = current,
            total_steps = self.total_steps,
            progress = progress,
            "Calculated execution progress"
        );
        progress
    }

    /// Helper to send a message asynchronously
    fn send_message(&self, message: Outgoing) {
        let tx = self.progress_tx.clone();
        let execution_id = self.execution_id;

        Handle::current().spawn(async move {
            debug!(execution_id = %execution_id, "Sending progress message");
            match tx.send(message).await {
                Ok(_) => debug!(execution_id = %execution_id, "Progress message sent successfully"),
                Err(e) => error!(execution_id = %execution_id, error = %e, "Failed to send progress message"),
            }
        });
    }
}

impl ProgressTracker for ExecutorProgressTracker {
    #[instrument(skip(self, event), fields(execution_id = %self.execution_id))]
    fn on_progress(&self, event: ProgressEvent) {
        debug!("Processing progress event");

        let current = self
            .completed_steps
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
            + 1;
        let progress = self.calculate_progress(current);

        let message = Outgoing::ProgressUpdate {
            execution_id: self.execution_id,
            progress,
            event,
        };

        // Send the progress update via the channel
        self.send_message(message);
    }

    #[instrument(skip(self, schema, batches), fields(execution_id = %self.execution_id, stage = %stage_name, output_type = ?output_type))]
    fn on_output(
        &self,
        stage_name: &str,
        output_type: aqueducts::progress_tracker::OutputType,
        schema: &datafusion::common::DFSchema,
        batches: &[datafusion::arrow::array::RecordBatch],
    ) {
        debug!("Processing stage output");

        // Generate output header based on type
        let output_header = match output_type {
            OutputType::Show => {
                format!("\n📋 Table Data: {stage_name}\n───────────────────────────────────────\n")
            }
            OutputType::ShowLimit => format!(
                "\n📋 Table Data (Preview): {stage_name}\n───────────────────────────────────────\n"
            ),
            OutputType::Explain => {
                format!("\n🔍 Query Plan: {stage_name}\n───────────────────────────────────────\n")
            }
            OutputType::ExplainAnalyze => format!(
                "\n📊 Query Metrics: {stage_name}\n───────────────────────────────────────\n"
            ),
            OutputType::PrintSchema => format!(
                "\n🔢 Schema: {stage_name}\n───────────────────────────────────────\n{schema:#?}\n"
            ),
        };

        // Send output start message
        self.send_message(Outgoing::StageOutput {
            execution_id: self.execution_id,
            stage_name: stage_name.to_string(),
            payload: StageOutputMessage::OutputStart { output_header },
        });

        // Format and split output
        let output = match datafusion::arrow::util::pretty::pretty_format_batches(batches) {
            Ok(output) => output,
            Err(e) => {
                error!(error = %e, "Failed to format stage output");
                return;
            }
        };

        let output_str = output.to_string();
        let chunks = chunk_by_chars(&output_str, MAX_MESSAGE_CHARS);

        info!(
            chunk_count = chunks.len(),
            total_size = output_str.len(),
            "Chunking stage output"
        );

        // Send each chunk
        for (sequence, chunk) in chunks.into_iter().enumerate() {
            debug!(
                sequence = sequence,
                chunk_size = chunk.len(),
                "Sending output chunk"
            );

            self.send_message(Outgoing::StageOutput {
                execution_id: self.execution_id,
                stage_name: stage_name.to_string(),
                payload: StageOutputMessage::OutputChunk {
                    sequence,
                    body: chunk,
                },
            });
        }

        // Send output end message
        self.send_message(Outgoing::StageOutput {
            execution_id: self.execution_id,
            stage_name: stage_name.to_string(),
            payload: StageOutputMessage::OutputEnd {
                output_footer: "".to_string(),
            },
        });

        debug!("Stage output processing complete");
    }
}

fn chunk_by_chars(s: &str, max_chars: usize) -> Vec<String> {
    s.chars()
        .chunks(max_chars) // now available!
        .into_iter()
        .map(|chunk| chunk.collect())
        .collect()
}
