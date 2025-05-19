use aqueducts::model::stages::OutputType;
use aqueducts::prelude::ProgressEvent;
use aqueducts::protocol::{ExecutorMessage, StageOutputMessage};
use itertools::Itertools;
use std::sync::atomic::AtomicUsize;
use tokio::runtime::Handle;
use tokio::sync::mpsc;
use tracing::{debug, error, info, instrument};
use uuid::Uuid;

/// Maximum number of characters allowed in a single stage output message
/// Messages larger than this will be split into multiple chunks
const MAX_MESSAGE_CHARS: usize = 32_000;

/// Tracks and reports pipeline execution progress and stage outputs to clients.
///
/// The `ExecutorProgressTracker` implements the `aqueducts::pipeline::progress_tracker::ProgressTracker` trait,
/// providing an executor-specific implementation that:
///
/// 1. **Progress Tracking**:
///    - Monitors execution progress through the pipeline
///    - Calculates percentage completion based on pipeline complexity
///    - Reports progress events to clients via websocket messages
///
/// 2. **Stage Output Handling**:
///    - Captures and formats outputs from pipeline stages
///    - Handles large outputs by chunking them to meet message size limits
///    - Formats different output types with appropriate presentation
///
/// 3. **Asynchronous Communication**:
///    - Sends progress and output messages asynchronously to clients
///    - Ensures executor processing is not blocked by client message handling
///    - Logs message send failures for observability
///
/// The tracker is created with a specific execution ID and total step count,
/// which are determined based on the pipeline structure (sources, stages, etc.).
///
/// # Example
///
/// This is an illustrative example (not meant to be run as a doctest):
///
///     // Example: Using the progress tracker
///     use aqueducts::prelude::ProgressEvent;
///     use tokio::sync::mpsc;
///     use uuid::Uuid;
///     use std::sync::Arc;
///
///     async fn track_progress() {
///         // Create channel for client communication
///         let (client_tx, mut client_rx) = mpsc::channel(16);
///         
///         // Generate a unique execution ID
///         let execution_id = Uuid::new_v4();
///         
///         // Calculate total steps based on pipeline structure
///         // (e.g., 5 sources + 10 stages + 1 destination = 16 steps)
///         let total_steps = 16;
///         
///         // Create the progress tracker
///         let tracker = Arc::new(ExecutorProgressTracker::new(
///             client_tx,
///             execution_id,
///             total_steps
///         ));
///         
///         // Spawn a task to process progress messages
///         tokio::spawn(async move {
///             while let Some(msg) = client_rx.recv().await {
///                 // Process progress messages here
///                 println!("Received progress message");
///             }
///         });
///         
///         // Example: Simulate a progress event
///         tracker.on_progress(ProgressEvent::SourceLoaded {
///             source_name: "customers".to_string()
///         });
///         
///         // The progress update is sent asynchronously to the client
///     }
pub struct ExecutorProgressTracker {
    /// Channel for sending messages back to clients
    client_tx: mpsc::Sender<ExecutorMessage>,
    /// The unique identifier of the execution being tracked
    execution_id: Uuid,
    /// Total number of steps expected in the pipeline execution
    total_steps: usize,
    /// Counter for completed steps (atomic for thread safety)
    completed_steps: AtomicUsize,
}

impl ExecutorProgressTracker {
    /// Creates a new tracker for monitoring pipeline execution progress.
    ///
    /// # Arguments
    /// * `client_tx` - Channel for sending messages back to the client
    /// * `execution_id` - Unique identifier for the execution being tracked
    /// * `total_steps` - Total number of steps in the pipeline execution, calculated
    ///   based on the number of sources, stages, and destinations in the pipeline
    pub fn new(
        client_tx: mpsc::Sender<ExecutorMessage>,
        execution_id: Uuid,
        total_steps: usize,
    ) -> Self {
        info!(
            execution_id = %execution_id,
            total_steps = total_steps,
            "Creating executor progress tracker"
        );
        Self {
            client_tx,
            execution_id,
            total_steps,
            completed_steps: AtomicUsize::new(0),
        }
    }

    /// Calculates the percentage of completion based on current progress.
    ///
    /// This method converts the current step count into a percentage (0-100)
    /// based on the total expected steps, rounding down to a whole number.
    ///
    /// # Arguments
    /// * `current` - The current step number (1-based)
    ///
    /// # Returns
    /// A percentage (0-100) as a u8 representing execution progress
    fn calculate_progress(&self, current: usize) -> u8 {
        let progress = ((current as f32) / (self.total_steps as f32) * 100.0) as u8;
        debug!(
            execution_id = %self.execution_id,
            current_step = current,
            total_steps = self.total_steps,
            progress = progress,
            "Calculated execution progress"
        );
        progress
    }

    /// Sends a message to the client asynchronously without blocking the executor.
    ///
    /// This helper method spawns a new task to send the message, ensuring that
    /// the pipeline execution is not blocked while waiting for client communication.
    /// The method includes proper error handling and logging for message delivery.
    ///
    /// # Arguments
    /// * `message` - The ExecutorMessage to send to the client
    fn send_message(&self, message: ExecutorMessage) {
        let tx = self.client_tx.clone();
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

/// Implementation of the aqueducts_core::ProgressTracker trait.
///
/// This trait implementation connects the executor's progress tracking system with the
/// core pipeline execution engine in the aqueducts-core crate. The aqueducts-core
/// execution engine calls these methods to report progress and output data.
///
/// The implementation here transforms the core progress events into protocol messages
/// that can be sent to clients via WebSocket connections.
impl aqueducts::pipeline::progress_tracker::ProgressTracker for ExecutorProgressTracker {
    /// Processes a progress event from the pipeline execution and reports it to the client.
    ///
    /// This method is called by the Aqueduct core engine whenever a significant
    /// execution milestone is reached (e.g., source loaded, stage started, etc.).
    /// It:
    /// 1. Increments the completed steps counter atomically
    /// 2. Calculates the current progress percentage
    /// 3. Creates and sends a progress update message to the client
    ///
    /// # Arguments
    /// * `event` - The progress event describing what milestone was reached
    #[instrument(skip(self, event), fields(execution_id = %self.execution_id))]
    fn on_progress(&self, event: ProgressEvent) {
        debug!("Processing progress event");

        let current = self
            .completed_steps
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
            + 1;
        let progress = self.calculate_progress(current);

        let message = ExecutorMessage::ProgressUpdate {
            execution_id: self.execution_id,
            progress,
            event,
        };

        // Send the progress update via the channel
        self.send_message(message);
    }

    /// Processes and formats stage output data and sends it to the client.
    ///
    /// This method is called by the Aqueduct core engine when a stage produces
    /// output data (e.g., show(), explain(), etc.). It:
    /// 1. Creates a formatted header based on the output type
    /// 2. Formats the record batches into a pretty-printed table
    /// 3. Splits large outputs into chunks to meet message size limits
    /// 4. Sends the output as a sequence of messages (start, chunks, end)
    ///
    /// # Arguments
    /// * `stage_name` - Name of the stage producing the output
    /// * `output_type` - Type of output (Show, Explain, etc.)
    /// * `schema` - DataFusion schema for the output data
    /// * `batches` - Arrow record batches containing the actual data
    #[instrument(skip(self, schema, batches), fields(execution_id = %self.execution_id, stage = %stage_name, output_type = ?output_type))]
    fn on_output(
        &self,
        stage_name: &str,
        output_type: aqueducts::model::stages::OutputType,
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

        // Send the output header
        self.send_message(ExecutorMessage::StageOutput {
            execution_id: self.execution_id,
            stage_name: stage_name.to_string(),
            payload: StageOutputMessage::OutputStart { output_header },
        });

        // Format the record batches into a pretty table
        let output = match datafusion::arrow::util::pretty::pretty_format_batches(batches) {
            Ok(output) => output,
            Err(e) => {
                error!(error = %e, "Failed to format stage output");
                return;
            }
        };

        // Split large outputs into chunks to meet message size limits
        let output_str = output.to_string();
        let chunks = chunk_by_chars(&output_str, MAX_MESSAGE_CHARS);

        info!(
            chunk_count = chunks.len(),
            total_size = output_str.len(),
            "Chunking stage output"
        );

        // Send each output chunk as a separate message
        for (sequence, chunk) in chunks.into_iter().enumerate() {
            debug!(
                sequence = sequence,
                chunk_size = chunk.len(),
                "Sending output chunk"
            );

            self.send_message(ExecutorMessage::StageOutput {
                execution_id: self.execution_id,
                stage_name: stage_name.to_string(),
                payload: StageOutputMessage::OutputChunk {
                    sequence,
                    body: chunk,
                },
            });
        }

        // Send the output end marker
        self.send_message(ExecutorMessage::StageOutput {
            execution_id: self.execution_id,
            stage_name: stage_name.to_string(),
            payload: StageOutputMessage::OutputEnd {
                output_footer: String::from(""),
            },
        });

        debug!("Stage output processing complete");
    }
}

/// Splits a string into chunks of specified maximum character count.
///
/// This utility function is used to split large output strings into smaller
/// chunks that can be sent as individual messages, ensuring we don't exceed
/// message size limits. It preserves Unicode character boundaries correctly
/// by operating on character iterators rather than byte slices.
///
/// # Arguments
/// * `s` - The input string to split into chunks
/// * `max_chars` - Maximum number of characters per chunk
///
/// # Returns
/// A vector of strings, each containing at most max_chars characters
fn chunk_by_chars(s: &str, max_chars: usize) -> Vec<String> {
    s.chars()
        .chunks(max_chars)
        .into_iter()
        .map(|chunk| chunk.collect())
        .collect()
}
