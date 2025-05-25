use aqueducts_schemas::{OutputType, ProgressEvent};
use datafusion::arrow::array::RecordBatch;
use datafusion::common::DFSchema;
use tracing::{error, info, instrument};

/// A trait for handling progress events and stage output during pipeline execution.
///
/// Implement this trait to create custom progress tracking and monitoring for
/// Aqueducts pipeline execution. This allows you to:
///
/// - Monitor pipeline progress in real-time
/// - Capture and display stage outputs
/// - Send progress updates to external systems
/// - Build custom UIs for pipeline monitoring
///
/// # Examples
///
/// ## Basic Custom Progress Tracker
///
/// ```rust
/// use aqueducts_core::progress_tracker::ProgressTracker;
/// use aqueducts_schemas::{ProgressEvent, OutputType};
/// use datafusion::arrow::array::RecordBatch;
/// use datafusion::common::DFSchema;
///
/// struct MyCustomTracker {
///     start_time: std::time::Instant,
/// }
///
/// impl MyCustomTracker {
///     fn new() -> Self {
///         Self {
///             start_time: std::time::Instant::now(),
///         }
///     }
/// }
///
/// impl ProgressTracker for MyCustomTracker {
///     fn on_progress(&self, event: ProgressEvent) {
///         match event {
///             ProgressEvent::Started => {
///                 println!("Pipeline started at {:?}", self.start_time);
///             }
///             ProgressEvent::SourceRegistered { name } => {
///                 println!("Source '{}' registered", name);
///             }
///             ProgressEvent::StageCompleted { name, duration_ms, .. } => {
///                 println!("Stage '{}' completed in {}ms", name, duration_ms);
///             }
///             ProgressEvent::Completed { duration_ms } => {
///                 println!("Pipeline completed in {}ms", duration_ms);
///             }
///             _ => {}
///         }
///     }
///
///     fn on_output(
///         &self,
///         stage_name: &str,
///         output_type: OutputType,
///         _schema: &DFSchema,
///         batches: &[RecordBatch],
///     ) {
///         let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();
///         println!("Stage '{}' produced {} rows ({:?})", stage_name, row_count, output_type);
///     }
/// }
/// ```
pub trait ProgressTracker: Send + Sync {
    /// Called when a progress event occurs during pipeline execution.
    ///
    /// This method receives various types of progress events:
    /// - `Started` - Pipeline execution has begun
    /// - `SourceRegistered` - A data source has been registered
    /// - `StageStarted` - A processing stage has started
    /// - `StageCompleted` - A processing stage has finished
    /// - `DestinationCompleted` - Data has been written to destination
    /// - `Completed` - Entire pipeline has finished
    ///
    /// # Arguments
    ///
    /// * `event` - The progress event that occurred
    fn on_progress(&self, event: ProgressEvent);

    /// Called when a stage produces output that should be displayed or captured.
    ///
    /// This method is called for stages that use output directives like `show`,
    /// `explain`, or `print_schema`. It allows you to capture and process the
    /// results of these operations.
    ///
    /// # Arguments
    ///
    /// * `stage_name` - Name of the stage producing output
    /// * `output_type` - Type of output (Show, Explain, etc.)
    /// * `schema` - Schema of the data being output
    /// * `batches` - The actual data batches to display
    fn on_output(
        &self,
        stage_name: &str,
        output_type: OutputType,
        schema: &DFSchema,
        batches: &[RecordBatch],
    );
}

/// A simple progress tracker that logs progress events and stage output using the `tracing` crate.
///
/// This is the default progress tracker provided by Aqueducts. It logs all progress events
/// and stage outputs using structured logging with emoji icons for better readability.
///
/// # Examples
///
/// ```rust,no_run
/// use aqueducts_core::{run_pipeline, progress_tracker::LoggingProgressTracker, templating::TemplateLoader};
/// use aqueducts_schemas::Aqueduct;
/// use datafusion::prelude::SessionContext;
/// use std::sync::Arc;
///
/// async fn example() -> Result<(), Box<dyn std::error::Error>> {
///     let pipeline = Aqueduct::from_file("pipeline.yml", Default::default())?;
///     let ctx = Arc::new(SessionContext::new());
///     let tracker = Arc::new(LoggingProgressTracker);
///     
///     // This will log progress events as the pipeline executes
///     let _result = run_pipeline(ctx, pipeline, Some(tracker)).await?;
///     
///     Ok(())
/// }
/// ```
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
