use std::sync::Arc;

use aqueducts::protocol::ExecutorMessage;
use aqueducts::Aqueduct;
use datafusion::{execution::runtime_env::RuntimeEnvBuilder, prelude::SessionContext};
use futures::future::BoxFuture;
use tokio::sync::mpsc;
use tracing::{error, info, instrument};
use uuid::Uuid;

pub use manager::ExecutionManager;
pub use progress_tracker::ExecutorProgressTracker;

/// The executor module provides the core functionality for running Aqueduct pipelines.
///
/// This module is responsible for:
/// - Managing pipeline execution with progress tracking
/// - Handling concurrent execution requests through a queue system
/// - Providing real-time feedback on execution progress
/// - Resource management including memory limits
///
/// The main components are:
/// - `ExecutionManager`: Coordinates execution of multiple queued pipelines
/// - `ExecutionQueue`: Manages the queue of pending executions
/// - `ExecutorProgressTracker`: Tracks and reports progress during pipeline execution
/// - `execute_pipeline`: Core function that runs a single pipeline with proper setup
///
/// # Integration with Other Crates
///
/// ## aqueducts-core
///
/// The executor module uses the `aqueducts-core` crate to:
/// - Execute Aqueduct pipeline definitions (`Aqueduct` struct)
/// - Run the actual pipeline logic via `run_pipeline`
/// - Track execution progress using the `ProgressTracker` trait
/// - Access the pipeline models and definitions from `model` module
///
/// ## aqueducts-protocol
///
/// The executor module communicates with clients using message types from `aqueducts-protocol`:
/// - `ExecutorMessage`: Messages sent from executor to client (progress updates, results, etc.)
/// - `StageOutputMessage`: Messages containing stage output data
///
/// # Threading and Concurrency
///
/// The executor module uses Tokio's asynchronous runtime for concurrency:
/// - `ExecutionManager` uses a single-permit semaphore to ensure only one pipeline executes at a time
/// - `ExecutionQueue` manages the queue of pending executions
/// - Progress and stage output are sent through Tokio channels
/// - Cancellation is handled via `CancellationToken` from tokio-util
mod manager;
mod progress_tracker;
mod queue;

/// Update message broadcast to clients when queue positions change.
///
/// This struct is sent via a broadcast channel to all clients that are
/// monitoring a specific execution, allowing them to track their position
/// in the execution queue.
#[derive(Debug, Clone)]
pub struct QueueUpdate {
    /// Unique identifier of the execution this update relates to
    pub execution_id: Uuid,
    /// Current position in the queue (0 = next to execute)
    pub position: usize,
}

/// Represents a pending or active Aqueduct pipeline execution.
///
/// This struct combines the unique identifier for the execution with
/// the actual Future that will execute the pipeline when run. The
/// Future is pinned to enable it to be stored in collections and
/// passed between components.
pub struct Execution {
    /// Unique identifier for this execution
    pub id: Uuid,
    /// Future that will be executed to run the pipeline
    pub handler: BoxFuture<'static, ()>,
}

/// Execute a single Aqueduct pipeline and communicate progress back to clients.
///
/// This function handles the complete lifecycle of a pipeline execution:
/// 1. Setting up the execution environment with optional memory limits
/// 2. Configuring the DataFusion execution context
/// 3. Creating a progress tracker to report execution events
/// 4. Running the pipeline and handling success/failure outcomes
/// 5. Sending result messages back to the client
///
/// The function sets up proper tracing and logging for observability.
///
/// # Arguments
/// * `execution_id` - Unique identifier for this execution
/// * `client_tx` - Channel for sending progress and result messages to clients
/// * `pipeline` - The Aqueduct pipeline definition to execute
/// * `max_memory_gb` - Optional memory limit in gigabytes (None = unlimited)
///
/// # Example
///
/// This is an illustrative example showing how to use this function
/// (not meant to be run as a doctest):
///
///     // Example: executing a pipeline
///     use aqueducts::{Aqueduct, YamlLoader};
///     use tokio::sync::mpsc;
///     use uuid::Uuid;
///     use std::fs;
///
///     async fn run_pipeline() -> Result<(), Box<dyn std::error::Error>> {
///         // Generate a unique ID for this execution
///         let execution_id = Uuid::new_v4();
///
///         // Create a channel for progress updates
///         let (client_tx, mut client_rx) = mpsc::channel(32);
///
///         // Load a pipeline from a YAML file
///         let yaml_content = fs::read_to_string("examples/aqueduct_pipeline_example.yml")?;
///         let pipeline = YamlLoader::load_from_str(&yaml_content)?;
///
///         // Spawn a task to process progress updates
///         tokio::spawn(async move {
///             while let Some(message) = client_rx.recv().await {
///                 // Process execution messages here
///                 println!("Received execution message");
///             }
///         });
///
///         // Execute the pipeline with a 4GB memory limit
///         execute_pipeline(execution_id, client_tx, pipeline, Some(4)).await;
///
///         Ok(())
///     }
#[instrument(skip(client_tx, pipeline), fields(source_count = pipeline.sources.len(), stage_count = pipeline.stages.len()))]
pub async fn execute_pipeline(
    execution_id: Uuid,
    client_tx: mpsc::Sender<ExecutorMessage>,
    pipeline: Aqueduct,
    max_memory_gb: Option<usize>,
) {
    info!(execution_id = %execution_id, "Starting pipeline execution setup");

    let mut ctx = if let Some(memory_gb) = max_memory_gb {
        // Convert max_memory_gb directly to bytes (GB * 1024^3)
        let max_memory_bytes = memory_gb * 1024 * 1024 * 1024;

        info!(
            execution_id = %execution_id,
            memory_gb = memory_gb,
            memory_bytes = max_memory_bytes,
            "Creating runtime environment with memory limit"
        );

        // Use 0.95 as the memory use percentage (allowing 95% of the limit to be used)
        let runtime_env = match RuntimeEnvBuilder::new()
            .with_memory_limit(max_memory_bytes, 0.95)
            .build_arc()
        {
            Ok(env) => env,
            Err(e) => {
                error!(execution_id = %execution_id, error = %e, "Failed to build runtime environment");
                let _ = client_tx
                    .send(ExecutorMessage::ExecutionError {
                        execution_id,
                        message: format!("Failed to build runtime environment: {}", e),
                    })
                    .await;
                return;
            }
        };

        let config = datafusion::execution::config::SessionConfig::new();
        SessionContext::new_with_config_rt(config, runtime_env)
    } else {
        info!(execution_id = %execution_id, "Using session with unlimited memory allocation");
        SessionContext::new()
    };

    datafusion_functions_json::register_all(&mut ctx).expect("Failed to register JSON functions");

    let num_sources = pipeline.sources.len();
    let num_stages = pipeline
        .stages
        .iter()
        .map(|s| s.len())
        .reduce(|acc, e| acc + e)
        .unwrap_or(0);
    let num_destinations = pipeline.destination.is_some() as usize;

    let total_steps = num_sources
        + num_stages * 2 // 2 progress events per stage (started, completed)
        + num_destinations;

    info!(
        execution_id = %execution_id,
        total_steps = total_steps,
        "Creating progress tracker"
    );

    let progress_tracker = Arc::new(ExecutorProgressTracker::new(
        client_tx.clone(),
        execution_id,
        total_steps,
    ));

    info!(execution_id = %execution_id, "Starting pipeline execution");
    let result =
        aqueducts::pipeline::run_pipeline(Arc::new(ctx), pipeline, Some(progress_tracker)).await;

    match result {
        Ok(_) => {
            info!(execution_id = %execution_id, "Pipeline executed successfully");
            if let Err(e) = client_tx
                .send(ExecutorMessage::ExecutionSucceeded { execution_id })
                .await
            {
                error!(
                    execution_id = %execution_id,
                    error = %e,
                    "Failed to send error message to client"
                );
            }
        }
        Err(error) => {
            error!(execution_id = %execution_id, error = %error, "Pipeline execution failed");
            if let Err(e) = client_tx
                .send(ExecutorMessage::ExecutionError {
                    execution_id,
                    message: error.to_string(),
                })
                .await
            {
                error!(
                    execution_id = %execution_id,
                    error = %e,
                    "Failed to send error message to client"
                );
            }
        }
    }
}
