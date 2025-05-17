mod manager;
mod progress_tracker;
mod queue;

use std::sync::Arc;

use aqueducts::Aqueduct;
use aqueducts_websockets::Outgoing;
use datafusion::{execution::runtime_env::RuntimeEnvBuilder, prelude::SessionContext};
use futures::future::BoxFuture;
use tokio::sync::mpsc;
use tracing::{error, info, instrument};
use uuid::Uuid;

pub use manager::ExecutionManager;
pub use progress_tracker::ExecutorProgressTracker;

/// Broadcast when queue positions change
#[derive(Debug, Clone)]
pub struct QueueUpdate {
    pub execution_id: Uuid,
    pub position: usize,
}

/// An aqueduct pipeline execution
pub struct Execution {
    pub id: Uuid,
    pub handler: BoxFuture<'static, ()>,
}

#[instrument(skip(progress_tx, pipeline), fields(source_count = pipeline.sources.len(), stage_count = pipeline.stages.len()))]
pub async fn execute_pipeline(
    execution_id: Uuid,
    progress_tx: mpsc::Sender<Outgoing>,
    pipeline: Aqueduct,
    max_memory_gb: Option<u32>,
) {
    info!(execution_id = %execution_id, "Starting pipeline execution setup");

    let mut ctx = if let Some(memory_gb) = max_memory_gb.clone() {
        // Convert max_memory_gb directly to bytes (GB * 1024^3)
        let max_memory_bytes = (memory_gb as usize) * 1024 * 1024 * 1024;

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
                let _ = progress_tx
                    .send(Outgoing::ExecutionError {
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

    let total_steps = pipeline.sources.len()
        + pipeline
            .stages
            .iter()
            .map(|s| s.len() + 1)
            .reduce(|acc, e| acc + e)
            .unwrap_or(0)
            * 2 // 2 progress events per stage (started, completed)
        + pipeline.destination.is_some() as usize;

    info!(
        execution_id = %execution_id,
        total_steps = total_steps,
        "Creating progress tracker"
    );

    let progress_tracker = Arc::new(ExecutorProgressTracker::new(
        progress_tx.clone(),
        execution_id,
        total_steps,
    ));

    info!(execution_id = %execution_id, "Starting pipeline execution");
    let result = aqueducts::run_pipeline(Arc::new(ctx), pipeline, Some(progress_tracker)).await;

    match result {
        Ok(_) => {
            info!(execution_id = %execution_id, "Pipeline executed successfully");
            if let Err(e) = progress_tx
                .send(Outgoing::ExecutionSucceeded { execution_id })
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
            if let Err(e) = progress_tx
                .send(Outgoing::ExecutionError {
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
