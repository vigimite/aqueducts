use std::sync::Arc;
use std::{collections::HashMap, path::PathBuf, sync::atomic::AtomicBool};

use anyhow::{anyhow, Context};
use aqueducts_websockets::ExecutorMessage;
use tokio::select;
use tokio::signal::ctrl_c;
use tokio::sync::mpsc;
use tracing::{error, info};
use uuid::Uuid;

use crate::websocket_client::handlers::{print_progress_update, StageOutputBuffer};
use crate::{parse_aqueduct_file, websocket_client::WebSocketClient};

/// Execute a pipeline on a remote executor
pub async fn run_remote(
    file: PathBuf,
    params: HashMap<String, String>,
    executor_url: String,
    api_key: String,
) -> Result<(), anyhow::Error> {
    info!("Parsing pipeline from file: {}", file.display());
    let aqueduct = parse_aqueduct_file(&file, params)?;

    let client = WebSocketClient::try_new(executor_url, api_key)
        .context("failed to build websocket client")?;

    info!("Connecting to remote executor...");
    let mut receiver = client
        .connect()
        .await
        .context("Failed to connect to executor")?;

    // Set up execution cancellation handling
    let cancelled = Arc::new(AtomicBool::new(false));
    let cancelled_clone = cancelled.clone();

    tokio::spawn(async move {
        if ctrl_c().await.is_ok() {
            info!("Received Ctrl+C, cancelling execution...");
            cancelled_clone.store(true, std::sync::atomic::Ordering::SeqCst);
        }
    });

    info!("Submitting pipeline for remote execution...");
    client
        .execute_pipeline(aqueduct)
        .await
        .context("Failed to submit pipeline for execution")?;

    let mut execution_id = None;
    let mut stage_buffer = StageOutputBuffer::new();

    let (cancel_tx, mut cancel_rx) = mpsc::channel::<Uuid>(1);

    loop {
        select! {
            // Handle incoming messages from the executor
            message = receiver.recv() => {
                match message {
                    Some(ExecutorMessage::ExecutionResponse { execution_id: id }) => {
                        info!("Pipeline execution started on remote executor (ID: {})", id);
                        execution_id = Some(id);
                    }
                    Some(ExecutorMessage::QueuePosition { execution_id: _, position }) => {
                        info!("Pipeline is queued for execution (position: {})", position);
                    }
                    Some(ExecutorMessage::ProgressUpdate { execution_id: _, progress: _, event }) => {
                        print_progress_update(&event);
                    }
                    Some(ExecutorMessage::StageOutput { execution_id: _, stage_name, payload }) => {
                        let output_ready = stage_buffer.process_message(stage_name, payload);
                        if output_ready {
                            stage_buffer.print_output();
                        }
                    }
                    Some(ExecutorMessage::ExecutionSucceeded { execution_id: _ }) => {
                        info!("Pipeline execution completed successfully");
                        break;
                    }
                    Some(ExecutorMessage::ExecutionError { execution_id: _, message }) => {
                        error!("Pipeline execution failed: {}", message);
                        return Err(anyhow!("Pipeline execution failed: {}", message));
                    }
                    Some(ExecutorMessage::CancelResponse { execution_id: id }) => {
                        info!("Execution cancelled (ID: {})", id);
                        break;
                    }
                    None => {
                        error!("Lost connection to executor");
                        return Err(anyhow!("Lost connection to executor"));
                    }
                }
            }

            // Handle cancellation requests
            id = cancel_rx.recv() => {
                if let Some(id) = id {
                    info!("Cancelling execution (ID: {})...", id);
                    if let Err(e) = client.cancel_execution(id).await {
                        error!("Failed to cancel execution: {}", e);
                    }
                }
            }

            // Check for Ctrl+C cancellation
            _ = async {
                while !cancelled.load(std::sync::atomic::Ordering::SeqCst) {
                    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                }
                Ok::<_, anyhow::Error>(())
            } => {
                if let Some(id) = execution_id {
                    let _ = cancel_tx.send(id).await;
                } else {
                    error!("Cannot cancel: No execution ID available");
                    return Err(anyhow!("Execution cancelled, but no execution ID was available"));
                }
            }
        }
    }

    info!("Remote execution finished");
    Ok(())
}

/// Cancel a specific execution on a remote executor
pub async fn cancel_remote_execution(
    executor_url: String,
    api_key: String,
    execution_id: Uuid,
) -> Result<(), anyhow::Error> {
    info!(
        "Connecting to remote executor to cancel execution {}...",
        execution_id
    );

    let client = WebSocketClient::try_new(executor_url, api_key)?;

    let mut receiver = client
        .connect()
        .await
        .context("Failed to connect to executor")?;

    client
        .cancel_execution(execution_id)
        .await
        .context("Failed to send cancellation request")?;

    while let Some(message) = receiver.recv().await {
        match message {
            ExecutorMessage::CancelResponse { execution_id: id } => {
                if id == execution_id {
                    info!("Execution cancelled successfully");
                    return Ok(());
                }
            }
            ExecutorMessage::ExecutionError {
                execution_id: id,
                message,
            } => {
                if id == execution_id {
                    return Err(anyhow!("Failed to cancel execution: {}", message));
                }
            }
            _ => {}
        }
    }

    Err(anyhow!(
        "Lost connection to executor before receiving cancellation confirmation"
    ))
}
