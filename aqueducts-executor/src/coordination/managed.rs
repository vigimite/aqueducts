use std::sync::Arc;
use std::time::Duration;

use aqueducts::prelude::{ClientMessage, ExecutorMessage};
use futures::{SinkExt, StreamExt};
use serde_json;
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, error, info};

use crate::config::Config;
use crate::execution::ExecutionManager;

pub struct OrchestratorHandler {
    config: Config,
    manager: Arc<ExecutionManager>,
}

impl OrchestratorHandler {
    pub fn new(config: Config, manager: Arc<ExecutionManager>) -> Self {
        Self { config, manager }
    }

    pub async fn start(&self) {
        loop {
            match self.connect_to_orchestrator().await {
                Ok(_) => {
                    info!("Orchestrator connection ended, attempting to reconnect");
                }
                Err(e) => {
                    error!("Failed to connect to orchestrator: {}", e);
                }
            }

            info!("Waiting 5 seconds before reconnecting to orchestrator");
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    }

    async fn connect_to_orchestrator(&self) -> Result<(), Box<dyn std::error::Error>> {
        let orchestrator_url = self
            .config
            .orchestrator_url
            .as_ref()
            .ok_or("Missing orchestrator URL")?;

        let ws_url = format!("ws://{}/ws/executor/register", orchestrator_url);
        info!(url = %ws_url, "Connecting to orchestrator");

        let (ws_stream, _) = connect_async(&ws_url).await?;
        let (mut ws_sender, mut ws_receiver) = ws_stream.split();

        info!(executor_id = %self.config.executor_id, "Connected to orchestrator, sending registration");

        let registration_msg = ExecutorMessage::RegisterExecutor {
            executor_id: self.config.executor_id,
        };

        let registration_json = serde_json::to_string(&registration_msg)?;
        ws_sender
            .send(Message::Text(registration_json.into()))
            .await?;

        let (manager_tx, mut manager_rx) = mpsc::unbounded_channel();

        // Task to forward messages from manager to orchestrator
        let ws_sender = Arc::new(tokio::sync::Mutex::new(ws_sender));
        let ws_sender_clone = ws_sender.clone();
        let manager_forward_task = tokio::spawn(async move {
            while let Some(msg) = manager_rx.recv().await {
                let json = match serde_json::to_string(&msg) {
                    Ok(json) => json,
                    Err(e) => {
                        error!("Failed to serialize message: {}", e);
                        continue;
                    }
                };

                if let Err(e) = ws_sender_clone
                    .lock()
                    .await
                    .send(Message::Text(json.into()))
                    .await
                {
                    error!("Failed to send message to orchestrator: {}", e);
                    break;
                }
            }
        });

        // Handle incoming messages from orchestrator
        while let Some(msg_result) = ws_receiver.next().await {
            match msg_result {
                Ok(Message::Text(text)) => {
                    debug!(msg_len = text.len(), "Received message from orchestrator");

                    match serde_json::from_str::<ClientMessage>(&text) {
                        Ok(ClientMessage::RegistrationResponse { success, message }) => {
                            if success {
                                info!("Successfully registered with orchestrator");
                                if let Some(msg) = message {
                                    info!("Registration message: {}", msg);
                                }
                            } else {
                                error!(
                                    "Registration rejected by orchestrator: {}",
                                    message.unwrap_or_else(|| "No reason provided".to_string())
                                );
                                break;
                            }
                        }
                        Ok(ClientMessage::ExecutionRequest { pipeline }) => {
                            info!(
                                source_count = pipeline.sources.len(),
                                stage_count = pipeline.stages.len(),
                                "Received execution request from orchestrator"
                            );

                            // Submit execution to manager
                            let manager_tx_clone = manager_tx.clone();
                            let (execution_id, mut queue_rx, mut progress_rx) = self
                                .manager
                                .submit(move |execution_id, client_tx| {
                                    Box::pin(async move {
                                        crate::execution::execute_pipeline(
                                            execution_id,
                                            client_tx,
                                            pipeline,
                                            None, // TODO: Get max_memory from config
                                        )
                                        .await
                                    })
                                })
                                .await;

                            info!(execution_id = %execution_id, "Execution submitted to queue");

                            // Forward queue updates to orchestrator
                            let queue_tx = manager_tx_clone.clone();
                            tokio::spawn(async move {
                                while let Ok(update) = queue_rx.recv().await {
                                    if update.execution_id == execution_id {
                                        let msg = ExecutorMessage::QueuePosition {
                                            execution_id: update.execution_id,
                                            position: update.position,
                                        };
                                        if queue_tx.send(msg).is_err() {
                                            break;
                                        }
                                    }
                                }
                            });

                            // Forward progress updates to orchestrator
                            let progress_tx = manager_tx_clone;
                            tokio::spawn(async move {
                                while let Some(progress) = progress_rx.recv().await {
                                    if progress_tx.send(progress).is_err() {
                                        break;
                                    }
                                }
                            });
                        }
                        Ok(ClientMessage::CancelRequest { execution_id }) => {
                            info!(execution_id = %execution_id, "Received cancellation request from orchestrator");
                            self.manager.cancel(execution_id).await;
                        }
                        Err(e) => {
                            error!("Failed to parse message from orchestrator: {}", e);
                        }
                    }
                }
                Ok(Message::Close(_)) => {
                    info!("Orchestrator closed connection");
                    break;
                }
                Ok(message) => {
                    info!("Ignoring message: {message:?}");
                    // Ignore other message types (ping/pong handled automatically by tungstenite)
                }
                Err(e) => {
                    error!("WebSocket error: {}", e);
                    break;
                }
            }
        }

        // Clean up manager forward task
        manager_forward_task.abort();

        Ok(())
    }
}
