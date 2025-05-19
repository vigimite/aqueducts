use anyhow::anyhow;
use aqueducts::prelude::ProgressEvent;
use aqueducts_websockets::{ClientMessage, ExecutorMessage, StageOutputMessage};
use futures_util::{SinkExt, StreamExt};
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{client::IntoClientRequest, http::HeaderValue, protocol::Message},
};
use tracing::{debug, error, info};
use url::Url;
use uuid::Uuid;

/// The custom header for API key authentication
const X_API_KEY_HEADER: &str = "X-API-Key";

/// Manages connection to an executor server
pub struct WebSocketClient {
    executor_url: Url,
    api_key: String,
    sender: Arc<Mutex<Option<mpsc::Sender<ClientMessage>>>>,
}

impl WebSocketClient {
    /// Create a new client
    pub fn try_new(executor_url: String, api_key: String) -> anyhow::Result<Self> {
        let executor_url = Url::parse(&format!("ws://{executor_url}/ws/connect"))?;
        Ok(Self {
            executor_url,
            api_key,
            sender: Arc::new(Mutex::new(None)),
        })
    }

    /// Connect to the executor and set up message handling
    pub async fn connect(&self) -> anyhow::Result<mpsc::Receiver<ExecutorMessage>> {
        info!("Connecting to executor at: {}", self.executor_url);

        // Set up channels for message passing
        let (outgoing_tx, mut outgoing_rx) = mpsc::channel::<ClientMessage>(16);
        let (incoming_tx, incoming_rx) = mpsc::channel::<ExecutorMessage>(32);

        debug!("Connecting with API key authentication");
        let mut request = self.executor_url.clone().into_client_request()?;
        request
            .headers_mut()
            .insert(X_API_KEY_HEADER, HeaderValue::from_str(&self.api_key)?);

        let (ws_stream, _) = connect_async(request).await?;
        debug!("WebSocket connection established");

        let (mut ws_sender, mut ws_receiver) = ws_stream.split();
        {
            let mut sender = self.sender.lock().await;
            *sender = Some(outgoing_tx);
        }

        // Handle outgoing messages
        tokio::spawn(async move {
            while let Some(message) = outgoing_rx.recv().await {
                match serde_json::to_string(&message) {
                    Ok(json) => {
                        debug!("Sending message: {}", json);
                        if let Err(e) = ws_sender.send(Message::Text(json)).await {
                            error!("Error sending message: {}", e);
                            break;
                        }
                    }
                    Err(e) => {
                        error!("Failed to serialize message: {}", e);
                    }
                }
            }
            debug!("Outgoing message handler finished");
        });

        // Handle incoming messages
        tokio::spawn(async move {
            while let Some(msg) = ws_receiver.next().await {
                match msg {
                    Ok(Message::Text(text)) => {
                        debug!("Received message: {}", text);
                        match serde_json::from_str::<ExecutorMessage>(&text) {
                            Ok(message) => {
                                if let Err(e) = incoming_tx.send(message).await {
                                    error!("Failed to forward incoming message: {}", e);
                                    break;
                                }
                            }
                            Err(e) => {
                                error!("Failed to parse message: {}", e);
                            }
                        }
                    }
                    Ok(Message::Close(_)) => {
                        info!("WebSocket connection closed by server");
                        break;
                    }
                    Err(e) => {
                        error!("Error receiving message: {}", e);
                        break;
                    }
                    _ => {} // Ignore other message types
                }
            }
            debug!("Incoming message handler finished");
        });

        // Return the receiver channel
        Ok(incoming_rx)
    }

    /// Submit a pipeline for execution
    pub async fn execute_pipeline(&self, pipeline: aqueducts::Aqueduct) -> anyhow::Result<Uuid> {
        // Send execution request
        self.send_message(ClientMessage::ExecutionRequest { pipeline })
            .await?;

        // The response will be handled by the caller through the receiver channel
        // The actual execution ID will come back in an ExecutionResponse message
        Ok(Uuid::nil()) // Placeholder, actual ID will be returned by caller
    }

    /// Cancel an execution
    pub async fn cancel_execution(&self, execution_id: Uuid) -> anyhow::Result<()> {
        self.send_message(ClientMessage::CancelRequest { execution_id })
            .await
    }

    /// Send a message to the executor
    async fn send_message(&self, message: ClientMessage) -> anyhow::Result<()> {
        let sender = self.sender.lock().await;
        match &*sender {
            Some(tx) => {
                tx.send(message).await?;
                Ok(())
            }
            None => Err(anyhow!("Connection Closed")),
        }
    }
}

/// Helper functions for processing incoming messages
pub mod handlers {
    use super::*;

    pub fn print_progress_update(event: &ProgressEvent) {
        match event {
            ProgressEvent::Started => {
                info!("🚀 Pipeline execution started");
            }
            ProgressEvent::SourceRegistered { name } => {
                info!("📚 Registered source: {name}");
            }
            ProgressEvent::StageStarted {
                name,
                position,
                sub_position,
            } => {
                info!(
                    "⚙️  Processing stage: {name} (position: {position}, sub-position: {sub_position})",
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
                    *duration_ms as f64 / 1000.0
                );
            }
            ProgressEvent::DestinationCompleted => {
                info!("📦 Data successfully written to destination");
            }
            ProgressEvent::Completed { duration_ms } => {
                info!(
                    "🎉 Pipeline execution completed (total time: {:.2}s)",
                    *duration_ms as f64 / 1000.0
                );
            }
        }
    }

    /// Buffer for accumulating stage output chunks
    #[derive(Debug, Default)]
    pub struct StageOutputBuffer {
        current_stage: Option<String>,
        chunks: Vec<(usize, String)>,
        header: Option<String>,
        footer: Option<String>,
    }

    impl StageOutputBuffer {
        /// Create a new buffer
        pub fn new() -> Self {
            Self::default()
        }

        /// Process a stage output message
        pub fn process_message(&mut self, stage_name: String, payload: StageOutputMessage) -> bool {
            if self
                .current_stage
                .as_ref()
                .is_some_and(|s| *s != stage_name)
            {
                self.current_stage = Some(stage_name.clone());
                self.chunks.clear();
                self.header = None;
                self.footer = None;
            }

            match payload {
                StageOutputMessage::OutputStart { output_header } => {
                    self.header = Some(output_header);
                }
                StageOutputMessage::OutputChunk { sequence, body } => {
                    self.chunks.push((sequence, body));
                }
                StageOutputMessage::OutputEnd { output_footer } => {
                    self.footer = Some(output_footer);
                    return true; // Signal that we're ready to print
                }
            }

            false
        }

        /// Print the accumulated output
        pub fn print_output(&mut self) {
            if let (Some(header), Some(footer)) = (&self.header, &self.footer) {
                self.chunks.sort_by_key(|(seq, _)| *seq);

                let joined = self
                    .chunks
                    .iter()
                    .map(|(_, c)| c.as_str())
                    .collect::<Vec<&str>>()
                    .join("\n");

                info!("{header}{joined}{footer}\n");

                self.chunks.clear();
                self.header = None;
                self.footer = None;
            } else {
                error!("Failed to build stage output. Skipping...")
            }
        }
    }
}
