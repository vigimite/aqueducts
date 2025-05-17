use anyhow::anyhow;
use aqueducts::prelude::ProgressEvent;
use aqueducts_websockets::{Incoming, Outgoing, StageOutputMessage};
use futures_util::{SinkExt, StreamExt};
use indicatif::{ProgressBar, ProgressStyle};
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
    executor_url: String,
    api_key: String,
    sender: Arc<Mutex<Option<mpsc::Sender<Incoming>>>>,
}

impl WebSocketClient {
    /// Create a new client
    pub fn new(executor_url: String, api_key: String) -> Self {
        Self {
            executor_url,
            api_key,
            sender: Arc::new(Mutex::new(None)),
        }
    }

    /// Connect to the executor and set up message handling
    pub async fn connect(&self) -> anyhow::Result<mpsc::Receiver<Outgoing>> {
        // Ensure we have the full path to the WebSocket endpoint
        let connect_url = if self.executor_url.ends_with("/api/ws/connect") {
            self.executor_url.clone()
        } else if self.executor_url.ends_with("/api/ws") {
            format!("{}/connect", self.executor_url)
        } else if self.executor_url.ends_with("/api") {
            format!("{}/ws/connect", self.executor_url)
        } else if self.executor_url.ends_with("/") {
            format!("{}api/ws/connect", self.executor_url)
        } else {
            format!("{}/api/ws/connect", self.executor_url)
        };
        info!("Connecting to executor at: {}", connect_url);

        // Set up channels for message passing
        let (outgoing_tx, mut outgoing_rx) = mpsc::channel::<Incoming>(16);
        let (incoming_tx, incoming_rx) = mpsc::channel::<Outgoing>(32);

        // Parse the URL for proper WebSocket connection
        let url = Url::parse(&connect_url)?;

        debug!("Connecting with API key authentication");

        // Build a request with custom headers
        let mut request = url.into_client_request()?;

        // Add the API key header
        request
            .headers_mut()
            .insert(X_API_KEY_HEADER, HeaderValue::from_str(&self.api_key)?);

        // Connect using the request with API key
        let (ws_stream, _) = connect_async(request).await?;
        debug!("WebSocket connection established");

        let (mut ws_sender, mut ws_receiver) = ws_stream.split();

        // Store sender channel for later use
        {
            let mut sender = self.sender.lock().await;
            *sender = Some(outgoing_tx);
        }

        // Handle outgoing messages
        let incoming_tx_clone = incoming_tx.clone();
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

            // Notify client that connection was closed
            let _ = incoming_tx_clone
                .send(Outgoing::ExecutionError {
                    execution_id: Uuid::nil(), // We don't know the execution ID at this point
                    message: "WebSocket connection closed".to_string(),
                })
                .await;
        });

        // Handle incoming messages
        tokio::spawn(async move {
            while let Some(msg) = ws_receiver.next().await {
                match msg {
                    Ok(Message::Text(text)) => {
                        debug!("Received message: {}", text);
                        match serde_json::from_str::<Outgoing>(&text) {
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

    /// Send a message to the executor
    pub async fn send_message(&self, message: Incoming) -> anyhow::Result<()> {
        let sender = self.sender.lock().await;
        match &*sender {
            Some(tx) => {
                tx.send(message).await?;
                Ok(())
            }
            None => Err(anyhow!("Connection Closed")),
        }
    }

    /// Submit a pipeline for execution
    pub async fn execute_pipeline(&self, pipeline: aqueducts::Aqueduct) -> anyhow::Result<Uuid> {
        // Send execution request
        self.send_message(Incoming::ExecutionRequest { pipeline })
            .await?;

        // The response will be handled by the caller through the receiver channel
        // The actual execution ID will come back in an ExecutionResponse message
        Ok(Uuid::nil()) // Placeholder, actual ID will be returned by caller
    }

    /// Cancel an execution
    pub async fn cancel_execution(&self, execution_id: Uuid) -> anyhow::Result<()> {
        self.send_message(Incoming::CancelRequest { execution_id })
            .await
    }
}

/// Helper functions for processing incoming messages
pub mod handlers {
    use super::*;
    use std::sync::OnceLock;

    // Global progress bar that can be reused
    static PROGRESS_BAR: OnceLock<ProgressBar> = OnceLock::new();

    // Initialize the progress bar if it hasn't been initialized yet
    fn get_progress_bar() -> &'static ProgressBar {
        PROGRESS_BAR.get_or_init(|| {
            // Create a multi-line progress bar that stays at the bottom of the screen
            let pb = ProgressBar::new(100);
            pb.set_style(
                ProgressStyle::default_bar()
                    .template("{spinner:.green} [{bar:40.cyan/blue}] {percent:>3}% {msg}")
                    .unwrap()
                    .progress_chars("█▓▒░  "),
            );
            // Set the bar as steady (fixed position)
            pb.enable_steady_tick(std::time::Duration::from_millis(120));
            // Use the `.with_position()` variant of functions when possible
            pb.set_message("Pipeline execution");

            // Make the progress bar print at a fixed position
            pb.set_draw_target(indicatif::ProgressDrawTarget::stderr_with_hz(30));

            pb
        })
    }

    /// Print a progress update to the console with progress bar
    pub fn print_progress_update(event: &ProgressEvent, progress: u8) {
        // Get the progress bar and update it
        let pb = get_progress_bar();
        pb.set_position(progress as u64);

        // Set the message based on the event, without printing duplicate info logs
        match event {
            ProgressEvent::Started => {
                pb.println("🚀 Pipeline execution started");
                pb.set_message("Pipeline execution in progress");
            }
            ProgressEvent::SourceRegistered { name } => {
                let msg = format!("📚 Registered source: {}", name);
                pb.println(&msg);
                pb.set_message(format!("Processing source: {}", name));
            }
            ProgressEvent::StageStarted {
                name,
                position,
                sub_position,
            } => {
                let msg = format!(
                    "⚙️  Processing stage: {} (position: {}, sub-position: {})",
                    name, position, sub_position
                );
                pb.println(&msg);
                pb.set_message(format!("⚙️  Processing: {}", name));
            }
            ProgressEvent::StageCompleted {
                name,
                position: _,
                sub_position: _,
                duration_ms,
            } => {
                let msg = format!(
                    "✅ Completed stage: {} (took: {:.2}s)",
                    name,
                    *duration_ms as f64 / 1000.0
                );
                pb.println(&msg);
                pb.set_message(format!("Last completed: {}", name));
            }
            ProgressEvent::DestinationCompleted => {
                pb.println("📦 Data successfully written to destination");
                pb.set_message("Writing to destination completed");
            }
            ProgressEvent::Completed { duration_ms } => {
                let msg = format!(
                    "🎉 Pipeline execution completed (total time: {:.2}s)",
                    *duration_ms as f64 / 1000.0
                );
                pb.println(&msg);
                pb.finish_with_message(format!(
                    "✅ Pipeline completed in {:.2}s",
                    *duration_ms as f64 / 1000.0
                ));
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
            // If we get a new stage, reset our buffer
            if self.current_stage.as_ref() != Some(&stage_name) {
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
                // Sort chunks by sequence number
                self.chunks.sort_by_key(|(seq, _)| *seq);

                // Join chunks into a single string with indentation
                let joined = self
                    .chunks
                    .iter()
                    .map(|(_, c)| {
                        // Add indentation to each line in the chunk
                        c.lines()
                            .map(|line| format!("    {}", line))
                            .collect::<Vec<String>>()
                            .join("\n")
                    })
                    .collect::<Vec<String>>()
                    .join("\n");

                // Get the progress bar and print the output through it
                let pb = get_progress_bar();

                // Use the progress bar's println to print output above the progress bar
                pb.println(&format!("{header}{joined}{footer}\n"));

                // Clear buffer
                self.chunks.clear();
                self.header = None;
                self.footer = None;
            }
        }
    }
}
