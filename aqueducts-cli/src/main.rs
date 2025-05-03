use anyhow::{anyhow, Context};
use aqueducts::prelude::*;
use clap::{Parser, Subcommand};
use env_logger::Env;
use log::{debug, error, info, warn};
use reqwest::header::{HeaderMap, HeaderValue, CONTENT_TYPE};
use serde::Deserialize;
use serde_json::json;
use std::{
    collections::HashMap,
    error::Error,
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio_stream::StreamExt;

/// Aqueducts CLI for executing data pipelines locally or remotely
#[derive(Debug, Parser)]
#[command(name = "aqueducts", version, about, long_about = None)]
struct Args {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Run an Aqueduct pipeline locally
    Run {
        /// Path to Aqueduct configuration file
        #[arg(short, long)]
        file: PathBuf,

        /// k=v list of parameters to pass to the configuration file
        /// e.g. aqueduct run -f file.yml -p key1=value1 -p key2=value2
        #[arg(short, long, value_parser = parse_key_val::<String, String>)]
        params: Option<Vec<(String, String)>>,

        /// Execute the pipeline on a remote executor instead of locally
        #[arg(long)]
        executor: Option<String>,

        /// API key for the remote executor
        #[arg(long)]
        api_key: Option<String>,
    },

    /// Check the status of a remote executor
    Status {
        /// URL of the executor
        #[arg(short, long)]
        executor: String,

        /// API key for the remote executor
        #[arg(short, long)]
        api_key: String,
    },

    /// Cancel a running pipeline on a remote executor
    Cancel {
        /// URL of the executor
        #[arg(short, long)]
        executor: String,

        /// API key for the remote executor
        #[arg(short, long)]
        api_key: String,

        /// Execution ID to cancel (optional, will cancel current execution if not provided)
        #[arg(short, long)]
        execution_id: Option<String>,
    },
}

fn parse_key_val<T, U>(s: &str) -> Result<(T, U), Box<dyn Error + Send + Sync + 'static>>
where
    T: std::str::FromStr,
    T::Err: Error + Send + Sync + 'static,
    U: std::str::FromStr,
    U::Err: Error + Send + Sync + 'static,
{
    let pos = s
        .find('=')
        .ok_or_else(|| format!("invalid KEY=value: no `=` found in `{s}`"))?;
    Ok((s[..pos].parse()?, s[pos + 1..].parse()?))
}

/// Parse an Aqueduct pipeline from a file with the appropriate format
/// Validates file extension and feature flags
fn parse_aqueduct_file(
    file: &PathBuf,
    params: HashMap<String, String>,
) -> Result<Aqueduct, anyhow::Error> {
    // Determine file extension and validate feature support
    let ext = file
        .extension()
        .and_then(|s| s.to_str())
        .ok_or_else(|| anyhow!("Unable to determine file type: file has no extension"))?;

    debug!("Parsing file with extension: {}", ext);

    match ext {
        "toml" => {
            #[cfg(feature = "toml")]
            {
                debug!("Parsing TOML file: {}", file.display());
                Aqueduct::try_from_toml(file, params).context("failed to parse TOML file")
            }
            #[cfg(not(feature = "toml"))]
            {
                Err(anyhow!(
                    "TOML support is not enabled in this build of aqueducts-cli.\n\
                    Please reinstall with: cargo install aqueducts-cli --features toml\n\
                    Or use a supported format like YAML (default)"
                ))
            }
        }
        "json" => {
            #[cfg(feature = "json")]
            {
                debug!("Parsing JSON file: {}", file.display());
                Aqueduct::try_from_json(file, params).context("failed to parse JSON file")
            }
            #[cfg(not(feature = "json"))]
            {
                Err(anyhow!(
                    "JSON support is not enabled in this build of aqueducts-cli.\n\
                    Please reinstall with: cargo install aqueducts-cli --features json\n\
                    Or use a supported format like YAML (default)"
                ))
            }
        }
        "yml" | "yaml" => {
            #[cfg(feature = "yaml")]
            {
                debug!("Parsing YAML file: {}", file.display());
                Aqueduct::try_from_yml(file, params).context("failed to parse YAML file")
            }
            #[cfg(not(feature = "yaml"))]
            {
                Err(anyhow!(
                    "YAML support is not enabled in this build of aqueducts-cli.\n\
                    Please reinstall with: cargo install aqueducts-cli --features yaml"
                ))
            }
        }
        _ => {
            Err(anyhow!(
                "Unsupported file extension: .{}. Supported formats are: YAML (.yml, .yaml), JSON (.json), TOML (.toml)",
                ext
            ))
        }
    }
}

/// Progress tracker that outputs to stdout via the tracing crate
struct LoggingProgressTracker;

impl ProgressTracker for LoggingProgressTracker {
    fn on_progress(&self, event: ProgressEventType) {
        match event {
            ProgressEventType::Started => {
                info!("🚀 Pipeline execution started");
            }
            ProgressEventType::SourceRegistered { name } => {
                info!("📚 Registered source: {}", name);
            }
            ProgressEventType::StageStarted {
                name,
                position,
                sub_position,
            } => {
                info!(
                    "⚙️  Processing stage: {} (position: {}, sub-position: {})",
                    name, position, sub_position
                );
            }
            ProgressEventType::StageCompleted {
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
            ProgressEventType::DestinationCompleted => {
                info!("📦 Data successfully written to destination");
            }
            ProgressEventType::Completed { duration_ms } => {
                info!(
                    "🎉 Pipeline execution completed (total time: {:.2}s)",
                    duration_ms as f64 / 1000.0
                );
            }
        }
    }
}

#[derive(Debug, Deserialize)]
struct ExecutorStatusResponse {
    status: String,
    execution_id: Option<String>,
    start_time: Option<String>,
    elapsed_time: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct ExecutorEvent {
    event_type: String,
    #[serde(default)]
    message: String,
    progress: Option<u32>,
    #[allow(dead_code)]
    execution_id: Option<String>,
    current_stage: Option<String>,
    error: Option<String>,
}

#[derive(Debug, Deserialize)]
struct CancelResponse {
    status: String,
    message: String,
    cancelled_execution_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ErrorResponse {
    error: String,
    #[allow(dead_code)]
    execution_id: Option<String>,
    retry_after: Option<u64>,
}

/// Create a preconfigured HTTP client with the given API key
fn create_client(api_key: &str, timeout_secs: Option<u64>) -> Result<reqwest::Client, anyhow::Error> {
    debug!("Creating HTTP client");
    
    // Create headers with API key
    let mut headers = HeaderMap::new();
    headers.insert(
        "X-API-Key",
        HeaderValue::from_str(api_key).context("Invalid API key format")?
    );
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    
    // Configure the client builder
    let mut builder = reqwest::Client::builder()
        .default_headers(headers);
        
    // Add timeout if specified
    if let Some(secs) = timeout_secs {
        builder = builder.timeout(Duration::from_secs(secs));
    }
    
    // Build the client
    builder.build().context("Failed to create HTTP client")
}

async fn run_local(file: PathBuf, params: HashMap<String, String>) -> Result<(), anyhow::Error> {
    info!("Running pipeline locally from file: {}", file.display());

    let aqueduct = parse_aqueduct_file(&file, params)?;

    debug!("Creating SessionContext");
    let mut ctx = datafusion::prelude::SessionContext::new();
    datafusion_functions_json::register_all(&mut ctx)
        .context("Failed to register JSON functions")?;

    let progress_tracker = Arc::new(LoggingProgressTracker);

    debug!("Starting pipeline execution");
    run_pipeline(Arc::new(ctx), aqueduct, Some(progress_tracker))
        .await
        .context("Failure during execution of aqueducts file")?;

    debug!("Pipeline execution completed successfully");
    Ok(())
}

async fn run_remote(
    client: &reqwest::Client,
    file: PathBuf,
    params: HashMap<String, String>,
    executor_url: String,
) -> Result<(), anyhow::Error> {
    info!("Running pipeline remotely on executor: {}", executor_url);
    debug!("Using file: {}", file.display());

    let aqueduct = parse_aqueduct_file(&file, params)?;

    let executor_url = format!("{}/execute", executor_url.trim_end_matches('/'));
    info!("📡 Connecting to executor: {}", executor_url);

    let request_body = json!({
        "pipeline": aqueduct
    });

    debug!("Sending pipeline execution request");
    let start_time = Instant::now();
    let response = client
        .post(&executor_url)
        .json(&request_body)
        .send()
        .await
        .context("Failed to send request to executor")?;

    if !response.status().is_success() {
        let status = response.status();
        debug!("Received error status: {}", status);

        if status.as_u16() == 429 {
            // Too many requests - another pipeline is running
            let error = response
                .json::<ErrorResponse>()
                .await
                .context("Failed to parse error response")?;

            warn!(
                "Executor is busy: {}. Retry after {} seconds",
                error.error,
                error.retry_after.unwrap_or(30)
            );

            return Err(anyhow!(
                "Executor is busy: {}. Retry after {} seconds",
                error.error,
                error.retry_after.unwrap_or(30)
            ));
        } else {
            let error_text = response
                .text()
                .await
                .context("Failed to read error response")?;

            error!(
                "Failed to execute pipeline (status code: {}): {}",
                status, error_text
            );

            return Err(anyhow!(
                "Failed to execute pipeline (status code: {}): {}",
                status,
                error_text
            ));
        }
    }

    debug!("Processing SSE stream");
    let mut stream = response.bytes_stream();
    let mut last_progress = 0;

    info!("🚀 Pipeline execution started");

    while let Some(chunk_result) = stream.next().await {
        let chunk = chunk_result.context("Error reading from SSE stream")?;
        let data = String::from_utf8_lossy(&chunk);

        // Process SSE messages (could be multiple in one chunk)
        for line in data.split('\n').filter(|l| l.starts_with("data: ")) {
            let event_data = line.trim_start_matches("data: ");
            if event_data.is_empty() || event_data == "[DONE]" {
                continue;
            }

            match serde_json::from_str::<ExecutorEvent>(event_data) {
                Ok(event) => {
                    // Process event based on type
                    match event.event_type.as_str() {
                        "started" => {
                            debug!("Received started event");
                        }
                        "progress" => {
                            // Only update progress if it's changed
                            if let Some(progress) = event.progress {
                                if progress > last_progress {
                                    last_progress = progress;
                                    if let Some(stage) = &event.current_stage {
                                        info!("⚙️  Processing: {} ({}%)", stage, progress);
                                    } else {
                                        info!("⚙️  Progress: {}%", progress);
                                    }
                                }
                            }
                        }
                        "completed" => {
                            let duration = start_time.elapsed();
                            info!(
                                "🎉 Pipeline execution completed (total time: {:.2}s)",
                                duration.as_secs_f64()
                            );
                            return Ok(());
                        }
                        "error" => {
                            let error_msg =
                                event.error.unwrap_or_else(|| "Unknown error".to_string());
                            error!("Pipeline execution failed: {}", error_msg);
                            return Err(anyhow!("Pipeline execution failed: {}", error_msg));
                        }
                        "cancelled" => {
                            warn!("Pipeline execution was cancelled");
                            return Err(anyhow!("Pipeline execution was cancelled"));
                        }
                        _ => {
                            info!("📝 {}: {}", event.event_type, event.message);
                        }
                    }
                }
                Err(e) => {
                    warn!("Failed to parse event: {} ({})", event_data, e);
                }
            }
        }
    }

    error!("Stream ended unexpectedly without completion");
    Err(anyhow!("Stream ended unexpectedly"))
}

async fn check_status(client: &reqwest::Client, executor_url: String) -> Result<(), anyhow::Error> {
    info!("Checking status of executor: {}", executor_url);

    let status_url = format!("{}/status", executor_url.trim_end_matches('/'));
    debug!("Sending request to: {}", status_url);

    let response = client
        .get(&status_url)
        .send()
        .await
        .context("Failed to send status request")?;
    let status_code = response.status();

    if !status_code.is_success() {
        if status_code.as_u16() == 401 {
            error!("Authentication failed: Invalid API key");
            return Err(anyhow!("Authentication failed: Invalid API key"));
        } else {
            let error_text = response
                .text()
                .await
                .context("Failed to read error response")?;

            error!(
                "Failed to get status (status code: {}): {}",
                status_code, error_text
            );
            return Err(anyhow!(
                "Failed to get status (status code: {}): {}",
                status_code,
                error_text
            ));
        }
    }

    let status = response
        .json::<ExecutorStatusResponse>()
        .await
        .context("Failed to parse status response")?;

    info!("Executor status: {}", status.status);

    if status.status == "running" {
        if let Some(id) = &status.execution_id {
            info!("Currently running execution ID: {id}");
        }

        if let Some(start) = &status.start_time {
            info!("Started at: {start}");
        }

        if let Some(elapsed) = status.elapsed_time {
            info!("Elapsed time: {:.2}s", elapsed as f64 / 1000.0);
        }
    }

    Ok(())
}

async fn cancel_execution(
    client: &reqwest::Client,
    executor_url: String,
    execution_id: Option<String>,
) -> Result<(), anyhow::Error> {
    info!(
        "Cancelling pipeline execution on executor: {}",
        executor_url
    );
    if let Some(id) = &execution_id {
        debug!("Targeting specific execution ID: {}", id);
    } else {
        debug!("Targeting current execution (no ID specified)");
    };

    let request_body = if let Some(id) = execution_id {
        json!({ "execution_id": id })
    } else {
        json!({})
    };

    let cancel_url = format!("{}/cancel", executor_url.trim_end_matches('/'));
    debug!("Sending cancel request to: {}", cancel_url);

    let response = client
        .post(&cancel_url)
        .json(&request_body)
        .send()
        .await
        .context("Failed to send cancel request")?;
    let status_code = response.status();

    if !status_code.is_success() {
        if status_code.as_u16() == 401 {
            error!("Authentication failed: Invalid API key");
            return Err(anyhow!("Authentication failed: Invalid API key"));
        } else {
            let error_text = response
                .text()
                .await
                .context("Failed to read error response")?;

            error!(
                "Failed to cancel execution (status code: {}): {}",
                status_code, error_text
            );
            return Err(anyhow!(
                "Failed to cancel execution (status code: {}): {}",
                status_code,
                error_text
            ));
        }
    }

    // Parse response
    let cancel_response: CancelResponse = response
        .json()
        .await
        .context("Failed to parse cancel response")?;

    // Display result
    info!("Status: {}", cancel_response.status);
    info!("Message: {}", cancel_response.message);

    if let Some(id) = &cancel_response.cancelled_execution_id {
        info!("Cancelled execution ID: {}", id);
    }

    Ok(())
}

async fn check_executor(client: &reqwest::Client, url: &str) -> Result<bool, anyhow::Error> {
    debug!("Checking executor availability at: {}", url);

    // Check health endpoint first (doesn't need auth)
    let health_url = format!("{}/health", url.trim_end_matches('/'));
    debug!("Checking health endpoint: {}", health_url);

    match client.get(&health_url).send().await {
        Ok(response) => {
            if !response.status().is_success() {
                let status = response.status();
                error!("Executor health check failed with status code: {}", status);
                return Err(anyhow!(
                    "Executor health check failed with status code: {}",
                    status
                ));
            }
            debug!("Health check successful");
        }
        Err(e) => {
            error!("Failed to connect to executor: {}", e);
            return Err(anyhow!("Failed to connect to executor: {}", e));
        }
    }

    // Now check status endpoint to verify API key works
    let status_url = format!("{}/status", url.trim_end_matches('/'));
    debug!("Checking status endpoint: {}", status_url);

    match client.get(&status_url).send().await {
        Ok(response) => {
            if response.status().as_u16() == 401 {
                error!("Authentication failed: Invalid API key");
                return Err(anyhow!("Authentication failed: Invalid API key"));
            } else if !response.status().is_success() {
                let status = response.status();
                error!("Executor status check failed with status code: {}", status);
                return Err(anyhow!(
                    "Executor status check failed with status code: {}",
                    status
                ));
            }
            debug!("Status check successful");
        }
        Err(e) => {
            error!("Failed to get executor status: {}", e);
            return Err(anyhow!("Failed to get executor status: {}", e));
        }
    }

    debug!("Executor is available and API key is valid");
    Ok(true)
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let env = Env::default().default_filter_or("aqueducts=info");
    env_logger::Builder::from_env(env)
        .format_target(false)
        .format_level(false)
        .format_timestamp(None)
        .init();

    aqueducts::register_handlers();

    let args = Args::parse();

    match args.command {
        Commands::Run {
            file,
            params,
            executor,
            api_key,
        } => {
            let params = HashMap::from_iter(params.unwrap_or_default());

            // Execute either locally or remotely
            if let Some(executor_url) = executor {
                // Remote execution requires an API key
                let api_key = api_key.ok_or_else(|| {
                    error!("API key is required for remote execution");
                    anyhow!("API key is required for remote execution")
                })?;

                // Create an HTTP client with a 5-second timeout for initial connection
                let client = create_client(&api_key, Some(5))?;

                // Check if executor is reachable
                info!("Checking executor availability...");
                check_executor(&client, &executor_url)
                    .await
                    .context("Failed to connect to executor")?;
                info!("Executor is available and API key is valid");

                // Execute pipeline remotely
                run_remote(&client, file, params, executor_url).await?;
            } else {
                // Execute pipeline locally
                run_local(file, params).await?;
            }
        }
        Commands::Status { executor, api_key } => {
            // Create HTTP client
            let client = create_client(&api_key, None)?;
            
            // Check status
            check_status(&client, executor).await?;
        }
        Commands::Cancel {
            executor,
            api_key,
            execution_id,
        } => {
            // Create HTTP client
            let client = create_client(&api_key, None)?;
            
            // Cancel execution
            cancel_execution(&client, executor, execution_id).await?;
        }
    }

    Ok(())
}
