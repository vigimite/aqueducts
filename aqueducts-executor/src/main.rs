//! # Aqueducts Executor
//!
//! The Aqueducts Executor is a standalone service that runs Aqueduct data pipelines remotely.
//! It provides a WebSocket API for clients to submit pipeline execution requests, monitor
//! execution progress, and receive results.
//!
//! ## Key Features
//!
//! - **Remote Pipeline Execution**: Runs Aqueduct data pipelines on a separate server
//! - **Execution Queue Management**: Handles multiple pipeline execution requests with queue management
//! - **Real-time Progress Tracking**: Provides detailed execution progress updates to clients
//! - **Resource Management**: Configurable memory limits for pipeline execution
//! - **Cancellation Support**: Allows graceful cancellation of running or queued pipelines
//! - **Authentication**: API key-based authentication for secure access
//!
//! ## Architecture
//!
//! The executor consists of the following main components:
//!
//! - **API Server**: WebSocket-based API for client communication (in the `api` module)
//! - **Execution Engine**: Core pipeline execution functionality (in the `executor` module)
//! - **Configuration**: Server and execution configuration (in the `config` module)
//!
//! ## Crate Organization
//!
//! - **api/**: HTTP and WebSocket API for client communication
//!   - **auth.rs**: API key authentication middleware
//!   - **mod.rs**: API routes and WebSocket message handling
//! - **executor/**: Pipeline execution engine
//!   - **manager.rs**: Manages concurrent execution with queuing
//!   - **queue.rs**: Queue implementation for pending executions
//!   - **progress_tracker.rs**: Tracks and reports pipeline execution progress
//! - **config.rs**: Configuration management
//! - **error.rs**: Error types and handling
//! - **main.rs**: Application entry point and server setup
//!
//! ## Integration with Other Crates
//!
//! - **aqueducts-core**: Core pipeline execution functionality
//! - **aqueducts-protocol**: Message types for client-server communication
//!
//! See the module-level documentation for more details on each component.

use std::{net::SocketAddr, str::FromStr, sync::Arc, time::Duration};

use axum::Router;
use clap::Parser;
use config::Config;
use executor::ExecutionManager;
use tokio::signal;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, Level};
use tracing_subscriber::{fmt, prelude::*, EnvFilter};
use uuid::Uuid;

mod api;
mod config;
mod error;
mod executor;

/// Remote executor for Aqueducts data pipeline framework
#[derive(Debug, Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    /// API key for authentication
    #[arg(long, env = "AQUEDUCTS_API_KEY")]
    api_key: String,

    /// Host address to bind to
    #[arg(long, env = "AQUEDUCTS_HOST", default_value = "0.0.0.0")]
    host: String,

    /// Port to listen on
    #[arg(long, env = "AQUEDUCTS_PORT", default_value = "3031")]
    port: u16,

    /// Maximum memory usage in GB (optional)
    #[arg(long, env = "AQUEDUCTS_MAX_MEMORY")]
    max_memory: Option<usize>,

    /// URL of Aqueducts server for registration (optional)
    #[arg(long, env = "AQUEDUCTS_SERVER_URL")]
    server_url: Option<String>,

    /// Unique identifier for this executor (optional)
    #[arg(long, env = "AQUEDUCTS_EXECUTOR_ID")]
    executor_id: Option<Uuid>,

    /// Logging level (info, debug, trace)
    #[arg(long, env = "AQUEDUCTS_LOG_LEVEL", default_value = "info")]
    log_level: String,
}

type ApiContextRef = Arc<ApiContext>;

pub struct ApiContext {
    pub config: Config,
    pub manager: Arc<ExecutionManager>,
}

impl ApiContext {
    pub fn new(config: Config) -> Self {
        Self {
            config,
            manager: Arc::new(ExecutionManager::new(100)),
        }
    }
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    let log_level = Level::from_str(cli.log_level.to_lowercase().as_str()).unwrap_or(Level::INFO);
    tracing_subscriber::registry()
        .with(
            fmt::layer()
                .json()
                .with_current_span(true)
                .with_span_list(true)
                .with_target(true),
        )
        .with(EnvFilter::from_default_env().add_directive(log_level.into()))
        .init();

    info!("Registering Aqueducts handlers");
    aqueducts::register_handlers();

    let executor_id = cli.executor_id.unwrap_or_else(Uuid::new_v4);
    info!(
        executor_id = %executor_id,
        version = %env!("CARGO_PKG_VERSION"),
        "Starting Aqueducts Executor"
    );

    let config = match Config::try_new(cli.api_key, executor_id, cli.max_memory) {
        Ok(config) => config,
        Err(e) => {
            error!("Configuration error: {}", e);
            std::process::exit(1);
        }
    };

    info!(
        executor_id = %config.executor_id,
        max_memory_gb = ?config.max_memory_gb,
        "Configuration validated successfully"
    );

    let context = Arc::new(ApiContext::new(config));

    // Create shutdown signal handler
    let shutdown_token = CancellationToken::new();
    let shutdown_token_ = shutdown_token.clone();

    // Spawn a task to handle shutdown signals
    tokio::spawn(async move {
        handle_shutdown_signals(shutdown_token_).await;
    });

    // Start the execution manager
    let manager_handle = {
        let manager = context.manager.clone();
        tokio::spawn(async move {
            manager.start().await;
        })
    };

    let app = Router::new()
        .merge(api::router(Arc::clone(&context)))
        .with_state(context);

    let addr: SocketAddr = match format!("{}:{}", cli.host, cli.port).parse() {
        Ok(addr) => addr,
        Err(e) => {
            error!("Failed to parse socket address: {}", e);
            std::process::exit(1);
        }
    };

    info!(addr = %addr, "Listening for connections");
    let listener = match tokio::net::TcpListener::bind(addr).await {
        Ok(listener) => listener,
        Err(e) => {
            error!("Failed to bind to address {}: {}", addr, e);
            std::process::exit(1);
        }
    };

    info!("Server started, press Ctrl+C to stop");
    let server_handle = axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal_handler(shutdown_token))
        .await;

    match server_handle {
        Ok(_) => info!("Server shut down gracefully"),
        Err(e) => error!(error = %e, "Server error during shutdown"),
    }

    info!("Forcing shutdown of the execution manager");
    drop(manager_handle);

    info!("Aqueducts executor shutdown complete");
}

/// Handler function for shutdown signals
async fn handle_shutdown_signals(shutdown_token: CancellationToken) {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("Failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("Failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {
            info!("Received Ctrl+C, starting graceful shutdown");
        },
        _ = terminate => {
            info!("Received SIGTERM, starting graceful shutdown");
        },
    }

    // Signal the server to shut down
    shutdown_token.cancel();
}

/// Returns a future that resolves when the shutdown signal is received
async fn shutdown_signal_handler(token: CancellationToken) {
    token.cancelled().await;
    info!("Shutdown signal received, starting graceful shutdown");

    // Give in-flight requests some time to complete
    tokio::time::sleep(Duration::from_secs(1)).await;
}
