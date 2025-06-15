use std::{net::SocketAddr, str::FromStr, sync::Arc, time::Duration};

use axum::Router;
use clap::Parser;
use config::{Config, ExecutorMode};
use coordination::OrchestratorHandler;
use execution::ExecutionManager;
use tokio::signal;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, Level};
use tracing_subscriber::{fmt, prelude::*, EnvFilter};
use uuid::Uuid;

mod config;
mod coordination;
mod error;
mod execution;

/// Remote executor for Aqueducts data pipeline framework
#[derive(Debug, Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    /// Server bind address
    #[arg(long, env = "BIND_ADDRESS", default_value = "0.0.0.0:3031")]
    bind_address: String,

    /// Executor operation mode (standalone or managed)
    #[arg(long, env = "EXECUTOR_MODE", default_value = "standalone")]
    mode: ExecutorMode,

    /// Orchestrator URL for managed mode
    #[arg(long, env = "ORCHESTRATOR_URL")]
    orchestrator_url: Option<String>,

    /// API key - for standalone mode: executor's own key, for managed mode: orchestrator's key
    #[arg(long, env = "API_KEY")]
    api_key: Option<String>,

    /// Maximum memory usage in GB (optional)
    #[arg(long, env = "MAX_MEMORY")]
    max_memory: Option<usize>,

    /// Unique identifier for this executor (optional)
    #[arg(long, env = "EXECUTOR_ID")]
    executor_id: Option<Uuid>,

    /// Logging level (info, debug, trace)
    #[arg(long, env = "RUST_LOG", default_value = "info")]
    log_level: String,
}

pub type ApiContextRef = Arc<ApiContext>;

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

    let executor_id = cli.executor_id.unwrap_or_else(Uuid::new_v4);
    info!(
        executor_id = %executor_id,
        version = %env!("CARGO_PKG_VERSION"),
        "Starting Aqueducts Executor"
    );

    let config = match Config::try_new(
        executor_id,
        cli.mode,
        cli.max_memory,
        cli.orchestrator_url,
        cli.api_key,
    ) {
        Ok(config) => config,
        Err(e) => {
            error!("Configuration error: {}", e);
            std::process::exit(1);
        }
    };

    info!(
        executor_id = %config.executor_id,
        max_memory_gb = ?config.max_memory_gb,
        mode = ?config.mode,
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

    match context.config.mode {
        ExecutorMode::Standalone => {
            let app = Router::new()
                .merge(coordination::standalone::router(Arc::clone(&context)))
                .with_state(context);

            let addr: SocketAddr = match cli.bind_address.parse() {
                Ok(addr) => addr,
                Err(e) => {
                    error!("Failed to parse bind address '{}': {}", cli.bind_address, e);
                    std::process::exit(1);
                }
            };

            info!(addr = %addr, "Starting standalone executor server");
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
        }
        ExecutorMode::Managed => {
            info!("Starting managed executor, connecting to orchestrator");

            let orchestrator_client =
                OrchestratorHandler::new(context.config.clone(), context.manager.clone());

            // Run orchestrator client until shutdown
            tokio::select! {
                _ = orchestrator_client.start() => {
                    info!("Orchestrator client stopped");
                },
                _ = shutdown_signal_handler(shutdown_token) => {
                    info!("Received shutdown signal, stopping orchestrator client");
                }
            }
        }
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
