mod api;
mod config;
mod error;
mod executor;

use axum::Router;
use clap::Parser;
use config::Config;
use executor::ExecutionManager;
use std::{net::SocketAddr, str::FromStr, sync::Arc, time::Duration};
use tokio::signal;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, Level};
use tracing_subscriber::{fmt, prelude::*, EnvFilter};
use uuid::Uuid;

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
    max_memory: Option<u32>,

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

    let executor_id = cli.executor_id.unwrap_or_else(|| Uuid::new_v4());
    info!(
        executor_id = %executor_id,
        version = %env!("CARGO_PKG_VERSION"),
        "Starting Aqueducts Executor"
    );

    // Create and validate configuration
    let config = match Config::new(cli.api_key, executor_id, cli.max_memory) {
        Ok(config) => config,
        Err(e) => {
            eprintln!("Configuration error: {}", e);
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
        .nest("/api", api::router(Arc::clone(&context)))
        .with_state(context);

    let addr: SocketAddr = match format!("{}:{}", cli.host, cli.port).parse() {
        Ok(addr) => addr,
        Err(e) => {
            error!("Failed to parse socket address: {}", e);
            eprintln!("Invalid host or port configuration: {}", e);
            std::process::exit(1);
        }
    };

    info!(addr = %addr, "Listening for connections");
    let listener = match tokio::net::TcpListener::bind(addr).await {
        Ok(listener) => listener,
        Err(e) => {
            error!("Failed to bind to address {}: {}", addr, e);
            eprintln!("Could not start server: {}", e);
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
