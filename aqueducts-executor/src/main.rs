mod auth;
mod error;
mod executor;
mod handlers;
mod progress;

use auth::api_key_auth;
use axum::{
    middleware,
    routing::{get, post},
    Router,
};
use clap::Parser;
use handlers::{cancel_pipeline, execute_pipeline, get_status, health_check};
use std::{net::SocketAddr, str::FromStr, sync::Arc};
use tracing::{info, Level};
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

    /// Maximum memory usage in GB (0 for unlimited)
    #[arg(long, env = "AQUEDUCTS_MAX_MEMORY", default_value = "0")]
    max_memory: u32,

    /// URL of Aqueducts server for registration (optional)
    #[arg(long, env = "AQUEDUCTS_SERVER_URL")]
    server_url: Option<String>,

    /// Unique identifier for this executor
    #[arg(long, env = "AQUEDUCTS_EXECUTOR_ID")]
    executor_id: Option<String>,

    /// Logging level (info, debug, trace)
    #[arg(long, env = "AQUEDUCTS_LOG_LEVEL", default_value = "info")]
    log_level: String,
}

#[derive(Debug, Clone)]
pub struct AppState {
    pub api_key: String,
    pub executor_id: String,
    pub max_memory_gb: u32,
    pub _server_url: Option<String>,
}

impl AppState {
    pub fn new(
        api_key: String,
        executor_id: String,
        max_memory_gb: u32,
        _server_url: Option<String>,
    ) -> Self {
        Self {
            api_key,
            executor_id,
            max_memory_gb,
            _server_url,
        }
    }
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    // Setup JSON-formatted logging for better integration with log aggregation systems
    let log_level = Level::from_str(cli.log_level.to_lowercase().as_str()).unwrap_or(Level::INFO);

    tracing_subscriber::registry()
        .with(
            fmt::layer()
                .json()
                .with_current_span(true)
                .with_span_list(true)
                .with_file(true)
                .with_line_number(true)
                .with_target(true),
        )
        .with(EnvFilter::from_default_env().add_directive(log_level.into()))
        .init();

    info!("Registering Aqueducts handlers");
    aqueducts::register_handlers();

    if let Err(e) = datafusion_functions_json::register_all(
        &mut datafusion::execution::context::SessionContext::new(),
    ) {
        info!("Failed to initialize DataFusion JSON functions (will be retried at execution time): {}", e);
    }

    // Generate executor ID if not provided
    let executor_id = cli
        .executor_id
        .unwrap_or_else(|| Uuid::new_v4().to_string());
    info!(
        executor_id = %executor_id,
        version = %env!("CARGO_PKG_VERSION"),
        "Starting Aqueducts Executor"
    );

    let state = Arc::new(AppState::new(
        cli.api_key,
        executor_id,
        cli.max_memory,
        cli.server_url,
    ));

    // Public routes (no authentication required)
    let public_routes = Router::new().route("/health", get(health_check));

    // Protected routes (require authentication)
    let protected_routes = Router::new()
        .route("/execute", post(execute_pipeline))
        .route("/cancel", post(cancel_pipeline))
        .route("/status", get(get_status))
        .route_layer(middleware::from_fn_with_state(
            Arc::clone(&state),
            api_key_auth,
        ));

    let app = Router::new()
        .merge(public_routes)
        .merge(protected_routes)
        .with_state(state);

    let addr: SocketAddr = format!("{}:{}", cli.host, cli.port)
        .parse()
        .expect("Failed to parse socket address");

    info!(addr = %addr, "Listening for connections");
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
