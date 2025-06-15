pub mod config;
pub mod coordination;
pub mod error;
pub mod execution;

pub use config::{Config, ExecutorMode};
pub use coordination::OrchestratorHandler;
pub use execution::{ExecutionManager, ExecutorProgressTracker};

// Re-export from main for API context
use std::sync::Arc;
pub type ApiContextRef = Arc<ApiContext>;

pub struct ApiContext {
    pub config: Config,
    pub manager: Arc<ExecutionManager>,
}
