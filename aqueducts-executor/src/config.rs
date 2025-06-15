use thiserror::Error;
use uuid::Uuid;

/// Executor operation modes
#[derive(Debug, Clone, clap::ValueEnum)]
pub enum ExecutorMode {
    /// Standalone mode - executor manages its own API key for CLI connections
    Standalone,
    /// Managed mode - executor is fully managed by orchestrator
    Managed,
}

/// Errors that can occur during configuration validation
#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("API key cannot be empty")]
    EmptyApiKey,

    #[error("Max memory must be at least 1 GB")]
    InvalidMemoryLimit,

    #[error("Missing orchestrator URL for managed mode")]
    MissingOrchestratorUrl,

    #[error("Missing orchestrator API key for managed mode")]
    MissingOrchestratorApiKey,
}

/// Configuration for the executor
#[derive(Debug, Clone)]
pub struct Config {
    pub executor_id: Uuid,
    pub mode: ExecutorMode,
    pub max_memory_gb: Option<usize>,
    pub orchestrator_url: Option<String>,
    pub api_key: Option<String>,
}

impl Config {
    /// Create a new config with validation
    pub fn try_new(
        executor_id: Uuid,
        mode: ExecutorMode,
        max_memory_gb: Option<usize>,
        orchestrator_url: Option<String>,
        api_key: Option<String>,
    ) -> Result<Self, ConfigError> {
        // Validate memory limit
        if let Some(mem) = max_memory_gb {
            if mem == 0 {
                return Err(ConfigError::InvalidMemoryLimit);
            }
        }

        // Validate mode-specific configuration
        match (&mode, &orchestrator_url, &api_key) {
            (ExecutorMode::Managed, None, _) => Err(ConfigError::MissingOrchestratorUrl),
            (ExecutorMode::Managed, _, None) => Err(ConfigError::MissingOrchestratorApiKey),
            _ => Ok(Self {
                executor_id,
                mode,
                max_memory_gb,
                orchestrator_url,
                api_key,
            }),
        }
    }
}
