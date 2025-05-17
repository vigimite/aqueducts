use thiserror::Error;
use uuid::Uuid;

/// Errors that can occur during configuration validation
#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("API key cannot be empty")]
    EmptyApiKey,

    #[error("Max memory must be at least 1 GB")]
    InvalidMemoryLimit,
}

/// Configuration for the executor
#[derive(Debug, Clone)]
pub struct Config {
    pub api_key: String,
    pub executor_id: Uuid,
    pub max_memory_gb: Option<u32>,
}

impl Config {
    /// Create a new config with validation
    pub fn new(
        api_key: String,
        executor_id: Uuid,
        max_memory_gb: Option<u32>,
    ) -> Result<Self, ConfigError> {
        if api_key.trim().is_empty() {
            return Err(ConfigError::EmptyApiKey);
        }

        if let Some(mem) = max_memory_gb {
            if mem == 0 {
                return Err(ConfigError::InvalidMemoryLimit);
            }
        }

        Ok(Self {
            api_key,
            executor_id,
            max_memory_gb,
        })
    }
}
