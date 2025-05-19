//! Trait definitions for runtime behavior

use async_trait::async_trait;
use datafusion::dataframe::DataFrame;
use datafusion::execution::context::SessionContext;
use std::sync::Arc;

use crate::Result;

/// Trait for registering a source with a session context
#[async_trait]
pub trait SourceProvider {
    /// Register the source with the session context
    async fn register(&self, ctx: Arc<SessionContext>) -> Result<()>;
}

/// Trait for registering and writing to a destination
#[async_trait]
pub trait DestinationProvider {
    /// Register the destination with the session context
    async fn register(&self, ctx: Arc<SessionContext>) -> Result<()>;

    /// Write data to the destination
    async fn write(&self, ctx: Arc<SessionContext>, data: DataFrame) -> Result<()>;
}

/// Trait for executing a stage
#[async_trait]
pub trait StageProvider {
    /// Execute the stage and return the resulting DataFrame
    async fn execute(&self, ctx: Arc<SessionContext>) -> Result<DataFrame>;
}
