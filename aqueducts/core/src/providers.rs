//! # Provider System
//!
//! This module provides registration hooks for external provider crates.
//! External crates register handlers on first use, avoiding circular dependencies.

use async_trait::async_trait;
use datafusion::{dataframe::DataFrame, execution::context::SessionContext};
use aqueducts_schemas::{DeltaSource, DeltaDestination, OdbcSource, OdbcDestination};
use serde::{Deserialize, Serialize};
use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, OnceLock},
};

/// Trait for source providers that can register data sources with DataFusion.
#[async_trait]
pub trait SourceProvider: Send + Sync {
    type Config: for<'de> Deserialize<'de> + Serialize + Clone + Send + Sync;
    type Error: std::error::Error + Send + Sync + 'static;

    fn name(&self) -> &'static str;

    async fn register(
        &self,
        ctx: Arc<SessionContext>,
        config: &Self::Config,
    ) -> std::result::Result<(), Self::Error>;
}

/// Trait for destination providers that can write DataFrames to various outputs.
#[async_trait]
pub trait DestinationProvider: Send + Sync {
    type Config: for<'de> Deserialize<'de> + Serialize + Clone + Send + Sync;
    type Error: std::error::Error + Send + Sync + 'static;

    fn name(&self) -> &'static str;

    async fn prepare(
        &self,
        ctx: Arc<SessionContext>,
        config: &Self::Config,
    ) -> std::result::Result<(), Self::Error>;

    async fn write(
        &self,
        ctx: Arc<SessionContext>,
        config: &Self::Config,
        data: DataFrame,
    ) -> std::result::Result<(), Self::Error>;
}

/// Type alias for async handlers to avoid repetition
type AsyncResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;
type AsyncFuture = Pin<Box<dyn Future<Output = AsyncResult> + Send>>;

/// Handler function types
pub type DeltaSourceHandler = fn(Arc<SessionContext>, &DeltaSource) -> AsyncFuture;
pub type DeltaDestinationPrepareHandler = fn(Arc<SessionContext>, &DeltaDestination) -> AsyncFuture;
pub type DeltaDestinationWriteHandler = fn(Arc<SessionContext>, &DeltaDestination, DataFrame) -> AsyncFuture;

pub type OdbcSourceHandler = fn(Arc<SessionContext>, &OdbcSource) -> AsyncFuture;
pub type OdbcDestinationPrepareHandler = fn(Arc<SessionContext>, &OdbcDestination) -> AsyncFuture;
pub type OdbcDestinationWriteHandler = fn(Arc<SessionContext>, &OdbcDestination, DataFrame) -> AsyncFuture;

/// Global handler registries - use OnceLock for thread-safe one-time initialization
static DELTA_SOURCE_HANDLER: OnceLock<DeltaSourceHandler> = OnceLock::new();
static DELTA_DESTINATION_PREPARE_HANDLER: OnceLock<DeltaDestinationPrepareHandler> = OnceLock::new();
static DELTA_DESTINATION_WRITE_HANDLER: OnceLock<DeltaDestinationWriteHandler> = OnceLock::new();

static ODBC_SOURCE_HANDLER: OnceLock<OdbcSourceHandler> = OnceLock::new();
static ODBC_DESTINATION_PREPARE_HANDLER: OnceLock<OdbcDestinationPrepareHandler> = OnceLock::new();
static ODBC_DESTINATION_WRITE_HANDLER: OnceLock<OdbcDestinationWriteHandler> = OnceLock::new();

/// Registration functions for external crates to call
pub fn register_delta_source_handler(handler: DeltaSourceHandler) {
    DELTA_SOURCE_HANDLER.set(handler).ok();
}

pub fn register_delta_destination_prepare_handler(handler: DeltaDestinationPrepareHandler) {
    DELTA_DESTINATION_PREPARE_HANDLER.set(handler).ok();
}

pub fn register_delta_destination_write_handler(handler: DeltaDestinationWriteHandler) {
    DELTA_DESTINATION_WRITE_HANDLER.set(handler).ok();
}

pub fn register_odbc_source_handler(handler: OdbcSourceHandler) {
    ODBC_SOURCE_HANDLER.set(handler).ok();
}

pub fn register_odbc_destination_prepare_handler(handler: OdbcDestinationPrepareHandler) {
    ODBC_DESTINATION_PREPARE_HANDLER.set(handler).ok();
}

pub fn register_odbc_destination_write_handler(handler: OdbcDestinationWriteHandler) {
    ODBC_DESTINATION_WRITE_HANDLER.set(handler).ok();
}

/// Functions for core to call handlers
pub async fn call_delta_source_handler(ctx: Arc<SessionContext>, config: &DeltaSource) -> AsyncResult {
    if let Some(handler) = DELTA_SOURCE_HANDLER.get() {
        handler(ctx, config).await
    } else {
        Err("Delta source handler not registered. Please ensure aqueducts-delta is included and initialized.".into())
    }
}

pub async fn call_delta_destination_prepare_handler(ctx: Arc<SessionContext>, config: &DeltaDestination) -> AsyncResult {
    if let Some(handler) = DELTA_DESTINATION_PREPARE_HANDLER.get() {
        handler(ctx, config).await
    } else {
        Err("Delta destination prepare handler not registered. Please ensure aqueducts-delta is included and initialized.".into())
    }
}

pub async fn call_delta_destination_write_handler(ctx: Arc<SessionContext>, config: &DeltaDestination, data: DataFrame) -> AsyncResult {
    if let Some(handler) = DELTA_DESTINATION_WRITE_HANDLER.get() {
        handler(ctx, config, data).await
    } else {
        Err("Delta destination write handler not registered. Please ensure aqueducts-delta is included and initialized.".into())
    }
}

pub async fn call_odbc_source_handler(ctx: Arc<SessionContext>, config: &OdbcSource) -> AsyncResult {
    if let Some(handler) = ODBC_SOURCE_HANDLER.get() {
        handler(ctx, config).await
    } else {
        Err("ODBC source handler not registered. Please ensure aqueducts-odbc is included and initialized.".into())
    }
}

pub async fn call_odbc_destination_prepare_handler(ctx: Arc<SessionContext>, config: &OdbcDestination) -> AsyncResult {
    if let Some(handler) = ODBC_DESTINATION_PREPARE_HANDLER.get() {
        handler(ctx, config).await
    } else {
        Err("ODBC destination prepare handler not registered. Please ensure aqueducts-odbc is included and initialized.".into())
    }
}

pub async fn call_odbc_destination_write_handler(ctx: Arc<SessionContext>, config: &OdbcDestination, data: DataFrame) -> AsyncResult {
    if let Some(handler) = ODBC_DESTINATION_WRITE_HANDLER.get() {
        handler(ctx, config, data).await
    } else {
        Err("ODBC destination write handler not registered. Please ensure aqueducts-odbc is included and initialized.".into())
    }
}