//! # Object Store Module
//!
//! This module provides a unified interface for working with various object storage providers
//! through the `object_store` crate. It supports local file systems, in-memory storage,
//! and cloud providers like AWS S3, Google Cloud Storage, and Azure Blob Storage.
//!
//! ## Features
//!
//! - **Provider abstraction**: Unified interface for all storage backends
//! - **Feature-gated**: Cloud providers are only included when their respective features are enabled
//! - **Environment variable support**: Cloud providers automatically read configuration from environment variables
//! - **DataFusion integration**: Direct integration with DataFusion's SessionContext
//!
//! ## Supported URL Schemes
//!
//! - `file://` - Local file system
//! - `memory://` - In-memory storage
//! - `s3://`, `s3a://` - Amazon S3 (with "s3" feature)
//! - `gs://`, `gcs://` - Google Cloud Storage (with "gcs" feature)
//! - `az://`, `azure://`, `abfs://`, `abfss://` - Azure Blob Storage (with "azure" feature)

use std::{
    collections::HashMap,
    sync::{Arc, OnceLock},
};

use url::Url;

#[cfg(feature = "s3")]
mod s3;

#[cfg(feature = "gcs")]
mod gcs;

#[cfg(feature = "azure")]
mod azure;

#[cfg(feature = "s3")]
pub use s3::S3Provider;

#[cfg(feature = "gcs")]
pub use gcs::GcsProvider;

#[cfg(feature = "azure")]
pub use azure::AzureProvider;

use crate::error::AqueductsError;

/// Trait for object storage providers.
///
/// Each provider is responsible for:
/// - Determining which URL schemes it supports
/// - Creating object store instances for supported URLs
/// - Handling provider-specific configuration options
pub trait ObjectStoreProvider: Send + Sync {
    /// Check if this provider supports the given URL scheme.
    fn supports_scheme(&self, scheme: &str) -> bool;

    /// Create an object store instance for the given location and options.
    ///
    /// # Arguments
    ///
    /// * `location` - The URL of the storage location
    /// * `options` - Configuration options specific to the provider
    ///
    /// # Returns
    ///
    /// An `Arc<dyn ObjectStore>` that can be used to interact with the storage backend.
    fn create_store(
        &self,
        location: &Url,
        options: &HashMap<String, String>,
    ) -> crate::error::Result<Arc<dyn object_store::ObjectStore>>;
}

/// Registry for object storage providers.
///
/// The registry automatically includes all providers that are enabled via feature flags:
/// - Local file system and in-memory storage (always available)
/// - S3 provider (when "s3" feature is enabled)
/// - GCS provider (when "gcs" feature is enabled)
/// - Azure provider (when "azure" feature is enabled)
///
/// Note: While local file system and in-memory storage are handled directly by DataFusion,
/// this registry provides them for API consistency.
pub struct ObjectStoreRegistry {
    providers: Vec<Box<dyn ObjectStoreProvider>>,
}

impl ObjectStoreRegistry {
    /// Create a new registry with all available providers.
    ///
    /// The registry will include:
    /// - LocalFileProvider (always included)
    /// - S3Provider (if "s3" feature is enabled)
    /// - GcsProvider (if "gcs" feature is enabled)
    /// - AzureProvider (if "azure" feature is enabled)
    #[allow(clippy::vec_init_then_push)]
    pub fn new() -> Self {
        let mut providers: Vec<Box<dyn ObjectStoreProvider>> = Vec::new();

        #[cfg(feature = "s3")]
        providers.push(Box::new(S3Provider));

        #[cfg(feature = "gcs")]
        providers.push(Box::new(GcsProvider));

        #[cfg(feature = "azure")]
        providers.push(Box::new(AzureProvider));

        providers.push(Box::new(LocalFileProvider));

        Self { providers }
    }

    /// Create an object store for the given location.
    ///
    /// The registry will iterate through all registered providers and use the first one
    /// that supports the URL scheme.
    ///
    /// # Arguments
    ///
    /// * `location` - The URL of the storage location
    /// * `options` - Configuration options that will be passed to the provider
    ///
    /// # Returns
    ///
    /// An object store instance, or an error if no provider supports the URL scheme.
    pub fn create_store(
        &self,
        location: &Url,
        options: &HashMap<String, String>,
    ) -> crate::error::Result<Arc<dyn object_store::ObjectStore>> {
        for provider in &self.providers {
            if provider.supports_scheme(location.scheme()) {
                return provider.create_store(location, options);
            }
        }

        Err(AqueductsError::unsupported(
            "URL scheme",
            format!("Unsupported URL scheme: {}", location.scheme()),
        ))
    }
}

impl Default for ObjectStoreRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Provider for local file system and in-memory storage.
///
/// This provider supports:
/// - `file://` URLs for local file system access
/// - `memory://` URLs for in-memory storage (useful for testing)
pub struct LocalFileProvider;

impl ObjectStoreProvider for LocalFileProvider {
    fn supports_scheme(&self, scheme: &str) -> bool {
        matches!(scheme, "file" | "memory")
    }

    fn create_store(
        &self,
        location: &Url,
        _options: &HashMap<String, String>,
    ) -> crate::error::Result<Arc<dyn object_store::ObjectStore>> {
        match location.scheme() {
            "file" => Ok(Arc::new(object_store::local::LocalFileSystem::new())),
            "memory" => Ok(Arc::new(object_store::memory::InMemory::new())),
            scheme => Err(AqueductsError::unsupported(
                "URL scheme",
                format!("Unsupported URL scheme: {scheme}"),
            )),
        }
    }
}

/// Global registry instance for object store providers.
///
/// This ensures that the registry is only created once and reused across multiple
/// calls to `register_object_store()`
static GLOBAL_REGISTRY: OnceLock<ObjectStoreRegistry> = OnceLock::new();

/// Get the global object store registry.
///
/// This function returns a reference to the global registry, creating it on first access.
/// The registry includes all providers that are enabled via feature flags.
pub fn global_registry() -> &'static ObjectStoreRegistry {
    GLOBAL_REGISTRY.get_or_init(ObjectStoreRegistry::new)
}

/// Register a cloud object store with a DataFusion SessionContext.
///
/// This is a convenience function that uses the global registry to create a cloud storage
/// provider and registers it with DataFusion. The global registry is created once and reused
/// across multiple calls, making this efficient when working with many different storage locations.
///
/// Local file (`file://`) and memory (`memory://`) schemes are skipped since DataFusion handles them natively.
///
/// # Arguments
///
/// * `ctx` - The DataFusion SessionContext to register the store with
/// * `location` - The URL of the cloud storage location
/// * `storage_options` - Configuration options for the storage provider
///
/// # Returns
///
/// `Ok(())` if the store was registered successfully, or if the scheme is local.
/// `Err(StoreError)` if no provider supports the scheme or store creation fails.
pub fn register_object_store(
    ctx: Arc<datafusion::prelude::SessionContext>,
    location: &Url,
    storage_options: &HashMap<String, String>,
) -> crate::error::Result<()> {
    // Skip local schemes as DataFusion handles them natively
    if matches!(location.scheme(), "file" | "memory") {
        return Ok(());
    }

    let registry = global_registry();
    let store = registry.create_store(location, storage_options)?;

    ctx.runtime_env().register_object_store(location, store);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_local_file_provider_supports_schemes() {
        let provider = LocalFileProvider;
        assert!(provider.supports_scheme("file"));
        assert!(provider.supports_scheme("memory"));
        assert!(!provider.supports_scheme("s3"));
        assert!(!provider.supports_scheme("gs"));
        assert!(!provider.supports_scheme("azure"));
    }

    #[test]
    fn test_local_file_provider_creates_file_store() {
        let provider = LocalFileProvider;
        let url = Url::parse("file:///tmp/test").unwrap();
        let options = HashMap::new();

        let result = provider.create_store(&url, &options);
        assert!(result.is_ok());
    }

    #[test]
    fn test_local_file_provider_creates_memory_store() {
        let provider = LocalFileProvider;
        let url = Url::parse("memory://test").unwrap();
        let options = HashMap::new();

        let result = provider.create_store(&url, &options);
        assert!(result.is_ok());
    }

    #[test]
    fn test_local_file_provider_rejects_unsupported_scheme() {
        let provider = LocalFileProvider;
        let url = Url::parse("http://example.com").unwrap();
        let options = HashMap::new();

        let result = provider.create_store(&url, &options);
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert!(
            error.to_string().contains("http"),
            "Expected error message to contain 'http', got: {error}"
        );
    }

    #[test]
    fn test_registry_includes_local_provider() {
        let registry = ObjectStoreRegistry::new();
        let url = Url::parse("file:///tmp/test").unwrap();
        let options = HashMap::new();

        let result = registry.create_store(&url, &options);
        assert!(result.is_ok());
    }

    #[test]
    fn test_registry_rejects_unsupported_scheme() {
        let registry = ObjectStoreRegistry::new();
        let url = Url::parse("ftp://example.com").unwrap();
        let options = HashMap::new();

        let result = registry.create_store(&url, &options);
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert!(
            error.to_string().contains("ftp"),
            "Expected error message to contain 'ftp', got: {error}"
        );
    }

    #[cfg(feature = "s3")]
    #[test]
    fn test_s3_provider_supports_s3_schemes() {
        let provider = crate::store::S3Provider;
        assert!(provider.supports_scheme("s3"));
        assert!(provider.supports_scheme("s3a"));
        assert!(!provider.supports_scheme("file"));
        assert!(!provider.supports_scheme("gs"));
    }

    #[cfg(feature = "gcs")]
    #[test]
    fn test_gcs_provider_supports_gcs_schemes() {
        let provider = crate::store::GcsProvider;
        assert!(provider.supports_scheme("gs"));
        assert!(provider.supports_scheme("gcs"));
        assert!(!provider.supports_scheme("file"));
        assert!(!provider.supports_scheme("s3"));
    }

    #[cfg(feature = "azure")]
    #[test]
    fn test_azure_provider_supports_azure_schemes() {
        let provider = crate::store::AzureProvider;
        assert!(provider.supports_scheme("az"));
        assert!(provider.supports_scheme("azure"));
        assert!(provider.supports_scheme("abfs"));
        assert!(provider.supports_scheme("abfss"));
        assert!(!provider.supports_scheme("file"));
        assert!(!provider.supports_scheme("s3"));
    }
}
