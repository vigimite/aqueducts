//! # Google Cloud Storage Object Store Provider
//!
//! This module provides a GCS implementation of the `ObjectStoreProvider` trait
//! using the `object_store` crate's Google Cloud Storage backend.

use object_store::gcp::GoogleCloudStorageBuilder;
use std::{collections::HashMap, sync::Arc};
use tracing::warn;
use url::Url;

use super::{ObjectStoreProvider, StoreError};

/// Provider for Google Cloud Storage.
///
/// This provider supports:
/// - `gs://` URLs for standard GCS access
/// - `gcs://` URLs (alternative GCS scheme)
///
/// ## Automatic Environment Variable Configuration
///
/// The provider automatically reads GCP credentials and configuration from environment variables:
/// - `GOOGLE_APPLICATION_CREDENTIALS` - Path to service account JSON file
/// - `GOOGLE_SERVICE_ACCOUNT` - Service account email
/// - `GOOGLE_SERVICE_ACCOUNT_KEY` - Service account private key
///
/// ## Supported Configuration Options
///
/// | Option                           | Description                  | Environment Variable             |
/// |----------------------------------|------------------------------|----------------------------------|
/// | `google_service_account`         | Path to service account JSON | `GOOGLE_APPLICATION_CREDENTIALS` |
/// | `google_service_account_key`     | Service account private key  | -                                |
/// | `google_application_credentials` | Application credentials      | `GOOGLE_APPLICATION_CREDENTIALS` |
pub struct GcsProvider;

impl ObjectStoreProvider for GcsProvider {
    fn supports_scheme(&self, scheme: &str) -> bool {
        matches!(scheme, "gs" | "gcs")
    }

    fn create_store(
        &self,
        location: &Url,
        options: &HashMap<String, String>,
    ) -> Result<Arc<dyn object_store::ObjectStore>, StoreError> {
        let mut builder = GoogleCloudStorageBuilder::from_env();

        if let Some(bucket) = location.host_str() {
            builder = builder.with_bucket_name(bucket);
        }

        for (key, value) in options {
            builder = match key.as_str() {
                "google_service_account" => builder.with_service_account_path(value),
                "google_service_account_key" => builder.with_service_account_key(value),
                "google_application_credentials" => builder.with_application_credentials(value),
                unknown => {
                    warn!("Unknown object_store configuration key: {unknown}");
                    builder
                }
            };
        }

        let result = builder
            .build()
            .map(|store| Arc::new(store) as Arc<dyn object_store::ObjectStore>)?;

        Ok(result)
    }
}
