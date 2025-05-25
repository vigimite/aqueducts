//! Delta Lake object store handler registration.
//!
//! This module provides functionality to register Delta Lake object store factories
//! for cloud providers. These handlers are required for Delta Lake to work with
//! cloud storage services like S3, GCS, and Azure Blob Storage.

use std::sync::Once;

static INIT: Once = Once::new();

/// Register Delta Lake object store handlers for enabled cloud providers.
///
/// This function must be called before using Delta Lake with cloud storage to ensure
/// the proper object store factories are registered. It will register handlers for
/// all cloud providers that are enabled via feature flags.
///
/// The registration is performed exactly once, even if this function is called multiple times.
pub fn register_handlers() {
    INIT.call_once(|| {
        tracing::debug!("Registering Delta Lake object store handlers");

        #[cfg(feature = "s3")]
        {
            tracing::debug!("Registering Delta Lake S3 handlers");
            deltalake::aws::register_handlers(None);
        }

        #[cfg(feature = "gcs")]
        {
            tracing::debug!("Registering Delta Lake GCS handlers");
            deltalake::gcp::register_handlers(None);
        }

        #[cfg(feature = "azure")]
        {
            tracing::debug!("Registering Delta Lake Azure handlers");
            deltalake::azure::register_handlers(None);
        }

        tracing::debug!("Delta Lake handlers registration complete");
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_register_handlers() {
        // This test just ensures the function can be called without panicking
        register_handlers();

        // Call it again to test idempotency
        register_handlers();
    }
}
