//! # Azure Blob Storage Object Store Provider
//!
//! This module provides an Azure Blob Storage implementation of the `ObjectStoreProvider` trait
//! using the `object_store` crate's Microsoft Azure backend.

use object_store::azure::MicrosoftAzureBuilder;
use std::{collections::HashMap, sync::Arc};
use tracing::warn;
use url::Url;

use super::{ObjectStoreProvider, StoreError};
use crate::error::Result;

/// Provider for Azure Blob Storage.
///
/// This provider supports:
/// - `az://` URLs for Azure Blob Storage
/// - `azure://` URLs (alternative Azure scheme)
/// - `abfs://` URLs (Azure Data Lake Storage Gen2)
/// - `abfss://` URLs (Azure Data Lake Storage Gen2 with SSL)
///
/// ## Automatic Environment Variable Configuration
///
/// The provider automatically reads Azure credentials and configuration from environment variables:
/// - `AZURE_STORAGE_ACCOUNT_NAME` - Storage account name
/// - `AZURE_STORAGE_ACCOUNT_KEY` - Storage account access key
/// - `AZURE_CLIENT_ID` - Azure AD application client ID
/// - `AZURE_CLIENT_SECRET` - Azure AD application client secret
/// - `AZURE_TENANT_ID` - Azure AD tenant ID
///
/// ## Supported Configuration Options
///
/// | Option | Description | Environment Variable |
/// |--------|-------------|---------------------|
/// | `azure_storage_account_name` | Storage account name | `AZURE_STORAGE_ACCOUNT_NAME` |
/// | `azure_storage_account_key` | Storage account access key | `AZURE_STORAGE_ACCOUNT_KEY` |
/// | `azure_storage_client_id` | Azure AD client ID | `AZURE_CLIENT_ID` |
/// | `azure_storage_client_secret` | Azure AD client secret | `AZURE_CLIENT_SECRET` |
/// | `azure_storage_tenant_id` | Azure AD tenant ID | `AZURE_TENANT_ID` |
/// | `azure_storage_use_emulator` | Use storage emulator | - |
/// | `azure_storage_use_azure_cli` | Use Azure CLI credentials | - |
/// | `azure_federated_token_file` | Federated token file path | - |
/// | `azure_use_fabric_endpoint` | Use Fabric endpoint | - |
/// | `azure_msi_endpoint` | MSI endpoint URL | - |
/// | `azure_disable_tagging` | Disable object tagging | - |
pub struct AzureProvider;

impl ObjectStoreProvider for AzureProvider {
    fn supports_scheme(&self, scheme: &str) -> bool {
        matches!(scheme, "az" | "azure" | "abfs" | "abfss")
    }

    fn create_store(
        &self,
        location: &Url,
        options: &HashMap<String, String>,
    ) -> Result<Arc<dyn object_store::ObjectStore>> {
        let mut builder = MicrosoftAzureBuilder::from_env();

        if let Some(account) = location.host_str() {
            let container = location
                .path()
                .trim_start_matches('/')
                .split('/')
                .next()
                .unwrap_or("");
            if !container.is_empty() {
                builder = builder.with_container_name(container);
            }

            if account.ends_with(".blob.core.windows.net") {
                let account_name = account.replace(".blob.core.windows.net", "");
                builder = builder.with_account(account_name);
            } else {
                builder = builder.with_account(account);
            }
        }

        for (key, value) in options {
            builder = match key.as_str() {
                "azure_storage_account_name" | "account_name" => builder.with_account(value),
                "azure_storage_account_key" | "account_key" => builder.with_access_key(value),
                // Note: SAS token configuration may not be available in object_store 0.12
                "azure_storage_client_id" | "client_id" => builder.with_client_id(value),
                "azure_storage_client_secret" | "client_secret" => {
                    builder.with_client_secret(value)
                }
                "azure_storage_tenant_id" | "tenant_id" => builder.with_tenant_id(value),
                "azure_storage_use_emulator" => {
                    builder.with_use_emulator(value.parse::<bool>().unwrap_or(false))
                }
                "azure_storage_use_azure_cli" => {
                    builder.with_use_azure_cli(value.parse::<bool>().unwrap_or(false))
                }
                "azure_federated_token_file" => builder.with_federated_token_file(value),
                "azure_use_fabric_endpoint" => {
                    builder.with_use_fabric_endpoint(value.parse::<bool>().unwrap_or(false))
                }
                "azure_msi_endpoint" => builder.with_msi_endpoint(value),
                "azure_disable_tagging" => {
                    builder.with_disable_tagging(value.parse::<bool>().unwrap_or(false))
                }
                unknown => {
                    warn!("Unknown object_store configuration key: {unknown}");
                    builder
                }
            };
        }

        Ok(builder
            .build()
            .map(|store| Arc::new(store) as Arc<dyn object_store::ObjectStore>)
            .map_err(|e| StoreError::Creation { source: e })?)
    }
}
