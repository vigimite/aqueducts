//! # AWS S3 Object Store Provider
//!
//! This module provides an S3 implementation of the `ObjectStoreProvider` trait
//! using the `object_store` crate's AWS S3 backend.

use super::{ObjectStoreProvider, StoreError};
use crate::error::Result;
use object_store::aws::AmazonS3Builder;
use std::{collections::HashMap, sync::Arc};
use tracing::warn;
use url::Url;

/// Provider for Amazon S3 and S3-compatible storage.
///
/// This provider supports:
/// - `s3://` URLs for standard S3 access
/// - `s3a://` URLs (Hadoop-style S3 access)
///
/// ## Automatic Environment Variable Configuration
///
/// The provider automatically reads AWS credentials and configuration from environment variables:
/// - `AWS_ACCESS_KEY_ID` - AWS access key
/// - `AWS_SECRET_ACCESS_KEY` - AWS secret key
/// - `AWS_REGION` - AWS region (e.g., "us-west-2")
/// - `AWS_ENDPOINT` - Custom S3 endpoint (for S3-compatible services)
/// - `AWS_SESSION_TOKEN` - Session token for temporary credentials
/// - `AWS_PROFILE` - AWS profile name
/// - `AWS_ALLOW_HTTP` - Allow HTTP connections (set to "true")
///
/// ## Example Usage with Environment Variables
///
/// ```bash
/// export AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE
/// export AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
/// export AWS_REGION=us-west-2
/// ```
///
/// ```rust,no_run
/// # use std::collections::HashMap;
/// # use url::Url;
/// # use aqueducts_core::store::{S3Provider, ObjectStoreProvider};
/// let provider = S3Provider;
/// let url = Url::parse("s3://my-bucket/path/to/data").unwrap();
/// let options = HashMap::new(); // Uses environment variables
///
/// let store = provider.create_store(&url, &options).unwrap();
/// ```
///
/// ## Example Usage with Explicit Configuration
///
/// ```rust,no_run
/// # use std::collections::HashMap;
/// # use url::Url;
/// # use aqueducts_core::store::{S3Provider, ObjectStoreProvider};
/// let provider = S3Provider;
/// let url = Url::parse("s3://my-bucket/data").unwrap();
/// let mut options = HashMap::new();
/// options.insert("aws_access_key_id".to_string(), "AKIAIOSFODNN7EXAMPLE".to_string());
/// options.insert("aws_secret_access_key".to_string(), "your-secret-key".to_string());
/// options.insert("aws_region".to_string(), "us-west-2".to_string());
///
/// let store = provider.create_store(&url, &options).unwrap();
/// ```
///
/// ## S3-Compatible Services
///
/// ```rust,no_run
/// # use std::collections::HashMap;
/// # use url::Url;
/// # use aqueducts_core::store::{S3Provider, ObjectStoreProvider};
/// let provider = S3Provider;
/// let url = Url::parse("s3://my-bucket/data").unwrap();
/// let mut options = HashMap::new();
/// options.insert("aws_endpoint".to_string(), "https://minio.example.com".to_string());
/// options.insert("aws_access_key_id".to_string(), "minioadmin".to_string());
/// options.insert("aws_secret_access_key".to_string(), "minioadmin".to_string());
/// options.insert("aws_region".to_string(), "us-east-1".to_string());
/// options.insert("aws_allow_http".to_string(), "true".to_string());
///
/// let store = provider.create_store(&url, &options).unwrap();
/// ```
///
/// ## Supported Configuration Options
///
/// All options can be provided with or without the `aws_` prefix:
///
/// | Option | Description | Environment Variable |
/// |--------|-------------|---------------------|
/// | `aws_access_key_id` | AWS access key ID | `AWS_ACCESS_KEY_ID` |
/// | `aws_secret_access_key` | AWS secret access key | `AWS_SECRET_ACCESS_KEY` |
/// | `aws_region` | AWS region | `AWS_REGION` |
/// | `aws_endpoint` | Custom S3 endpoint | `AWS_ENDPOINT` |
/// | `aws_session_token` | AWS session token | `AWS_SESSION_TOKEN` |
/// | `aws_allow_http` | Allow HTTP connections | `AWS_ALLOW_HTTP` |
/// | `aws_virtual_hosted_style_request` | Use virtual hosted-style requests | - |
/// | `aws_checksum_algorithm` | Checksum algorithm for uploads | - |
/// | `aws_s3_express` | Enable S3 Express One Zone | - |
/// | `aws_unsigned_payload` | Use unsigned payload | - |
/// | `aws_skip_signature` | Skip request signing | - |
/// | `aws_imdsv1_fallback` | Enable IMDSv1 fallback | - |
pub struct S3Provider;

impl ObjectStoreProvider for S3Provider {
    fn supports_scheme(&self, scheme: &str) -> bool {
        matches!(scheme, "s3" | "s3a")
    }

    fn create_store(
        &self,
        location: &Url,
        options: &HashMap<String, String>,
    ) -> Result<Arc<dyn object_store::ObjectStore>> {
        let mut builder = AmazonS3Builder::from_env();

        if let Some(bucket) = location.host_str() {
            builder = builder.with_bucket_name(bucket);
        }

        for (key, value) in options {
            builder = match key.as_str() {
                "aws_access_key_id" | "access_key_id" => builder.with_access_key_id(value),
                "aws_secret_access_key" | "secret_access_key" => {
                    builder.with_secret_access_key(value)
                }
                "aws_region" | "region" => builder.with_region(value),
                "aws_endpoint" | "endpoint" => builder.with_endpoint(value),
                "aws_session_token" | "session_token" => builder.with_token(value),
                "aws_allow_http" => builder.with_allow_http(value.parse::<bool>().unwrap_or(false)),
                "aws_virtual_hosted_style_request" => builder
                    .with_virtual_hosted_style_request(value.parse::<bool>().unwrap_or(false)),
                "aws_checksum_algorithm" => {
                    if let Ok(checksum) = value.parse() {
                        builder.with_checksum_algorithm(checksum)
                    } else {
                        builder
                    }
                }
                "aws_s3_express" | "s3_express" => {
                    builder.with_s3_express(value.parse::<bool>().unwrap_or(false))
                }
                "aws_unsigned_payload" => {
                    builder.with_unsigned_payload(value.parse::<bool>().unwrap_or(false))
                }
                "aws_skip_signature" => {
                    builder.with_skip_signature(value.parse::<bool>().unwrap_or(false))
                }
                "aws_imdsv1_fallback" => {
                    if value.parse::<bool>().unwrap_or(false) {
                        builder.with_imdsv1_fallback()
                    } else {
                        builder
                    }
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
