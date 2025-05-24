// Manual test to verify store functionality without requiring cloud accounts
// Run with: cargo test -p aqueducts-core --features "s3,gcs,azure" store_manual_test --test store_manual_test

use aqueducts_core::store::{ObjectStoreProvider, ObjectStoreRegistry};
use std::collections::HashMap;
use url::Url;

#[test]
fn test_registry_includes_all_enabled_providers() {
    let registry = ObjectStoreRegistry::new();

    // Count how many providers are available
    let providers_count = {
        let mut count = 1; // LocalFileProvider is always present

        #[cfg(feature = "s3")]
        {
            count += 1;
        }

        #[cfg(feature = "gcs")]
        {
            count += 1;
        }

        #[cfg(feature = "azure")]
        {
            count += 1;
        }

        count
    };

    println!("Registry should have {} providers", providers_count);

    // Test that registry correctly handles different schemes
    let options = HashMap::new();

    // File system (always works)
    let file_url = Url::parse("file:///tmp/test").unwrap();
    assert!(registry.create_store(&file_url, &options).is_ok());

    // Memory (always works)
    let memory_url = Url::parse("memory://test").unwrap();
    assert!(registry.create_store(&memory_url, &options).is_ok());

    // Test cloud providers based on enabled features
    #[cfg(feature = "s3")]
    {
        let s3_url = Url::parse("s3://test-bucket/path").unwrap();
        let mut s3_options = HashMap::new();
        s3_options.insert("aws_region".to_string(), "us-east-1".to_string());
        s3_options.insert("aws_access_key_id".to_string(), "test_key".to_string());
        s3_options.insert(
            "aws_secret_access_key".to_string(),
            "test_secret".to_string(),
        );

        // This will likely fail auth, but should not fail parsing
        let result = registry.create_store(&s3_url, &s3_options);
        println!("S3 store creation result: {:?}", result.is_ok());
    }

    #[cfg(feature = "gcs")]
    {
        let gcs_url = Url::parse("gs://test-bucket/path").unwrap();
        let mut gcs_options = HashMap::new();
        gcs_options.insert(
            "google_service_account".to_string(),
            "/fake/path/to/service-account.json".to_string(),
        );

        let result = registry.create_store(&gcs_url, &gcs_options);
        println!("GCS store creation result: {:?}", result.is_ok());
    }

    #[cfg(feature = "azure")]
    {
        let azure_url = Url::parse("azure://account.blob.core.windows.net/container/path").unwrap();
        let mut azure_options = HashMap::new();
        azure_options.insert(
            "azure_storage_account_name".to_string(),
            "testaccount".to_string(),
        );
        azure_options.insert(
            "azure_storage_account_key".to_string(),
            "fake_key".to_string(),
        );

        let result = registry.create_store(&azure_url, &azure_options);
        println!("Azure store creation result: {:?}", result.is_ok());
    }

    // Test unsupported scheme
    let unsupported_url = Url::parse("ftp://example.com").unwrap();
    assert!(registry.create_store(&unsupported_url, &options).is_err());
}

#[cfg(feature = "s3")]
#[test]
fn test_s3_configuration_options() {
    use aqueducts_core::store::S3Provider;

    let provider = S3Provider;
    let url = Url::parse("s3://test-bucket/path").unwrap();

    // Test various S3 configuration options
    let test_cases = vec![
        // Basic auth
        vec![
            ("aws_access_key_id", "AKIAIOSFODNN7EXAMPLE"),
            (
                "aws_secret_access_key",
                "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            ),
            ("aws_region", "us-west-2"),
        ],
        // With endpoint
        vec![
            ("aws_access_key_id", "test_key"),
            ("aws_secret_access_key", "test_secret"),
            ("aws_region", "us-east-1"),
            ("aws_endpoint", "http://localhost:9000"),
            ("aws_allow_http", "true"),
        ],
        // With session token
        vec![
            ("aws_access_key_id", "test_key"),
            ("aws_secret_access_key", "test_secret"),
            ("aws_session_token", "test_token"),
            ("aws_region", "eu-west-1"),
        ],
    ];

    for (i, case) in test_cases.iter().enumerate() {
        let options: HashMap<String, String> = case
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();

        let result = provider.create_store(&url, &options);
        println!("S3 test case {}: {:?}", i + 1, result.is_ok());

        // We expect these to either succeed (if credentials are somehow valid)
        // or fail with auth errors (not parsing errors)
        match result {
            Ok(_) => println!("  Store created successfully"),
            Err(e) => {
                let error_str = e.to_string();
                // Should not be a parsing or unsupported scheme error
                assert!(!error_str.contains("Unsupported"));
                assert!(!error_str.contains("parsing"));
                println!("  Expected auth/config error: {}", error_str);
            }
        }
    }
}

#[test]
fn test_error_handling_scenarios() {
    let registry = ObjectStoreRegistry::new();
    let options = HashMap::new();

    // Test invalid URLs/schemes
    let invalid_cases = vec![
        "ftp://example.com",
        "http://example.com",
        "invalid-scheme://test",
    ];

    for invalid_url_str in invalid_cases {
        let url = Url::parse(invalid_url_str).unwrap();
        let result = registry.create_store(&url, &options);

        assert!(result.is_err());
        let error = result.unwrap_err();
        let scheme = url.scheme();
        if error.to_string().contains(scheme) {
            println!("✓ Correctly rejected unsupported scheme: {}", scheme);
        } else {
            panic!(
                "Expected UnsupportedScheme error with '{}', got: {:?}",
                scheme, error
            );
        }
    }
}

#[test]
fn test_scheme_support_detection() {
    use aqueducts_core::store::LocalFileProvider;

    let local = LocalFileProvider;

    // Test scheme detection
    assert!(local.supports_scheme("file"));
    assert!(local.supports_scheme("memory"));
    assert!(!local.supports_scheme("s3"));
    assert!(!local.supports_scheme("gs"));
    assert!(!local.supports_scheme("azure"));
    assert!(!local.supports_scheme("http"));

    #[cfg(feature = "s3")]
    {
        use aqueducts_core::store::S3Provider;
        let s3 = S3Provider;
        assert!(s3.supports_scheme("s3"));
        assert!(s3.supports_scheme("s3a"));
        assert!(!s3.supports_scheme("file"));
        assert!(!s3.supports_scheme("gs"));
    }

    #[cfg(feature = "gcs")]
    {
        use aqueducts_core::store::GcsProvider;
        let gcs = GcsProvider;
        assert!(gcs.supports_scheme("gs"));
        assert!(gcs.supports_scheme("gcs"));
        assert!(!gcs.supports_scheme("file"));
        assert!(!gcs.supports_scheme("s3"));
    }

    #[cfg(feature = "azure")]
    {
        use aqueducts_core::store::AzureProvider;
        let azure = AzureProvider;
        assert!(azure.supports_scheme("az"));
        assert!(azure.supports_scheme("azure"));
        assert!(azure.supports_scheme("abfs"));
        assert!(azure.supports_scheme("abfss"));
        assert!(!azure.supports_scheme("file"));
        assert!(!azure.supports_scheme("s3"));
    }
}
