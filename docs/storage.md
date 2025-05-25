# Storage Configuration

Aqueducts provides a unified storage abstraction that automatically handles different storage backends including local files, cloud storage (AWS S3, Google Cloud Storage, Azure Blob Storage), and in-memory storage. The storage system is designed to work seamlessly across different environments with minimal configuration.

## Overview

The storage system in Aqueducts consists of:

- **Local Storage**: File system and in-memory storage (always available)
- **Cloud Storage**: AWS S3, Google Cloud Storage, Azure Blob Storage (feature-gated)
- **Automatic Provider Detection**: URL-based storage provider selection
- **Environment Variable Support**: Automatic configuration from standard environment variables
- **Feature Flags**: Compile-time inclusion of cloud provider dependencies

## Automatic Configuration

!!! info "Environment Variables First"
    **All cloud storage providers automatically read configuration from standard environment variables.** You only need to specify `storage_config` in your pipeline configuration when you want to override environment variables or provide additional settings.

    This means you can often use cloud storage with **no explicit configuration** in your pipeline files if your environment variables are properly set.

## Supported Storage Providers

### Local File System

Always available for local development and testing.

**Supported URL Schemes**: `file://`

**Example URLs**:
```
file:///path/to/file.csv
file:///tmp/output.parquet
file://./relative/path/data.json
```

**Configuration**: No additional configuration required.

### In-Memory Storage

Useful for testing and temporary data processing.

**Supported URL Schemes**: `memory://`

**Example URLs**:
```
memory://temp-table
memory://cache
```

**Configuration**: No additional configuration required.

### AWS S3

Requires the `s3` feature flag to be enabled.

**Supported URL Schemes**: `s3://`, `s3a://`

**Example URLs**:
```
s3://my-bucket/path/to/file.parquet
s3://data-lake/year=2024/month=01/data.csv
s3a://bucket/prefix/  (Hadoop-style URL)
```

#### S3 Environment Variables

S3 storage automatically reads from these standard AWS environment variables:

| Environment Variable    | Description                               | Example                                    |
|-------------------------|-------------------------------------------|--------------------------------------------|
| `AWS_ACCESS_KEY_ID`     | AWS access key ID                         | `AKIAIOSFODNN7EXAMPLE`                     |
| `AWS_SECRET_ACCESS_KEY` | AWS secret access key                     | `wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY` |
| `AWS_SESSION_TOKEN`     | AWS session token (temporary credentials) | `AQoEXAMPLE...`                            |
| `AWS_REGION`            | AWS region                                | `us-east-1`                                |
| `AWS_ENDPOINT_URL`      | Custom S3 endpoint                        | `http://localhost:9000`                    |
| `AWS_PROFILE`           | AWS profile name                          | `production`                               |

#### S3 Configuration Parameters

You can override environment variables or provide additional configuration using the `storage_config` map in your source or destination configuration:

=== "Authentication"

    | Parameter               | Description                                   | Example                                                                                                                                                                                                                                           |
    |-------------------------|-----------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
    | `aws_access_key_id`     | AWS access key ID                             | `AKIAIOSFODNN7EXAMPLE`                                                                                                                                                                                                                            |
    | `aws_secret_access_key` | AWS secret access key                         | `wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY`                                                                                                                                                                                                        |
    | `aws_session_token`     | AWS session token (for temporary credentials) | `AQoEXAMPLEH4aoAH0gNCAPyJxz4BlCFFxWNE1OPTgk5TthT+FvwqnKwRcOIfrRh3c/LTo6UDdyJwOOvEVPvLXCrrrUtdnniCEXAMPLE/IvU1dYUg2RVAJBanLiHb4IgRmpRV3zrkuWJOgQs8IZZaIv2BXIa2R4OlgkBN9bkUDNCJiBeb/AXlzBBko7b15fjrBs2+cTQtpZ3CYWFXG8C5zqx37wnOE49mRl/+OtkIKGO7fAE` |
    | `aws_region`            | AWS region                                    | `us-east-1`, `eu-west-1`                                                                                                                                                                                                                          |

=== "Alternative Credentials"

    | Parameter           | Description                       | Example                                    |
    |---------------------|-----------------------------------|--------------------------------------------|
    | `access_key_id`     | Alias for `aws_access_key_id`     | `AKIAIOSFODNN7EXAMPLE`                     |
    | `secret_access_key` | Alias for `aws_secret_access_key` | `wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY` |
    | `session_token`     | Alias for `aws_session_token`     | `AQoEXAMPLE...`                            |
    | `region`            | Alias for `aws_region`            | `us-west-2`                                |

=== "Advanced Configuration"

    | Parameter                          | Description                                    | Default | Example                 |
    |------------------------------------|------------------------------------------------|---------|-------------------------|
    | `aws_endpoint`                     | Custom S3 endpoint (for S3-compatible storage) | AWS S3  | `http://localhost:9000` |
    | `aws_allow_http`                   | Allow HTTP connections (disable HTTPS)         | `false` | `true`                  |
    | `aws_virtual_hosted_style_request` | Use virtual hosted-style requests              | `false` | `true`                  |
    | `aws_unsigned_payload`             | Use unsigned payload for requests              | `false` | `true`                  |
    | `aws_skip_signature`               | Skip request signing (for public buckets)      | `false` | `true`                  |
    | `aws_checksum_algorithm`           | Checksum algorithm for uploads                 | None    | `CRC32C`, `SHA256`      |
    | `aws_s3_express`                   | Enable S3 Express One Zone                     | `false` | `true`                  |
    | `aws_imdsv1_fallback`              | Allow IMDSv1 fallback for EC2 metadata         | `false` | `true`                  |

#### S3 Configuration Examples

=== "Environment Variables Only"

    ```bash
    # Set environment variables
    export AWS_ACCESS_KEY_ID="AKIAIOSFODNN7EXAMPLE"
    export AWS_SECRET_ACCESS_KEY="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
    export AWS_REGION="us-east-1"
    ```

    ```yaml
    # No storage_config needed!
    sources:
      - type: File
        name: sales_data
        location: s3://my-data-bucket/sales/2024/data.parquet
        file_type:
          type: Parquet
        # storage_config automatically read from environment
    ```

=== "Explicit Configuration"

    ```yaml
    # Override or supplement environment variables
    sources:
      - type: File
        name: sales_data
        location: s3://my-data-bucket/sales/2024/data.parquet
        file_type:
          type: Parquet
        storage_config:
          aws_access_key_id: "AKIAIOSFODNN7EXAMPLE"
          aws_secret_access_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
          aws_region: "us-east-1"
    ```

=== "MinIO (S3-Compatible)"

    ```yaml
    sources:
      - type: File
        name: local_s3_data
        location: s3://test-bucket/data.csv
        file_type:
          type: Csv
        storage_config:
          aws_access_key_id: "minioadmin"
          aws_secret_access_key: "minioadmin"
          aws_endpoint: "http://localhost:9000"
          aws_allow_http: "true"
          aws_region: "us-east-1"
    ```

=== "IAM Role (EC2/ECS)"

    ```yaml
    sources:
      - type: File
        name: secure_data
        location: s3://secure-bucket/confidential.parquet
        file_type:
          type: Parquet
        storage_config:
          aws_region: "us-west-2"
          # No credentials needed - will use IAM role
    ```

### Google Cloud Storage

Requires the `gcs` feature flag to be enabled.

**Supported URL Schemes**: `gs://`, `gcs://`

**Example URLs**:
```
gs://my-bucket/path/to/file.parquet
gcs://data-lake/processed/output.csv
```

#### GCS Environment Variables

GCS storage automatically reads from these standard Google Cloud environment variables:

| Environment Variable             | Description                       | Example                            |
|----------------------------------|-----------------------------------|------------------------------------|
| `GOOGLE_APPLICATION_CREDENTIALS` | Path to service account JSON file | `/path/to/service-account.json`    |
| `GOOGLE_SERVICE_ACCOUNT`         | Path to service account JSON file | `/path/to/service-account.json`    |
| `GOOGLE_SERVICE_ACCOUNT_KEY`     | Service account JSON as string    | `{"type": "service_account", ...}` |

#### GCS Configuration Parameters

You can override environment variables or provide additional configuration:

| Parameter                        | Description                        | Example                            |
|----------------------------------|------------------------------------|------------------------------------|
| `google_service_account`         | Path to service account JSON file  | `/path/to/service-account.json`    |
| `google_service_account_key`     | Service account JSON key as string | `{"type": "service_account", ...}` |
| `google_application_credentials` | Path to application credentials    | `/path/to/credentials.json`        |

#### GCS Configuration Examples

=== "Environment Variables Only"

    ```bash
    # Set environment variable
    export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account.json"
    ```

    ```yaml
    # No storage_config needed!
    sources:
      - type: File
        name: gcp_data
        location: gs://my-gcp-bucket/data/file.parquet
        file_type:
          type: Parquet
        # credentials automatically read from environment
    ```

=== "Explicit Service Account File"

    ```yaml
    sources:
      - type: File
        name: gcp_data
        location: gs://my-gcp-bucket/data/file.parquet
        file_type:
          type: Parquet
        storage_config:
          google_service_account: "/path/to/service-account.json"
    ```

=== "Service Account JSON"

    ```yaml
    sources:
      - type: File
        name: gcp_data
        location: gs://my-gcp-bucket/data/file.csv
        file_type:
          type: Csv
        storage_config:
          google_service_account_key: |
            {
              "type": "service_account",
              "project_id": "my-project",
              "private_key_id": "...",
              "private_key": "-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n",
              "client_email": "service-account@my-project.iam.gserviceaccount.com",
              "client_id": "...",
              "auth_uri": "https://accounts.google.com/o/oauth2/auth",
              "token_uri": "https://oauth2.googleapis.com/token"
            }
    ```

=== "Application Default Credentials"

    ```yaml
    sources:
      - type: File
        name: gcp_data
        location: gs://my-gcp-bucket/data/file.json
        file_type:
          type: Json
        storage_config:
          google_application_credentials: "/path/to/application-credentials.json"
    ```

### Azure Blob Storage

Requires the `azure` feature flag to be enabled.

**Supported URL Schemes**: `az://`, `azure://`, `abfs://`, `abfss://`

**Example URLs**:
```
azure://storageaccount.blob.core.windows.net/container/file.parquet
az://mystorageaccount.blob.core.windows.net/data/output.csv
abfs://container@storageaccount.dfs.core.windows.net/path/file.json
abfss://container@storageaccount.dfs.core.windows.net/secure/data.parquet
```

#### Azure Environment Variables

Azure storage automatically reads from these standard Azure environment variables:

| Environment Variable         | Description                        | Example                                |
|------------------------------|------------------------------------|----------------------------------------|
| `AZURE_STORAGE_ACCOUNT_NAME` | Storage account name               | `mystorageaccount`                     |
| `AZURE_STORAGE_ACCOUNT_KEY`  | Storage account access key         | `base64encodedkey==`                   |
| `AZURE_CLIENT_ID`            | Azure AD application client ID     | `12345678-1234-1234-1234-123456789012` |
| `AZURE_CLIENT_SECRET`        | Azure AD application client secret | `your-client-secret`                   |
| `AZURE_TENANT_ID`            | Azure AD tenant ID                 | `87654321-4321-4321-4321-210987654321` |
| `AZURE_STORAGE_SAS_TOKEN`    | Shared Access Signature token      | `?sv=2020-08-04&ss=...`                |

#### Azure Configuration Parameters

You can override environment variables or provide additional configuration:

=== "Authentication"

    | Parameter                     | Description                        | Example                                |
    |-------------------------------|------------------------------------|----------------------------------------|
    | `azure_storage_account_name`  | Storage account name               | `mystorageaccount`                     |
    | `azure_storage_account_key`   | Storage account access key         | `base64encodedkey==`                   |
    | `azure_storage_client_id`     | Azure AD application client ID     | `12345678-1234-1234-1234-123456789012` |
    | `azure_storage_client_secret` | Azure AD application client secret | `your-client-secret`                   |
    | `azure_storage_tenant_id`     | Azure AD tenant ID                 | `87654321-4321-4321-4321-210987654321` |

=== "Alternative Names"

    | Parameter       | Description                             | Example                                |
    |-----------------|-----------------------------------------|----------------------------------------|
    | `account_name`  | Alias for `azure_storage_account_name`  | `mystorageaccount`                     |
    | `account_key`   | Alias for `azure_storage_account_key`   | `base64encodedkey==`                   |
    | `client_id`     | Alias for `azure_storage_client_id`     | `12345678-1234-1234-1234-123456789012` |
    | `client_secret` | Alias for `azure_storage_client_secret` | `your-client-secret`                   |
    | `tenant_id`     | Alias for `azure_storage_tenant_id`     | `87654321-4321-4321-4321-210987654321` |

=== "Advanced Configuration"

    | Parameter                     | Description                       | Default       | Example                                                 |
    |-------------------------------|-----------------------------------|---------------|---------------------------------------------------------|
    | `azure_storage_use_emulator`  | Use Azure Storage Emulator        | `false`       | `true`                                                  |
    | `azure_storage_use_azure_cli` | Use Azure CLI credentials         | `false`       | `true`                                                  |
    | `azure_federated_token_file`  | Path to federated token file      | None          | `/var/run/secrets/azure/tokens/azure-identity-token`    |
    | `azure_use_fabric_endpoint`   | Use Microsoft Fabric endpoint     | `false`       | `true`                                                  |
    | `azure_msi_endpoint`          | Managed Service Identity endpoint | Auto-detected | `http://169.254.169.254/metadata/identity/oauth2/token` |
    | `azure_disable_tagging`       | Disable object tagging            | `false`       | `true`                                                  |

#### Azure Configuration Examples

=== "Environment Variables Only"

    ```bash
    # Set environment variables
    export AZURE_STORAGE_ACCOUNT_NAME="mystorageaccount"
    export AZURE_STORAGE_ACCOUNT_KEY="your-base64-encoded-account-key=="
    ```

    ```yaml
    # No storage_config needed!
    sources:
      - type: File
        name: azure_data
        location: azure://mystorageaccount.blob.core.windows.net/data/file.parquet
        file_type:
          type: Parquet
        # credentials automatically read from environment
    ```

=== "Explicit Account Key"

    ```yaml
    sources:
      - type: File
        name: azure_data
        location: azure://mystorageaccount.blob.core.windows.net/data/file.parquet
        file_type:
          type: Parquet
        storage_config:
          azure_storage_account_name: "mystorageaccount"
          azure_storage_account_key: "your-base64-encoded-account-key=="
    ```

=== "Azure AD Service Principal"

    ```yaml
    sources:
      - type: File
        name: azure_secure_data
        location: azure://storageaccount.blob.core.windows.net/secure/data.csv
        file_type:
          type: Csv
        storage_config:
          azure_storage_account_name: "storageaccount"
          azure_storage_client_id: "12345678-1234-1234-1234-123456789012"
          azure_storage_client_secret: "your-client-secret"
          azure_storage_tenant_id: "87654321-4321-4321-4321-210987654321"
    ```

=== "Azure CLI Authentication"

    ```yaml
    sources:
      - type: File
        name: cli_auth_data
        location: azure://storageaccount.blob.core.windows.net/data/file.json
        file_type:
          type: Json
        storage_config:
          azure_storage_account_name: "storageaccount"
          azure_storage_use_azure_cli: "true"
    ```

## Feature Flags

Aqueducts uses Cargo feature flags to include only the cloud storage providers you need, reducing compilation time and binary size.

### Available Features

| Feature | Description                  | Dependencies Added                     |
|---------|------------------------------|----------------------------------------|
| `s3`    | AWS S3 support               | AWS SDK, object_store AWS features     |
| `gcs`   | Google Cloud Storage support | GCP SDK, object_store GCP features     |
| `azure` | Azure Blob Storage support   | Azure SDK, object_store Azure features |

### Enabling Features

=== "Cargo.toml"

    ```toml
    [dependencies]
    aqueducts-core = { version = "0.9", features = ["s3", "gcs", "azure"] }
    ```

=== "Command Line"

    ```bash
    # Build with all cloud providers
    cargo build --features "s3,gcs,azure"
    
    # Build with only S3 support
    cargo build --features "s3"
    
    # Build with no cloud providers (local only)
    cargo build --no-default-features --features "yaml"
    ```

## Troubleshooting

### Common Issues

1. **Authentication Errors**: Verify your credentials and ensure they have the necessary permissions.

2. **Network Connectivity**: Check firewall settings and network connectivity to cloud storage endpoints.

3. **Region Mismatches**: Ensure your storage bucket and configured region match.

4. **URL Format**: Verify your storage URLs follow the correct scheme and format for each provider.
