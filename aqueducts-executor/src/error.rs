use axum::{
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use serde::Serialize;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ExecutorError {
    #[error("Authentication failed")]
    AuthenticationFailed,

    #[error("Execution failed: {0}")]
    ExecutionFailed(String),

    #[error("A pipeline is already running (ID: {execution_id}). Please retry after {retry_after} seconds")]
    AlreadyRunning {
        execution_id: String,
        retry_after: u64,
    },

    #[error("Aqueducts pipeline error: {0}")]
    AqueductsError(#[from] aqueducts::error::Error),

    #[error("JSON deserialization error: {0}")]
    SerdeJsonError(#[from] serde_json::Error),

    #[error("DataFusion error: {0}")]
    DataFusionError(#[from] datafusion::error::DataFusionError),
}

#[derive(Serialize)]
struct ErrorResponse {
    error: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    execution_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    retry_after: Option<u64>,
}

impl IntoResponse for ExecutorError {
    fn into_response(self) -> Response {
        let (status, headers, error_response) = match &self {
            ExecutorError::AuthenticationFailed => {
                let response = ErrorResponse {
                    error: self.to_string(),
                    execution_id: None,
                    retry_after: None,
                };
                (StatusCode::UNAUTHORIZED, HeaderMap::new(), response)
            }
            ExecutorError::SerdeJsonError(_) => {
                let response = ErrorResponse {
                    error: self.to_string(),
                    execution_id: None,
                    retry_after: None,
                };
                (StatusCode::BAD_REQUEST, HeaderMap::new(), response)
            }
            ExecutorError::ExecutionFailed(_)
            | ExecutorError::AqueductsError(_)
            | ExecutorError::DataFusionError(_) => {
                let response = ErrorResponse {
                    error: self.to_string(),
                    execution_id: None,
                    retry_after: None,
                };
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    HeaderMap::new(),
                    response,
                )
            }
            ExecutorError::AlreadyRunning {
                execution_id,
                retry_after,
            } => {
                let mut headers = HeaderMap::new();

                // Add standard rate limit headers
                headers.insert(
                    header::RETRY_AFTER,
                    retry_after.to_string().parse().unwrap(),
                );
                headers.insert(
                    header::HeaderName::from_static("x-ratelimit-limit"),
                    "1".parse().unwrap(),
                );
                headers.insert(
                    header::HeaderName::from_static("x-ratelimit-remaining"),
                    "0".parse().unwrap(),
                );
                headers.insert(
                    header::HeaderName::from_static("x-ratelimit-reset"),
                    retry_after.to_string().parse().unwrap(),
                );

                let response = ErrorResponse {
                    error: self.to_string(),
                    execution_id: Some(execution_id.clone()),
                    retry_after: Some(*retry_after),
                };

                (StatusCode::TOO_MANY_REQUESTS, headers, response)
            }
        };

        // Convert the error response to JSON
        let body = serde_json::to_string(&error_response)
            .unwrap_or_else(|_| format!("{{\"error\": \"{}\"}}", self));

        // Build a response with proper headers and JSON body
        let mut response = Response::new(body.into());
        *response.status_mut() = status;

        // Add Content-Type header
        response.headers_mut().insert(
            header::CONTENT_TYPE,
            header::HeaderValue::from_static("application/json"),
        );

        // Add any additional headers
        for (key, value) in headers.iter() {
            response.headers_mut().insert(key, value.clone());
        }

        response
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body;
    use rstest::rstest;

    #[rstest]
    #[case(
        ExecutorError::AuthenticationFailed, 
        StatusCode::UNAUTHORIZED, 
        vec![]
    )]
    #[case(
        ExecutorError::ExecutionFailed("Test error".to_string()), 
        StatusCode::INTERNAL_SERVER_ERROR, 
        vec![]
    )]
    #[case(
        ExecutorError::AlreadyRunning { 
            execution_id: "test-id".to_string(), 
            retry_after: 30 
        }, 
        StatusCode::TOO_MANY_REQUESTS, 
        vec![
            (header::RETRY_AFTER.to_string(), "30".to_string()),
            ("x-ratelimit-limit".to_string(), "1".to_string()),
            ("x-ratelimit-remaining".to_string(), "0".to_string()),
            ("x-ratelimit-reset".to_string(), "30".to_string())
        ]
    )]
    fn test_error_into_response(
        #[case] error: ExecutorError, 
        #[case] expected_status: StatusCode, 
        #[case] expected_headers: Vec<(String, String)>
    ) {
        let response = error.into_response();
        assert_eq!(response.status(), expected_status);
        
        for (header_name, header_value) in expected_headers {
            let header = response.headers().get(header_name).expect("Header not found");
            assert_eq!(header.to_str().unwrap(), header_value);
        }
        
        assert_eq!(
            response.headers().get(header::CONTENT_TYPE).unwrap(),
            "application/json"
        );
    }

    #[tokio::test]
    async fn test_already_running_error_serializes_execution_id() {
        let error = ExecutorError::AlreadyRunning {
            execution_id: "test-id".to_string(),
            retry_after: 30,
        };
        
        let response = error.into_response();
        let body = response.into_body();
        
        // Use a large limit for testing (16MB)
        let body_bytes = body::to_bytes(body, 16 * 1024 * 1024).await.unwrap();
        let body_str = String::from_utf8(body_bytes.to_vec()).unwrap();
        
        assert!(body_str.contains("\"execution_id\":\"test-id\""));
        assert!(body_str.contains("\"retry_after\":30"));
    }

    // Convert individual error message tests to a table-driven test
    #[rstest]
    #[case(
        ExecutorError::AuthenticationFailed,
        "Authentication failed"
    )]
    #[case(
        ExecutorError::ExecutionFailed("Test failure".to_string()),
        "Execution failed: Test failure"
    )]
    #[case(
        ExecutorError::AlreadyRunning {
            execution_id: "test-id".to_string(),
            retry_after: 30,
        },
        "A pipeline is already running (ID: test-id). Please retry after 30 seconds"
    )]
    fn test_error_messages(#[case] error: ExecutorError, #[case] expected_message: &str) {
        assert_eq!(error.to_string(), expected_message);
    }
}
