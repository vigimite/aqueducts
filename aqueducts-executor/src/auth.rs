use axum::{
    extract::{Request, State},
    middleware::Next,
    response::Response,
};
use std::sync::Arc;
use tracing::{debug, warn};

use crate::{error::ExecutorError, AppState};

/// The custom header for API key authentication
const X_API_KEY_HEADER: &str = "X-API-Key";

/// Middleware function for API key authentication
pub async fn api_key_auth(
    State(state): State<Arc<AppState>>,
    req: Request,
    next: Next,
) -> Result<Response, ExecutorError> {
    let api_key_header = req.headers().get(X_API_KEY_HEADER);

    if let Some(header) = api_key_header {
        let header_value = header.to_str().unwrap_or("");

        if header_value == state.api_key {
            debug!("API key authentication successful via X-API-Key header");
            return Ok(next.run(req).await);
        }
    }

    warn!("Authentication failed: No valid API key provided");
    Err(ExecutorError::AuthenticationFailed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::Body,
        http::{Request, StatusCode},
        middleware::from_fn_with_state,
        response::Response,
    };
    use http_body_util::Empty;
    use rstest::rstest;
    use tower::{service_fn, Layer, Service, ServiceExt};

    // Helper function to mock a request with optional API key
    fn mock_request(api_key: Option<&str>) -> Request<Body> {
        let mut builder = Request::builder();
        if let Some(key) = api_key {
            builder = builder.header(X_API_KEY_HEADER, key);
        }
        builder
            .uri("https://example.com")
            .body(Body::empty())
            .unwrap()
    }

    // Mock service for testing middleware
    async fn test_service(_req: Request<Body>) -> Result<Response, std::convert::Infallible> {
        Ok(Response::builder()
            .status(StatusCode::OK)
            .body(Body::new(Empty::new()))
            .unwrap())
    }

    #[tokio::test]
    async fn test_auth_with_valid_api_key() {
        let state = Arc::new(AppState {
            api_key: "test-api-key".to_string(),
            executor_id: "test-executor-id".to_string(),
            max_memory_gb: None,
            _server_url: None,
            execution_state_manager: Arc::new(crate::executor::ExecutionStateManager::new()),
        });
        let middleware = from_fn_with_state(state, api_key_auth);
        let svc = service_fn(test_service);
        let mut service = middleware.layer(svc);
        let request = mock_request(Some("test-api-key"));

        let response = service.ready().await.unwrap().call(request).await.unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[rstest]
    #[case::invalid_api_key(Some("wrong-api-key"), "Invalid API key")]
    #[case::missing_api_key(None, "Missing API key")]
    #[tokio::test]
    async fn test_auth_failures(#[case] api_key: Option<&str>, #[case] test_name: &str) {
        let state = Arc::new(AppState {
            api_key: "test-api-key".to_string(),
            executor_id: "test-executor-id".to_string(),
            max_memory_gb: None,
            _server_url: None,
            execution_state_manager: Arc::new(crate::executor::ExecutionStateManager::new()),
        });

        let middleware = from_fn_with_state(state, api_key_auth);
        let svc = service_fn(test_service);
        let mut service = middleware.layer(svc);
        let request = mock_request(api_key);

        let response = service.ready().await.unwrap().call(request).await;

        match response {
            Ok(resp) => {
                assert_eq!(
                    resp.status(),
                    StatusCode::UNAUTHORIZED,
                    "{} should result in UNAUTHORIZED status",
                    test_name
                );
            }
            Err(_) => panic!("Expected a response with UNAUTHORIZED status, got an error"),
        }
    }
}
