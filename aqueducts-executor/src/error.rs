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
        };

        let body = serde_json::to_string(&error_response)
            .unwrap_or_else(|_| format!("{{\"error\": \"{}\"}}", self));

        let mut response = Response::new(body.into());
        *response.status_mut() = status;

        response.headers_mut().insert(
            header::CONTENT_TYPE,
            header::HeaderValue::from_static("application/json"),
        );

        for (key, value) in headers.iter() {
            response.headers_mut().insert(key, value.clone());
        }

        response
    }
}
