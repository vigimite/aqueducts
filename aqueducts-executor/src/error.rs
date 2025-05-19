use axum::{
    http::{header, StatusCode},
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
}

impl IntoResponse for ExecutorError {
    fn into_response(self) -> Response {
        let (status, error_response) = match &self {
            ExecutorError::AuthenticationFailed => {
                let response = ErrorResponse {
                    error: self.to_string(),
                };
                (StatusCode::UNAUTHORIZED, response)
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

        response
    }
}
