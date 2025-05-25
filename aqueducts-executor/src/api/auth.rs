use axum::{
    extract::{Request, State},
    middleware::Next,
    response::Response,
};
use tracing::{debug, warn};

use crate::{error::ExecutorError, ApiContextRef};

/// The custom header for API key authentication
const X_API_KEY_HEADER: &str = "X-API-Key";

/// Middleware function for API key authentication
pub async fn require_api_key(
    State(context): State<ApiContextRef>,
    req: Request,
    next: Next,
) -> Result<Response, ExecutorError> {
    let api_key = req
        .headers()
        .get(X_API_KEY_HEADER)
        .and_then(|value| value.to_str().ok());

    if let Some(provided) = api_key {
        if provided == context.config.api_key {
            debug!("API key authentication successful via X-API-Key header");
            return Ok(next.run(req).await);
        }
    }

    warn!("Authentication failed: No valid API key provided");
    Err(ExecutorError::AuthenticationFailed)
}
