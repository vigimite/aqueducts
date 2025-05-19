use axum::Router;
use tower_http::trace::{DefaultOnFailure, TraceLayer};
use tracing::Level;

use crate::ApiContextRef;

mod auth;
mod health;
mod websockets;

pub fn router(context: ApiContextRef) -> Router<ApiContextRef> {
    let public_routes = Router::new().nest("/api/health", health::router());

    let protected_routes = Router::new().nest("/ws", websockets::router()).layer(
        axum::middleware::from_fn_with_state(context, auth::require_api_key),
    );

    Router::new()
        .merge(public_routes)
        .merge(protected_routes)
        .layer(TraceLayer::new_for_http().on_failure(DefaultOnFailure::new().level(Level::ERROR)))
}
