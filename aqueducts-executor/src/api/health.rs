use axum::{routing::get, Json, Router};
use serde::Serialize;

use crate::ApiContextRef;

pub fn router() -> Router<ApiContextRef> {
    Router::new().route("/", get(health_check))
}

#[derive(Serialize)]
struct HealthCheckResponse {
    status: String,
}

async fn health_check() -> Json<HealthCheckResponse> {
    let response = HealthCheckResponse {
        status: "OK".to_string(),
    };

    response.into()
}
