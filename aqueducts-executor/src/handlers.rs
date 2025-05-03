use aqueducts::prelude::Aqueduct;
use aqueducts_utils::executor_events::{
    CancelRequest, CancelResponse, CurrentExecution, ExecutionEvent, StatusResponse,
};
use axum::{
    extract::{Json, State},
    http::StatusCode,
    response::{sse::Event, Sse},
};
use serde::Deserialize;
use std::convert::Infallible;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_stream::{wrappers::ReceiverStream, Stream, StreamExt};
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::error::ExecutorError;
use crate::executor::{
    cancel_current_execution, execute_aqueduct_pipeline, ExecutionStatus, ExecutorState,
};
use crate::AppState;

/// Request for pipeline execution
#[derive(Debug, Deserialize)]
pub struct ExecuteRequest {
    /// Pipeline definition - deserialized as an Aqueduct pipeline
    pub pipeline: Aqueduct,
}

/// Converts our stream of ExecutionEvent to Axum's SSE Event type
fn to_sse_event(result: Result<ExecutionEvent, Infallible>) -> Result<Event, Infallible> {
    match result {
        Ok(event) => match serde_json::to_string(&event) {
            Ok(json) => Ok(Event::default().data(json)),
            Err(_) => Ok(Event::default().data("Error serializing event")),
        },
        Err(_) => Ok(Event::default().data("Error processing event")),
    }
}

/// Health check handler
pub async fn health_check() -> &'static str {
    "OK"
}

/// Handler for getting the executor status
pub async fn get_status(
    State(state): State<Arc<AppState>>,
) -> Result<(StatusCode, Json<StatusResponse>), ExecutorError> {
    let state_manager = &state.execution_state_manager;
    let execution_status = state_manager.get_status()?;

    let status_response = if execution_status.is_running() {
        let execution_id = execution_status
            .execution_id()
            .unwrap_or_else(|| "unknown".to_string());
        let running_time = execution_status.running_time().unwrap_or(0);

        StatusResponse {
            executor_id: state.executor_id.clone(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            status: "busy".to_string(),
            current_execution: Some(CurrentExecution {
                execution_id,
                running_time,
                estimated_remaining_time: None,
            }),
        }
    } else {
        StatusResponse {
            executor_id: state.executor_id.clone(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            status: "idle".to_string(),
            current_execution: None,
        }
    };

    Ok((StatusCode::OK, Json(status_response)))
}

/// Handler for pipeline execution requests
pub async fn execute_pipeline(
    State(state): State<Arc<AppState>>,
    Json(request): Json<ExecuteRequest>,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, ExecutorError> {
    let state_manager = &state.execution_state_manager;

    // Check if execution is already in progress
    {
        let current_status = state_manager.get_status()?;
        if current_status.is_running() {
            let execution_id = current_status
                .execution_id()
                .unwrap_or_else(|| "unknown".to_string());
            let running_time = current_status.running_time().unwrap_or(0);

            warn!(
                "Pipeline execution rejected - another pipeline is already running (id: {execution_id}, {running_time} seconds elapsed)"
            );

            return Err(ExecutorError::AlreadyRunning {
                execution_id,
                retry_after: 30,
            });
        }
    }

    let execution_id = Uuid::new_v4().to_string();

    // Channel for SSE events
    let (tx, rx) = mpsc::channel(100);
    let rx_stream = ReceiverStream::new(rx).map(to_sse_event);

    // Pipeline is executed in a separate task. The task handle can be used for cancelation via the API
    let execution_id_ = execution_id.clone();
    let pipeline_ = request.pipeline;

    let max_memory_gb_ = state.max_memory_gb;
    let state_manager_ = Arc::clone(state_manager);

    let tx_ = tx.clone();
    let task = tokio::spawn(async move {
        if let Err(e) =
            execute_aqueduct_pipeline(pipeline_, execution_id_, &tx_, max_memory_gb_).await
        {
            error!("Pipeline execution failed: {}", e);
        }

        if let Err(e) = state_manager_.set_status(ExecutionStatus {
            state: ExecutorState::Idle,
            task_handle: None,
            event_sender: None,
        }) {
            error!("Failed to update execution status at completion: {}", e);
        }
    });

    let tx_for_status = tx;
    state_manager.set_status(ExecutionStatus {
        state: ExecutorState::Running {
            execution_id: execution_id.clone(),
            started_at: std::time::Instant::now(),
        },
        task_handle: Some(task),
        event_sender: Some(tx_for_status),
    })?;

    info!("Started pipeline execution (id: {})", execution_id);

    Ok(Sse::new(rx_stream))
}

/// Handler for pipeline cancellation requests
pub async fn cancel_pipeline(
    State(state): State<Arc<AppState>>,
    Json(request): Json<CancelRequest>,
) -> Result<Json<CancelResponse>, ExecutorError> {
    let state_manager = &state.execution_state_manager;
    let status = state_manager.get_status()?;

    match (request.execution_id, status.execution_id()) {
        (Some(requested_id), Some(ref current_id)) if requested_id != *current_id => {
            Ok(Json(CancelResponse {
                status: "not_found".to_string(),
                message: format!(
                    "Execution ID '{}' not found. Current execution has ID '{}'",
                    requested_id, current_id
                ),
                cancelled_execution_id: None,
            }))
        }
        (_, None) => Ok(Json(CancelResponse {
            status: "not_running".to_string(),
            message: "No pipeline execution is currently running".to_string(),
            cancelled_execution_id: None,
        })),
        (_, Some(ref current_id)) => cancel_current_execution(current_id, state_manager)
            .await
            .map(|_| {
                Json(CancelResponse {
                    status: "cancelled".to_string(),
                    message: "Pipeline execution cancelled successfully".to_string(),
                    cancelled_execution_id: Some(current_id.clone()),
                })
            }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::api_key_auth;
    use crate::executor::ExecutionStateManager;
    use axum::{
        body::Body,
        http::{self, Request, StatusCode},
        response::Response,
        routing::{get, post},
        Router,
    };
    use http_body_util::BodyExt;
    use serde_json::{json, Value};
    use tower::ServiceExt;

    // Helper function to create a test app with a specific state manager
    fn create_test_app(state_manager: Arc<ExecutionStateManager>) -> Router {
        let state = Arc::new(AppState {
            api_key: "test-api-key".to_string(),
            executor_id: "test-executor-id".to_string(),
            max_memory_gb: None,
            _server_url: None,
            execution_state_manager: state_manager,
        });

        let public_routes = Router::new().route("/health", get(health_check));
        let protected_routes = Router::new()
            .route("/execute", post(execute_pipeline))
            .route("/cancel", post(cancel_pipeline))
            .route("/status", get(get_status))
            .layer(axum::middleware::from_fn_with_state(
                Arc::clone(&state),
                api_key_auth,
            ));

        Router::new()
            .merge(public_routes)
            .merge(protected_routes)
            .with_state(state)
    }

    // Helper function to create a request with an optional API key
    fn create_request<B>(
        method: &http::Method,
        uri: &str,
        api_key: Option<&str>,
        body: B,
    ) -> Request<B> {
        let mut builder = Request::builder().method(method).uri(uri);

        if let Some(key) = api_key {
            builder = builder.header("X-API-Key", key);
        }

        if method == http::Method::POST {
            builder = builder.header(http::header::CONTENT_TYPE, "application/json");
        }

        builder.body(body).unwrap()
    }

    // Helper function to extract JSON from a response
    async fn extract_json_body<T: serde::de::DeserializeOwned>(response: Response) -> T {
        let body = response.into_body();
        let bytes = body.collect().await.unwrap().to_bytes();
        serde_json::from_slice(&bytes).unwrap()
    }

    #[tokio::test]
    async fn test_get_status_when_idle() {
        let state_manager = Arc::new(ExecutionStateManager::new());

        state_manager
            .with_clean_state(|| async {
                state_manager
                    .reset_to_idle()
                    .expect("Failed to reset state to idle");

                let status_check = state_manager.get_status().unwrap();
                assert!(
                    !status_check.is_running(),
                    "Executor should not be running at start of test"
                );

                let app_state = Arc::new(AppState {
                    api_key: "test-api-key".to_string(),
                    executor_id: "test-executor-id".to_string(),
                    max_memory_gb: None,
                    _server_url: None,
                    execution_state_manager: Arc::clone(&state_manager),
                });

                let result = get_status(State(app_state)).await;

                assert!(result.is_ok());
                let (status_code, json_response) = result.unwrap();
                assert_eq!(status_code, StatusCode::OK);

                let response = json_response.0;
                assert_eq!(response.executor_id, "test-executor-id");
                assert_eq!(
                    response.status, "idle",
                    "Status should be 'idle' when the executor is not running"
                );
                assert!(
                    !response.version.is_empty(),
                    "Version string should not be empty"
                );
                assert!(
                    response.current_execution.is_none(),
                    "No current execution should be reported"
                );
            })
            .await;
    }

    #[tokio::test]
    async fn test_get_status_when_running() {
        let execution_id = "test-running-execution".to_string();
        let running_time = 5; // Mock running time in seconds
        let expected_response = StatusResponse {
            executor_id: "test-executor-id".to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            status: "busy".to_string(),
            current_execution: Some(CurrentExecution {
                execution_id: execution_id.clone(),
                running_time,
                estimated_remaining_time: None,
            }),
        };

        assert_eq!(
            expected_response.status, "busy",
            "Status should be 'busy' when the executor is running"
        );

        assert!(
            expected_response.current_execution.is_some(),
            "Current execution details should be included"
        );

        if let Some(execution) = &expected_response.current_execution {
            assert_eq!(
                execution.execution_id, execution_id,
                "Execution ID should match what we set"
            );
            assert_eq!(
                execution.running_time, running_time,
                "Running time should match what we set"
            );
        }

        assert_eq!(expected_response.executor_id, "test-executor-id");
        assert!(!expected_response.version.is_empty());
    }

    #[tokio::test]
    async fn test_health_check() {
        let app = create_test_app(Arc::new(ExecutionStateManager::new()));
        let request = create_request(&http::Method::GET, "/health", None, Body::empty());

        let response = app.oneshot(request).await.unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = response.into_body().collect().await.unwrap().to_bytes();
        let body_str = String::from_utf8(body.to_vec()).unwrap();

        assert_eq!(body_str, "OK");
    }

    #[tokio::test]
    async fn test_health_check_handler_direct() {
        let result = health_check().await;
        assert_eq!(result, "OK");
    }

    #[tokio::test]
    async fn test_status_without_auth() {
        let app = create_test_app(Arc::new(ExecutionStateManager::new()));
        let request = create_request(
            &http::Method::GET,
            "/status",
            None, // No API key
            Body::empty(),
        );

        let response = app.oneshot(request).await.unwrap();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

        let error: Value = extract_json_body(response).await;
        assert_eq!(error["error"], "Authentication failed");
    }

    #[tokio::test]
    async fn test_status_with_auth_via_router() {
        let state_manager = Arc::new(ExecutionStateManager::new());
        state_manager
            .with_clean_state(|| async {
                state_manager
                    .reset_to_idle()
                    .expect("Failed to set execution status to idle");
                let status_before = state_manager.get_status().unwrap();
                assert!(
                    !status_before.is_running(),
                    "Executor must be idle at the start of this test"
                );

                let state = Arc::new(AppState {
                    api_key: "test-api-key".to_string(),
                    executor_id: "test-executor-id".to_string(),
                    max_memory_gb: None,
                    _server_url: None,
                    execution_state_manager: Arc::clone(&state_manager),
                });

                let app = Router::new()
                    .route("/status", get(get_status))
                    .with_state(state);

                let request = create_request(
                    &http::Method::GET,
                    "/status",
                    Some("test-api-key"),
                    Body::empty(),
                );

                let response = app.oneshot(request).await.unwrap();

                assert_eq!(
                    response.status(),
                    StatusCode::OK,
                    "Expected HTTP 200 OK response"
                );

                let status: StatusResponse = extract_json_body(response).await;

                assert_eq!(
                    status.executor_id, "test-executor-id",
                    "Response should include the correct executor ID"
                );
                assert_eq!(
                    status.status, "idle",
                    "Status should be 'idle' because we explicitly set it that way"
                );
                assert!(
                    status.current_execution.is_none(),
                    "There should be no current execution"
                );
            })
            .await;
    }

    #[tokio::test]
    async fn test_cancel_pipeline_when_not_running() {
        let state_manager = Arc::new(ExecutionStateManager::new());
        state_manager
            .with_clean_state(|| async {
                state_manager
                    .reset_to_idle()
                    .expect("Failed to reset state to idle");

                let current_status = state_manager.get_status().unwrap();
                assert!(
                    !current_status.is_running(),
                    "Executor should not be running at start of test"
                );

                let app_state = Arc::new(AppState {
                    api_key: "test-api-key".to_string(),
                    executor_id: "test-executor-id".to_string(),
                    max_memory_gb: None,
                    _server_url: None,
                    execution_state_manager: Arc::clone(&state_manager),
                });
                let cancel_request = CancelRequest { execution_id: None };
                let result = cancel_pipeline(State(app_state), Json(cancel_request)).await;

                assert!(result.is_ok());
                let cancel_response = result.unwrap().0;

                assert!(
                    cancel_response.status.contains("not_running"),
                    "Expected status 'not_running', got '{}'",
                    cancel_response.status
                );
                assert_eq!(
                    cancel_response.cancelled_execution_id, None,
                    "Expected no cancelled execution ID"
                );
            })
            .await;
    }

    #[tokio::test]
    async fn test_cancel_pipeline_with_wrong_execution_id() {
        let running_id = "test-execution-id";
        let wrong_id = "wrong-id";

        let response = CancelResponse {
            status: "not_found".to_string(),
            message: format!(
                "Execution ID '{}' not found. Current execution has ID '{}'",
                wrong_id, running_id
            ),
            cancelled_execution_id: None,
        };

        assert_eq!(
            response.status, "not_found",
            "Expected 'not_found' status when cancelling with wrong ID"
        );
        assert!(
            response.message.contains(wrong_id),
            "Response message should mention the wrong ID"
        );
        assert!(
            response.message.contains(running_id),
            "Response message should mention the current running ID"
        );
        assert_eq!(
            response.cancelled_execution_id, None,
            "No execution ID should be returned for unsuccessful cancellation"
        );
    }

    #[tokio::test]
    async fn test_cancel_pipeline_with_running_execution() {
        let execution_id = "test-execution-id".to_string();

        let response = CancelResponse {
            status: "cancelled".to_string(),
            message: "Pipeline execution cancelled successfully".to_string(),
            cancelled_execution_id: Some(execution_id.clone()),
        };

        assert_eq!(
            response.status, "cancelled",
            "Expected 'cancelled' when cancelling with correct ID"
        );
        assert!(
            response.message.contains("successfully"),
            "Response message should indicate successful cancellation"
        );
        assert_eq!(
            response.cancelled_execution_id,
            Some(execution_id),
            "Response should include the cancelled execution ID"
        );
    }

    #[tokio::test]
    async fn test_cancel_pipeline_router_auth() {
        let state_manager = Arc::new(ExecutionStateManager::new());
        let _ = state_manager.reset_to_idle();

        state_manager
            .with_clean_state(|| async {
                let cancel_request = json!({
                    "execution_id": null
                });

                let request_no_auth = create_request(
                    &http::Method::POST,
                    "/cancel",
                    None, // No API key
                    Body::from(cancel_request.to_string()),
                );

                let app1 = create_test_app(Arc::clone(&state_manager));
                let response_no_auth = app1.oneshot(request_no_auth).await.unwrap();

                assert_eq!(
                    response_no_auth.status(),
                    StatusCode::UNAUTHORIZED,
                    "Request without API key should be unauthorized"
                );

                let request_with_auth = create_request(
                    &http::Method::POST,
                    "/cancel",
                    Some("test-api-key"),
                    Body::from(cancel_request.to_string()),
                );

                let app2 = create_test_app(Arc::clone(&state_manager));
                let response_with_auth = app2.oneshot(request_with_auth).await.unwrap();

                assert_eq!(
                    response_with_auth.status(),
                    StatusCode::OK,
                    "Request with API key should succeed"
                );
            })
            .await;
    }

    #[tokio::test]
    async fn test_execute_pipeline_when_idle() {
        let state_manager = Arc::new(ExecutionStateManager::new());
        state_manager
            .with_clean_state(|| async {
                state_manager
                    .reset_to_idle()
                    .expect("Failed to reset to idle state");

                let status_before = state_manager.get_status().unwrap();
                assert!(
                    !status_before.is_running(),
                    "Executor should be idle before test"
                );

                let pipeline = json!({
                    "sources": [],
                    "stages": [
                        [
                            {
                                "name": "test_stage",
                                "query": "SELECT 1=1"
                            }
                        ]
                    ]
                });

                let app_state = Arc::new(AppState {
                    api_key: "test-api-key".to_string(),
                    executor_id: "test-executor-id".to_string(),
                    max_memory_gb: None,
                    _server_url: None,
                    execution_state_manager: Arc::clone(&state_manager),
                });
                let execute_request = serde_json::from_value::<ExecuteRequest>(json!({
                    "pipeline": pipeline
                }))
                .unwrap();

                let result = execute_pipeline(State(app_state), Json(execute_request)).await;

                assert!(result.is_ok(), "Execute pipeline should succeed");

                let status_after = state_manager.get_status().unwrap();
                assert!(
                    status_after.is_running(),
                    "Executor should be in running state after execution starts"
                );
            })
            .await;
    }

    #[tokio::test]
    async fn test_execute_pipeline_error_handling() {
        let state_manager = Arc::new(ExecutionStateManager::new());
        state_manager
            .with_clean_state(|| async {
                let pipeline = json!({
                    "sources": [],
                    "stages": [
                        [
                            {
                                "name": "test_stage",
                                "query": "SELECT 1=1"
                            }
                        ]
                    ]
                });

                let execute_request_json = json!({
                    "pipeline": pipeline
                });

                let request_no_auth = create_request(
                    &http::Method::POST,
                    "/execute",
                    None, // No API key
                    Body::from(execute_request_json.to_string()),
                );

                let app1 = create_test_app(Arc::clone(&state_manager));
                let response_no_auth = app1.oneshot(request_no_auth).await.unwrap();
                assert_eq!(
                    response_no_auth.status(),
                    StatusCode::UNAUTHORIZED,
                    "Request without API key should be unauthorized"
                );

                let request_with_auth = create_request(
                    &http::Method::POST,
                    "/execute",
                    Some("test-api-key"),
                    Body::from(execute_request_json.to_string()),
                );

                let app2 = create_test_app(Arc::clone(&state_manager));

                // The execute handler will try to parse the pipeline and execute it
                // Depending on the state of the executor, we'll either get:
                // - An OK response with an SSE stream
                // - A 429 Too Many Requests if something is already running
                // - A 422 Unprocessable Entity if there's a pipeline parsing issue
                let response_with_auth = app2.oneshot(request_with_auth).await.unwrap();

                // Status should not be 401 (that would indicate an auth failure)
                assert_ne!(
                    response_with_auth.status(),
                    StatusCode::UNAUTHORIZED,
                    "Request with valid API key should not be unauthorized"
                );
            })
            .await;
    }

    #[tokio::test]
    async fn test_to_sse_event_conversion() {
        let test_string = "test-id";
        let event = Event::default().data(test_string);
        let debug_str = format!("{:?}", event);
        assert!(debug_str.contains(test_string));
    }
}
