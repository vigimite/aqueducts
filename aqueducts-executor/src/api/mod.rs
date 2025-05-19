use std::sync::Arc;

use aqueducts::protocol::{ClientMessage, ExecutorMessage};
use axum::{
    extract::{
        ws::{Message, WebSocket},
        State, WebSocketUpgrade,
    },
    response::IntoResponse,
    routing::{any, get},
    Json, Router,
};
use futures::{SinkExt, StreamExt};
use serde::Serialize;
use tokio::sync::Mutex;
use tower_http::trace::{DefaultOnFailure, TraceLayer};
use tracing::{debug, error, info, instrument, Instrument, Level};

use crate::{
    executor::{execute_pipeline, ExecutionManager},
    ApiContextRef,
};

mod auth;

pub fn router(context: ApiContextRef) -> Router<ApiContextRef> {
    let public_routes = Router::new().route("/api/health", get(health_check));

    let protected_routes = Router::new().route("/ws/connect", any(ws_handler)).layer(
        axum::middleware::from_fn_with_state(context, auth::require_api_key),
    );

    Router::new()
        .merge(public_routes)
        .merge(protected_routes)
        .layer(TraceLayer::new_for_http().on_failure(DefaultOnFailure::new().level(Level::ERROR)))
}

#[derive(Serialize)]
struct HealthCheckResponse {
    status: String,
}

async fn health_check() -> Json<HealthCheckResponse> {
    let response = HealthCheckResponse {
        status: "OK".to_string(),
    };

    Json(response)
}

#[instrument(skip(ws, context), fields(executor_id = %context.config.executor_id))]
async fn ws_handler(
    ws: WebSocketUpgrade,
    State(context): State<ApiContextRef>,
) -> impl IntoResponse {
    info!("Opening WebSocket connection");
    ws.on_upgrade(move |socket| {
        handle_socket(
            socket,
            context.manager.clone(),
            context.config.max_memory_gb,
        )
    })
}

#[instrument(skip(socket, manager), fields(max_memory_gb = ?max_memory_gb))]
async fn handle_socket(
    socket: WebSocket,
    manager: Arc<ExecutionManager>,
    max_memory_gb: Option<usize>,
) {
    let (sender, mut receiver) = socket.split();
    let sender = Arc::new(Mutex::new(sender));

    debug!("WebSocket connection established");

    while let Some(Ok(msg)) = receiver.next().await {
        if let Message::Text(text) = msg {
            debug!(msg_len = text.len(), "Received message");

            match serde_json::from_str::<ClientMessage>(&text) {
                Ok(ClientMessage::ExecutionRequest { pipeline }) => {
                    info!(
                        source_count = pipeline.sources.len(),
                        stage_count = pipeline.stages.len(),
                        "Received execution request"
                    );

                    // Queue execution
                    let (execution_id, mut queue_rx, mut progress_rx) = manager
                        .submit(move |execution_id, client_tx| {
                            Box::pin(async move {
                                execute_pipeline(execution_id, client_tx, pipeline, max_memory_gb)
                                    .await
                            })
                        })
                        .await;

                    info!(
                        execution_id = %execution_id,
                        "Execution submitted to queue"
                    );

                    // forward queue updates
                    let send_q = sender.clone();
                    tokio::spawn(
                        async move {
                            debug!("Starting queue update forwarder");
                            while let Ok(update) = queue_rx.recv().await {
                                if update.execution_id == execution_id {
                                    debug!(position = update.position, "Queue position update");
                                    let msg =
                                        serde_json::to_string(&ExecutorMessage::QueuePosition {
                                            execution_id: update.execution_id,
                                            position: update.position,
                                        })
                                        .unwrap();
                                    if let Err(e) =
                                        send_q.lock().await.send(Message::text(msg)).await
                                    {
                                        error!("Failed to send queue update: {}", e);
                                        break;
                                    }
                                }
                            }
                            debug!("Queue update forwarder finished");
                        }
                        .instrument(
                            tracing::info_span!("queue_forwarder", execution_id = %execution_id),
                        ),
                    );

                    // forward progress updates
                    let send_p = sender.clone();
                    tokio::spawn(
                        async move {
                            debug!("Starting progress update forwarder");
                            while let Some(progress) = progress_rx.recv().await {
                                match serde_json::to_string(&progress) {
                                    Ok(msg) => {
                                        if let Err(e) =
                                            send_p.lock().await.send(Message::text(msg)).await
                                        {
                                            error!("Failed to send progress update: {}", e);
                                            break;
                                        }
                                    }
                                    Err(e) => {
                                        error!("Failed to serialize progress update: {}", e);
                                    }
                                }
                            }
                            debug!("Progress update forwarder finished");
                        }
                        .instrument(
                            tracing::info_span!("progress_forwarder", execution_id = %execution_id),
                        ),
                    );
                }
                Ok(ClientMessage::CancelRequest { execution_id }) => {
                    info!(
                        execution_id = %execution_id,
                        "Received cancellation request"
                    );
                    manager.cancel(execution_id).await;
                }
                Err(e) => {
                    error!(
                        error = %e,
                        "Failed to parse incoming message"
                    );
                }
            }
        }
    }

    info!("WebSocket connection closed");
}
