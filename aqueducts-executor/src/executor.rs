use aqueducts::prelude::*;
use datafusion::execution::{context::SessionContext, runtime_env::RuntimeEnvBuilder};
use std::{
    convert::Infallible,
    sync::{Arc, Mutex},
    time::Instant,
};
use tokio::sync::mpsc;
use tracing::{debug, error, info};

use crate::error::ExecutorError;
use crate::progress::ExecutorProgressTracker;
use aqueducts_utils::executor_events::ExecutionEvent;

/// Enum to track the state of the executor
#[derive(Debug, Clone, PartialEq)]
pub enum ExecutorState {
    /// No execution is currently running
    Idle,
    /// An execution is in progress
    Running {
        /// Unique ID of the running execution
        execution_id: String,
        /// When the execution started
        started_at: Instant,
    },
}

/// Structure to track execution status
#[derive(Debug)]
pub struct ExecutionStatus {
    /// Current state of execution
    pub state: ExecutorState,
    /// Task handle for cancellation
    pub task_handle: Option<tokio::task::JoinHandle<()>>,
    /// Event sender for SSE stream
    pub event_sender: Option<mpsc::Sender<Result<ExecutionEvent, Infallible>>>,
}

/// Separate read-only status struct for API responses
/// This avoids needing a partial Clone implementation for ExecutionStatus
#[derive(Debug, Clone)]
pub struct ExecutionStatusView {
    pub state: ExecutorState,
}

impl From<&ExecutionStatus> for ExecutionStatusView {
    fn from(status: &ExecutionStatus) -> Self {
        Self {
            state: status.state.clone(),
        }
    }
}

impl ExecutionStatusView {
    /// Returns whether an execution is currently running
    pub fn is_running(&self) -> bool {
        matches!(self.state, ExecutorState::Running { .. })
    }

    /// Returns the current execution ID if one is running
    pub fn execution_id(&self) -> Option<String> {
        match &self.state {
            ExecutorState::Running { execution_id, .. } => Some(execution_id.clone()),
            ExecutorState::Idle => None,
        }
    }

    /// Returns how long the current execution has been running
    pub fn running_time(&self) -> Option<u64> {
        match &self.state {
            ExecutorState::Running { started_at, .. } => Some(started_at.elapsed().as_secs()),
            ExecutorState::Idle => None,
        }
    }
}

/// State management for execution status
///
/// This structure encapsulates access to the execution status,
/// making it easier to control in tests and allows for dependency injection
#[derive(Debug)]
pub struct ExecutionStateManager {
    /// Internal mutex for thread-safe access to execution status
    execution_status: Mutex<ExecutionStatus>,
}

impl Default for ExecutionStateManager {
    fn default() -> Self {
        Self::new()
    }
}

impl ExecutionStateManager {
    /// Create a new instance of the state manager
    pub fn new() -> Self {
        Self {
            execution_status: Mutex::new(ExecutionStatus {
                state: ExecutorState::Idle,
                task_handle: None,
                event_sender: None,
            }),
        }
    }

    /// Get the current execution status, handling lock errors gracefully
    pub fn get_status(&self) -> Result<ExecutionStatusView, ExecutorError> {
        self.execution_status
            .lock()
            .map(|status| ExecutionStatusView::from(&*status))
            .map_err(|e| {
                let error_msg = format!("Failed to acquire execution status lock: {}", e);
                error!("{}", error_msg);
                ExecutorError::ExecutionFailed(error_msg)
            })
    }

    /// Set a new execution status, handling lock errors gracefully
    pub fn set_status(&self, mut status: ExecutionStatus) -> Result<(), ExecutorError> {
        match self.execution_status.lock() {
            Ok(mut guard) => {
                // Clear the task handle when transitioning to Idle
                if matches!(status.state, ExecutorState::Idle) {
                    status.task_handle = None;
                }

                *guard = status;
                Ok(())
            }
            Err(e) => {
                let error_msg = format!("Failed to acquire execution status lock: {}", e);
                error!("{}", error_msg);
                Err(ExecutorError::ExecutionFailed(error_msg))
            }
        }
    }

    /// Resets the state to idle, useful for tests
    pub fn reset_to_idle(&self) -> Result<(), ExecutorError> {
        self.set_status(ExecutionStatus {
            state: ExecutorState::Idle,
            task_handle: None,
            event_sender: None,
        })
    }

    /// Only for tests: sets up a known running state with the given execution ID
    #[cfg(test)]
    pub fn setup_running_state_for_test(&self, execution_id: String) -> Result<(), ExecutorError> {
        let task_handle = tokio::spawn(async {
            tokio::time::sleep(std::time::Duration::from_secs(10)).await;
        });

        self.set_status(ExecutionStatus {
            state: ExecutorState::Running {
                execution_id,
                started_at: std::time::Instant::now(),
            },
            task_handle: Some(task_handle),
            event_sender: None,
        })
    }

    /// Only for tests: ensures that any state manipulation in the closure happens
    /// safely and that the state is reset to idle after the test
    #[cfg(test)]
    pub async fn with_clean_state<F, Fut, T>(&self, test_fn: F) -> T
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = T>,
    {
        // Ensure we start with a clean slate
        let _ = self.reset_to_idle();

        // Run the test function
        let result = test_fn().await;

        // Clean up after the test
        let _ = self.reset_to_idle();

        result
    }
}

/// Execute the provided pipeline
pub async fn execute_aqueduct_pipeline(
    pipeline: Aqueduct,
    execution_id: String,
    tx: &mpsc::Sender<Result<ExecutionEvent, Infallible>>,
    max_memory_gb: Option<u32>,
) -> Result<(), ExecutorError> {
    let start_time = Instant::now();
    info!("Starting pipeline execution (id: {execution_id})");

    let _ = tx
        .send(Ok(ExecutionEvent::Started {
            execution_id: execution_id.clone(),
        }))
        .await;

    aqueducts::register_handlers();

    // Create a SessionContext with or without memory limits
    let mut ctx = if let Some(memory_gb) = max_memory_gb {
        // Convert max_memory_gb directly to bytes (GB * 1024^3)
        let max_memory_bytes = (memory_gb as usize) * 1024 * 1024 * 1024;

        debug!("Creating runtime environment with memory limit of {memory_gb} GB ({max_memory_bytes} bytes)");

        // Use 0.95 as the memory use percentage (allowing 95% of the limit to be used)
        let runtime_env = RuntimeEnvBuilder::new()
            .with_memory_limit(max_memory_bytes, 0.95)
            .build_arc()
            .expect("Failed to build runtime environment");

        let config = datafusion::execution::config::SessionConfig::new();
        SessionContext::new_with_config_rt(config, runtime_env)
    } else {
        debug!("No memory limit specified, using unlimited memory allocation");
        SessionContext::new()
    };

    datafusion_functions_json::register_all(&mut ctx)
        .expect("Failed to register DataFusion JSON functions");

    let ctx = Arc::new(ctx);

    // Count total stages for progress tracking (sources + stage groups + destination)
    let total_steps = pipeline.sources.len()
        + pipeline.stages.len()
        + if pipeline.destination.is_some() { 1 } else { 0 };

    let _ = tx
        .send(Ok(ExecutionEvent::Progress {
            execution_id: execution_id.clone(),
            message: "Pipeline parsed successfully".to_string(),
            progress: 10,
            current_stage: None,
        }))
        .await;

    let progress_tracker = Arc::new(ExecutorProgressTracker::new(
        tx.clone(),
        execution_id.clone(),
        total_steps,
    ));

    match aqueducts::run_pipeline(ctx, pipeline, Some(progress_tracker)).await {
        Ok(_) => {
            let _ = tx
                .send(Ok(ExecutionEvent::Completed {
                    execution_id: execution_id.clone(),
                    message: "Pipeline execution completed successfully".to_string(),
                }))
                .await;
        }
        Err(err) => {
            let error_msg = format!("Pipeline execution failed: {}", err);
            error!("{}", error_msg);

            let _ = tx
                .send(Ok(ExecutionEvent::Error {
                    execution_id: execution_id.clone(),
                    message: "Pipeline execution failed".to_string(),
                    details: Some(error_msg.clone()),
                }))
                .await;

            return Err(ExecutorError::ExecutionFailed(error_msg));
        }
    }

    let execution_duration = start_time.elapsed().as_secs();
    info!(
        "Pipeline execution completed (id: {execution_id}, duration: {execution_duration} seconds)"
    );

    Ok(())
}

/// Handle cancellation of the current execution
///
/// This function aborts the running task, sends a cancellation event through the SSE stream,
/// and updates the execution state to idle.
pub async fn cancel_current_execution(
    execution_id: &str,
    state_manager: &ExecutionStateManager,
) -> Result<(), ExecutorError> {
    let (tx, task_handle) = {
        let mut status_opt = None;

        if let Ok(mut locked_status) = state_manager.execution_status.lock() {
            let tx = locked_status.event_sender.clone();
            let handle = locked_status.task_handle.take();

            if handle.is_some() {
                locked_status.state = ExecutorState::Idle;
                locked_status.event_sender = None;
                status_opt = Some((tx, handle));
            }
        }

        status_opt.unwrap_or((None, None))
    };

    // If we have a sender, send the cancellation event
    if let Some(sender) = &tx {
        let cancelled_event = ExecutionEvent::Cancelled {
            execution_id: execution_id.to_string(),
            message: "Pipeline execution cancelled".to_string(),
        };

        let _ = sender.send(Ok(cancelled_event)).await;
    }

    // If we have a task handle, abort it
    if let Some(handle) = task_handle {
        handle.abort();
        info!("Cancelled pipeline execution (id: {execution_id})");
        return Ok(());
    }

    // If we couldn't get a lock or there wasn't a task handle,
    // reset the execution status
    state_manager.reset_to_idle()?;

    info!("Cancelled pipeline execution (id: {execution_id})");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;
    use std::time::Duration;

    #[tokio::test]
    async fn test_execution_status_view_from() {
        let execution_id = "test-id".to_string();
        let started_at = Instant::now();
        let status = ExecutionStatus {
            state: ExecutorState::Running {
                execution_id: execution_id.clone(),
                started_at,
            },
            task_handle: None,
            event_sender: None,
        };

        let view = ExecutionStatusView::from(&status);

        match view.state {
            ExecutorState::Running {
                execution_id: ref id,
                started_at: _,
            } => {
                assert_eq!(id, &execution_id);
            }
            _ => panic!("Expected Running state but got {:?}", view.state),
        }
    }

    #[rstest]
    #[case(ExecutorState::Idle, false)]
    #[case(
        ExecutorState::Running {
            execution_id: "test-id".to_string(),
            started_at: Instant::now()
        },
        true
    )]
    fn test_execution_status_view_is_running(#[case] state: ExecutorState, #[case] expected: bool) {
        let view = ExecutionStatusView { state };
        assert_eq!(view.is_running(), expected);
    }

    #[rstest]
    #[case(ExecutorState::Idle, None)]
    #[case(
        ExecutorState::Running {
            execution_id: "test-id".to_string(),
            started_at: Instant::now()
        },
        Some("test-id".to_string())
    )]
    fn test_execution_status_view_execution_id(
        #[case] state: ExecutorState,
        #[case] expected: Option<String>,
    ) {
        let view = ExecutionStatusView { state };
        assert_eq!(view.execution_id(), expected);
    }

    #[tokio::test]
    async fn test_running_time_for_running_status() {
        let execution_id = "test-id".to_string();
        let started_at = Instant::now();
        let view = ExecutionStatusView {
            state: ExecutorState::Running {
                execution_id,
                started_at,
            },
        };

        tokio::time::sleep(Duration::from_millis(10)).await;

        let running_time = view.running_time();
        assert!(running_time.is_some());
    }

    #[tokio::test]
    async fn test_running_time_for_idle_status() {
        let view = ExecutionStatusView {
            state: ExecutorState::Idle,
        };
        assert_eq!(view.running_time(), None);
    }

    #[tokio::test]
    async fn test_set_get_execution_status() {
        let state_manager = ExecutionStateManager::new();

        state_manager
            .reset_to_idle()
            .expect("Failed to reset to idle state");

        let execution_id = "test-id".to_string();
        let started_at = Instant::now();
        let status = ExecutionStatus {
            state: ExecutorState::Running {
                execution_id: execution_id.clone(),
                started_at,
            },
            task_handle: None,
            event_sender: None,
        };

        state_manager.set_status(status).unwrap();

        let view = state_manager.get_status().unwrap();
        assert!(view.is_running(), "Expected execution status to be running");
        assert_eq!(view.execution_id(), Some(execution_id));

        state_manager.reset_to_idle().unwrap();
    }

    #[tokio::test]
    async fn test_set_execution_status_to_idle_clears_task_handle() {
        let state_manager = ExecutionStateManager::new();

        state_manager
            .reset_to_idle()
            .expect("Failed to reset to idle state");

        let mut status = ExecutionStatus {
            state: ExecutorState::Idle,
            task_handle: Some(tokio::spawn(async {})),
            event_sender: None,
        };

        state_manager.set_status(status).unwrap();

        status = ExecutionStatus {
            state: ExecutorState::Idle,
            task_handle: None,
            event_sender: None,
        };

        state_manager.set_status(status).unwrap();

        let view = state_manager.get_status().unwrap();
        assert!(!view.is_running(), "Status should be idle");
    }
}
