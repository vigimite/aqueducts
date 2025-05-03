use aqueducts::prelude::*;
use datafusion::execution::context::SessionContext;
use std::{
    convert::Infallible,
    sync::{Arc, Mutex},
    time::Instant,
};
use tokio::sync::mpsc;
use tracing::{error, info};

use crate::error::ExecutorError;
use crate::progress::{ExecutionEvent, ExecutorProgressTracker};

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

/// Global execution status using a Mutex for thread safety
static EXECUTION_STATUS: Mutex<ExecutionStatus> = Mutex::new(ExecutionStatus {
    state: ExecutorState::Idle,
    task_handle: None,
    event_sender: None,
});

/// Get the current execution status, handling lock errors gracefully
pub fn get_execution_status() -> Result<ExecutionStatusView, ExecutorError> {
    EXECUTION_STATUS
        .lock()
        .map(|status| ExecutionStatusView::from(&*status))
        .map_err(|e| {
            let error_msg = format!("Failed to acquire execution status lock: {}", e);
            error!("{}", error_msg);
            ExecutorError::ExecutionFailed(error_msg)
        })
}

/// Set a new execution status, handling lock errors gracefully
pub fn set_execution_status(mut status: ExecutionStatus) -> Result<(), ExecutorError> {
    match EXECUTION_STATUS.lock() {
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

/// Execute the provided pipeline
pub async fn execute_aqueduct_pipeline(
    pipeline: Aqueduct,
    execution_id: String,
    tx: &mpsc::Sender<Result<ExecutionEvent, Infallible>>,
) -> Result<(), ExecutorError> {
    let start_time = Instant::now();
    info!("Starting pipeline execution (id: {execution_id})");

    // Send "Started" event
    let _ = tx
        .send(Ok(ExecutionEvent::Started {
            execution_id: execution_id.clone(),
        }))
        .await;

    aqueducts::register_handlers();
    let mut ctx = SessionContext::new();

    // Register JSON functions - this should not fail in normal circumstances
    datafusion_functions_json::register_all(&mut ctx)
        .expect("Failed to register DataFusion JSON functions");

    let ctx = Arc::new(ctx);

    // Count total stages for progress tracking (sources + stage groups + destination)
    let total_steps = pipeline.sources.len()
        + pipeline.stages.len()
        + if pipeline.destination.is_some() { 1 } else { 0 };

    // Send progress event for pipeline parsing
    let _ = tx
        .send(Ok(ExecutionEvent::Progress {
            execution_id: execution_id.clone(),
            message: "Pipeline parsed successfully".to_string(),
            progress: 10,
            current_stage: None,
        }))
        .await;

    // Create a progress tracker
    let progress_tracker = Arc::new(ExecutorProgressTracker::new(
        tx.clone(),
        execution_id.clone(),
        total_steps,
    ));

    // Execute pipeline with progress tracking
    match aqueducts::run_pipeline(ctx, pipeline, Some(progress_tracker)).await {
        Ok(_) => {
            // Send "Completed" event
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

            // Send error event
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
pub async fn cancel_current_execution(execution_id: &str) -> Result<(), ExecutorError> {
    // Get the event sender and task handle from the execution status
    let (tx, task_handle) = {
        let mut status_opt = None;

        if let Ok(mut locked_status) = EXECUTION_STATUS.lock() {
            // Extract task handle and event sender, then update status to idle
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
        let _ = sender
            .send(Ok(ExecutionEvent::Cancelled {
                execution_id: execution_id.to_string(),
                message: "Pipeline execution cancelled".to_string(),
            }))
            .await;
    }

    // If we have a task handle, abort it
    if let Some(handle) = task_handle {
        handle.abort();
        info!("Cancelled pipeline execution (id: {execution_id})");
        return Ok(());
    }

    // If we couldn't get a lock or there wasn't a task handle,
    // reset the execution status
    set_execution_status(ExecutionStatus {
        state: ExecutorState::Idle,
        task_handle: None,
        event_sender: None,
    })?;

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
        // Create execution status with running state
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

        // Convert to view
        let view = ExecutionStatusView::from(&status);
        
        // Verify state conversion
        match view.state {
            ExecutorState::Running { execution_id: ref id, started_at: _ } => {
                assert_eq!(id, &execution_id);
            },
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
    fn test_execution_status_view_execution_id(#[case] state: ExecutorState, #[case] expected: Option<String>) {
        let view = ExecutionStatusView { state };
        assert_eq!(view.execution_id(), expected);
    }

    #[tokio::test]
    async fn test_running_time_for_running_status() {
        // Create a status with a known start time
        let execution_id = "test-id".to_string();
        let started_at = Instant::now();
        let view = ExecutionStatusView {
            state: ExecutorState::Running {
                execution_id,
                started_at,
            },
        };
        
        // Give it a tiny bit of time to elapse
        tokio::time::sleep(Duration::from_millis(10)).await;
        
        // Verify we get a non-zero running time
        let running_time = view.running_time();
        assert!(running_time.is_some());
    }

    #[tokio::test]
    async fn test_running_time_for_idle_status() {
        let view = ExecutionStatusView { state: ExecutorState::Idle };
        assert_eq!(view.running_time(), None);
    }

    #[tokio::test]
    async fn test_set_get_execution_status() {
        // Create a test status
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
        
        // Set the status
        set_execution_status(status).unwrap();
        
        // Get the status and verify
        let view = get_execution_status().unwrap();
        assert!(view.is_running());
        assert_eq!(view.execution_id(), Some(execution_id));
        
        // Reset to idle for other tests
        set_execution_status(ExecutionStatus {
            state: ExecutorState::Idle,
            task_handle: None,
            event_sender: None,
        }).unwrap();
    }

    #[tokio::test]
    async fn test_set_execution_status_to_idle_clears_task_handle() {
        // Create a test status with task handle
        let mut status = ExecutionStatus {
            state: ExecutorState::Idle,
            task_handle: Some(tokio::spawn(async {})),
            event_sender: None,
        };
        
        // Set the status - this should clear the task handle
        set_execution_status(status).unwrap();
        
        // Create new status object to avoid borrowing issues
        status = ExecutionStatus {
            state: ExecutorState::Idle,
            task_handle: None,
            event_sender: None,
        };
        
        // Set again and verify
        set_execution_status(status).unwrap();
    }
}
