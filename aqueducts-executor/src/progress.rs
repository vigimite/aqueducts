use std::convert::Infallible;
use std::sync::atomic::AtomicUsize;
use tokio::sync::mpsc;
use tokio::runtime::Handle;
use aqueducts::ProgressEventType;
use serde::Serialize;

/// Event for real-time feedback using Server-Sent Events
#[derive(Debug, Serialize)]
#[serde(tag = "event_type", rename_all = "snake_case")]
pub enum ExecutionEvent {
    /// Pipeline execution has started
    Started {
        /// Unique execution ID
        execution_id: String,
    },
    
    /// Progress update during execution
    Progress {
        /// Unique execution ID
        execution_id: String,
        /// Human-readable message about the progress
        message: String,
        /// Progress percentage (0-100)
        progress: u8,
        /// Current stage being processed (if applicable)
        current_stage: Option<String>,
    },
    
    /// Pipeline execution completed successfully
    Completed {
        /// Unique execution ID
        execution_id: String,
        /// Human-readable message about completion
        message: String,
    },
    
    /// Error occurred during execution
    Error {
        /// Unique execution ID
        execution_id: String,
        /// Human-readable error message
        message: String,
        /// Detailed error information
        details: Option<String>,
    },
    
    /// Execution was cancelled
    Cancelled {
        /// Unique execution ID
        execution_id: String,
        /// Human-readable message about cancellation
        message: String,
    },
}

/// Implementation of ProgressTracker for the executor
pub struct ExecutorProgressTracker {
    tx: mpsc::Sender<Result<ExecutionEvent, Infallible>>,
    execution_id: String,
    total_steps: usize,
    completed_steps: AtomicUsize,
}

impl ExecutorProgressTracker {
    /// Create a new progress tracker
    pub fn new(
        tx: mpsc::Sender<Result<ExecutionEvent, Infallible>>,
        execution_id: String,
        total_steps: usize,
    ) -> Self {
        Self {
            tx,
            execution_id,
            total_steps,
            completed_steps: AtomicUsize::new(0),
        }
    }
    
    /// Calculate progress percentage based on completed steps
    fn calculate_progress(&self, current: usize) -> u8 {
        // Start at 10% after parsing, go up to 95% before completion (the last 5% is for cleanup)
        10 + ((current as f32) / (self.total_steps as f32) * 80.0) as u8
    }
}

impl aqueducts::ProgressTracker for ExecutorProgressTracker {
    fn on_progress(&self, event: ProgressEventType) {
        let execution_event = match &event {
            ProgressEventType::Started => {
                ExecutionEvent::Started {
                    execution_id: self.execution_id.clone(),
                }
            },
            ProgressEventType::SourceRegistered { name } => {
                let current = self
                    .completed_steps
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                    + 1;
                let progress = self.calculate_progress(current);
                
                ExecutionEvent::Progress {
                    execution_id: self.execution_id.clone(),
                    message: format!("Registered source: {}", name),
                    progress,
                    current_stage: Some(name.clone()),
                }
            },
            ProgressEventType::StageStarted { name, position, sub_position } => {
                let pos_info = format!("{}_{}", position, sub_position);
                
                ExecutionEvent::Progress {
                    execution_id: self.execution_id.clone(),
                    message: format!(
                        "Processing stage: {} (position: {}, sub-position: {})",
                        name, position, sub_position
                    ),
                    progress: self.calculate_progress(
                        self.completed_steps.load(std::sync::atomic::Ordering::SeqCst)
                    ),
                    current_stage: Some(format!("{}:{}", name, pos_info)),
                }
            },
            ProgressEventType::StageCompleted { name, position, sub_position, .. } => {
                let current = self
                    .completed_steps
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                    + 1;
                let progress = self.calculate_progress(current);
                
                let pos_info = format!("{}_{}", position, sub_position);
                
                ExecutionEvent::Progress {
                    execution_id: self.execution_id.clone(),
                    message: format!(
                        "Completed stage: {} (position: {}, sub-position: {})",
                        name, position, sub_position
                    ),
                    progress,
                    current_stage: Some(format!("{}:{}", name, pos_info)),
                }
            },
            ProgressEventType::DestinationCompleted => {
                let current = self
                    .completed_steps
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                    + 1;
                let progress = self.calculate_progress(current);
                
                ExecutionEvent::Progress {
                    execution_id: self.execution_id.clone(),
                    message: "Writing to destination completed".to_string(),
                    progress,
                    current_stage: Some("destination".to_string()),
                }
            },
            ProgressEventType::Completed { .. } => {
                ExecutionEvent::Progress {
                    execution_id: self.execution_id.clone(),
                    message: "Pipeline execution nearly complete".to_string(),
                    progress: 95,
                    current_stage: None,
                }
            },
        };
        
        // Send the execution event via the channel
        let tx = self.tx.clone();
        let event = Ok(execution_event);
        
        // Use the current tokio runtime to spawn a task for sending the event
        Handle::current().spawn(async move {
            let _ = tx.send(event).await;
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aqueducts::ProgressTracker;
    use rstest::rstest;
    use std::time::Duration;
    
    #[tokio::test]
    async fn test_execution_event_serialization() {
        // Test Started event
        let started_event = ExecutionEvent::Started {
            execution_id: "test-execution".to_string(),
        };
        let json = serde_json::to_string(&started_event).unwrap();
        assert!(json.contains("\"event_type\":\"started\""));
        assert!(json.contains("\"execution_id\":\"test-execution\""));
        
        // Test Progress event
        let progress_event = ExecutionEvent::Progress {
            execution_id: "test-execution".to_string(),
            message: "Test progress".to_string(),
            progress: 50,
            current_stage: Some("test-stage".to_string()),
        };
        let json = serde_json::to_string(&progress_event).unwrap();
        assert!(json.contains("\"event_type\":\"progress\""));
        assert!(json.contains("\"progress\":50"));
        assert!(json.contains("\"current_stage\":\"test-stage\""));
        
        // Test Completed event
        let completed_event = ExecutionEvent::Completed {
            execution_id: "test-execution".to_string(),
            message: "Test completed".to_string(),
        };
        let json = serde_json::to_string(&completed_event).unwrap();
        assert!(json.contains("\"event_type\":\"completed\""));
        assert!(json.contains("\"message\":\"Test completed\""));
        
        // Test Error event
        let error_event = ExecutionEvent::Error {
            execution_id: "test-execution".to_string(),
            message: "Test error".to_string(),
            details: Some("Error details".to_string()),
        };
        let json = serde_json::to_string(&error_event).unwrap();
        assert!(json.contains("\"event_type\":\"error\""));
        assert!(json.contains("\"details\":\"Error details\""));
        
        // Test Cancelled event
        let cancelled_event = ExecutionEvent::Cancelled {
            execution_id: "test-execution".to_string(),
            message: "Test cancelled".to_string(),
        };
        let json = serde_json::to_string(&cancelled_event).unwrap();
        assert!(json.contains("\"event_type\":\"cancelled\""));
        assert!(json.contains("\"message\":\"Test cancelled\""));
    }

    #[tokio::test]
    async fn test_progress_calculation() {
        // Create a progress tracker with 10 total steps
        let (tx, _rx) = mpsc::channel(100);
        let tracker = ExecutorProgressTracker::new(
            tx,
            "test-execution".to_string(),
            10,
        );
        
        // Test progress calculation for different step counts
        let progress1 = tracker.calculate_progress(0);  // 0/10 steps
        let progress2 = tracker.calculate_progress(5);  // 5/10 steps
        let progress3 = tracker.calculate_progress(10); // 10/10 steps
        
        // Progress should start at 10% (after parsing) and go up to 90% (before completion)
        assert_eq!(progress1, 10);  // 0% of remaining 80% + 10% base
        assert_eq!(progress2, 50);  // 50% of remaining 80% + 10% base
        assert_eq!(progress3, 90);  // 100% of remaining 80% + 10% base
    }

    #[tokio::test]
    async fn test_progress_tracker_sends_events() {
        // Create a channel and progress tracker
        let (tx, mut rx) = mpsc::channel::<Result<ExecutionEvent, Infallible>>(100);
        let tracker = ExecutorProgressTracker::new(
            tx,
            "test-execution".to_string(),
            4, // 4 total steps
        );
        
        // Send a source registered event
        tracker.on_progress(ProgressEventType::SourceRegistered { 
            name: "test-source".to_string() 
        });
        
        // Check that we receive the event
        let event = tokio::time::timeout(Duration::from_millis(100), rx.recv()).await.unwrap().unwrap().unwrap();
        match event {
            ExecutionEvent::Progress { execution_id, message, progress, current_stage } => {
                assert_eq!(execution_id, "test-execution");
                assert!(message.contains("test-source"));
                assert_eq!(progress, 30); // 10% + (1/4 * 80%)
                assert_eq!(current_stage, Some("test-source".to_string()));
            },
            _ => panic!("Expected Progress event but got {:?}", event),
        }
        
        // Send a stage started event
        tracker.on_progress(ProgressEventType::StageStarted { 
            name: "test-stage".to_string(),
            position: 1,
            sub_position: 0,
        });
        
        // Check that we receive the event
        let event = tokio::time::timeout(Duration::from_millis(100), rx.recv()).await.unwrap().unwrap().unwrap();
        match event {
            ExecutionEvent::Progress { execution_id, message, progress: _, current_stage } => {
                assert_eq!(execution_id, "test-execution");
                assert!(message.contains("test-stage"));
                assert_eq!(current_stage, Some("test-stage:1_0".to_string()));
            },
            _ => panic!("Expected Progress event but got {:?}", event),
        }
    }

    #[rstest]
    #[case(ProgressEventType::Started, "Started")]
    #[case(
        ProgressEventType::SourceRegistered { name: "test-source".to_string() }, 
        "Progress"
    )]
    #[case(
        ProgressEventType::StageStarted { 
            name: "test-stage".to_string(), 
            position: 0, 
            sub_position: 0 
        }, 
        "Progress"
    )]
    #[case(
        ProgressEventType::StageCompleted { 
            name: "test-stage".to_string(), 
            position: 0, 
            sub_position: 0,
            duration_ms: 10
        }, 
        "Progress"
    )]
    #[case(ProgressEventType::DestinationCompleted, "Progress")]
    #[case(
        ProgressEventType::Completed { duration_ms: 10 }, 
        "Progress"
    )]
    #[tokio::test]
    async fn test_progress_tracker_event_types(
        #[case] event_type: ProgressEventType, 
        #[case] expected_event_type: &str
    ) {
        // Create a channel and progress tracker

        use aqueducts::ProgressTracker;
        let (tx, mut rx) = mpsc::channel::<Result<ExecutionEvent, Infallible>>(100);
        let tracker = ExecutorProgressTracker::new(
            tx,
            "test-execution".to_string(),
            4, // 4 total steps
        );
        
        // Send the event
        tracker.on_progress(event_type);
        
        // Check that we receive an event of the expected type
        let event = tokio::time::timeout(Duration::from_millis(100), rx.recv()).await.unwrap().unwrap().unwrap();
        match event {
            ExecutionEvent::Started { .. } if expected_event_type == "Started" => {},
            ExecutionEvent::Progress { .. } if expected_event_type == "Progress" => {},
            ExecutionEvent::Completed { .. } if expected_event_type == "Completed" => {},
            ExecutionEvent::Error { .. } if expected_event_type == "Error" => {},
            ExecutionEvent::Cancelled { .. } if expected_event_type == "Cancelled" => {},
            _ => panic!("Expected {} event but got {:?}", expected_event_type, event),
        }
    }
}
