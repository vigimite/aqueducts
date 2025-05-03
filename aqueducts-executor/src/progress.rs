use aqueducts::ProgressEventType;
use aqueducts_utils::executor_events::ExecutionEvent;
use std::convert::Infallible;
use std::sync::atomic::AtomicUsize;
use tokio::runtime::Handle;
use tokio::sync::mpsc;

/// Implementation of ProgressTracker for the executor
pub struct ExecutorProgressTracker {
    tx: mpsc::Sender<Result<ExecutionEvent, Infallible>>,
    execution_id: String,
    total_steps: usize,
    completed_steps: AtomicUsize,
}

impl ExecutorProgressTracker {
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
            ProgressEventType::Started => ExecutionEvent::Started {
                execution_id: self.execution_id.clone(),
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
            }
            ProgressEventType::StageStarted {
                name,
                position,
                sub_position,
            } => {
                let pos_info = format!("{}_{}", position, sub_position);

                ExecutionEvent::Progress {
                    execution_id: self.execution_id.clone(),
                    message: format!(
                        "Processing stage: {} (position: {}, sub-position: {})",
                        name, position, sub_position
                    ),
                    progress: self.calculate_progress(
                        self.completed_steps
                            .load(std::sync::atomic::Ordering::SeqCst),
                    ),
                    current_stage: Some(format!("{}:{}", name, pos_info)),
                }
            }
            ProgressEventType::StageCompleted {
                name,
                position,
                sub_position,
                ..
            } => {
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
            }
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
            }
            ProgressEventType::Completed { .. } => ExecutionEvent::Progress {
                execution_id: self.execution_id.clone(),
                message: "Pipeline execution nearly complete".to_string(),
                progress: 95,
                current_stage: None,
            },
        };

        // Send the execution event via the channel
        let tx = self.tx.clone();
        let event = Ok(execution_event);

        Handle::current().spawn(async move {
            let _ = tx.send(event).await;
        });
    }

    fn on_stage_output(&self, stage_name: &str, output_type: &str, output: &str) {
        let execution_event = ExecutionEvent::StageOutput {
            execution_id: self.execution_id.clone(),
            stage_name: stage_name.to_string(),
            output_type: output_type.to_string(),
            data: output.to_string(),
        };

        let tx = self.tx.clone();
        let event = Ok(execution_event);

        Handle::current().spawn(async move {
            let _ = tx.send(event).await;
        });

        tracing::info!("Captured {} output for stage {}", output_type, stage_name);
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
        let started_event = ExecutionEvent::Started {
            execution_id: "test-execution".to_string(),
        };
        let json = serde_json::to_string(&started_event).unwrap();
        assert!(json.contains("\"event_type\":\"started\""));
        assert!(json.contains("\"execution_id\":\"test-execution\""));

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

        let completed_event = ExecutionEvent::Completed {
            execution_id: "test-execution".to_string(),
            message: "Test completed".to_string(),
        };
        let json = serde_json::to_string(&completed_event).unwrap();
        assert!(json.contains("\"event_type\":\"completed\""));
        assert!(json.contains("\"message\":\"Test completed\""));

        let error_event = ExecutionEvent::Error {
            execution_id: "test-execution".to_string(),
            message: "Test error".to_string(),
            details: Some("Error details".to_string()),
        };
        let json = serde_json::to_string(&error_event).unwrap();
        assert!(json.contains("\"event_type\":\"error\""));
        assert!(json.contains("\"details\":\"Error details\""));

        let cancelled_event = ExecutionEvent::Cancelled {
            execution_id: "test-execution".to_string(),
            message: "Test cancelled".to_string(),
        };
        let json = serde_json::to_string(&cancelled_event).unwrap();
        assert!(json.contains("\"event_type\":\"cancelled\""));
        assert!(json.contains("\"message\":\"Test cancelled\""));

        let stage_output_event = ExecutionEvent::StageOutput {
            execution_id: "test-execution".to_string(),
            stage_name: "test-stage".to_string(),
            output_type: "show".to_string(),
            data: "Table content".to_string(),
        };
        let json = serde_json::to_string(&stage_output_event).unwrap();
        assert!(json.contains("\"event_type\":\"stage_output\""));
        assert!(json.contains("\"stage_name\":\"test-stage\""));
        assert!(json.contains("\"output_type\":\"show\""));
        assert!(json.contains("\"data\":\"Table content\""));
    }

    #[tokio::test]
    async fn test_progress_calculation() {
        let (tx, _rx) = mpsc::channel(100);
        let tracker = ExecutorProgressTracker::new(tx, "test-execution".to_string(), 10);

        let progress1 = tracker.calculate_progress(0); // 0/10 steps
        let progress2 = tracker.calculate_progress(5); // 5/10 steps
        let progress3 = tracker.calculate_progress(10); // 10/10 steps

        assert_eq!(progress1, 10); // 0% of remaining 80% + 10% base
        assert_eq!(progress2, 50); // 50% of remaining 80% + 10% base
        assert_eq!(progress3, 90); // 100% of remaining 80% + 10% base
    }

    #[tokio::test]
    async fn test_progress_tracker_sends_events() {
        let (tx, mut rx) = mpsc::channel::<Result<ExecutionEvent, Infallible>>(100);
        let tracker = ExecutorProgressTracker::new(
            tx,
            "test-execution".to_string(),
            4, // 4 total steps
        );

        tracker.on_progress(ProgressEventType::SourceRegistered {
            name: "test-source".to_string(),
        });

        let event = tokio::time::timeout(Duration::from_millis(100), rx.recv())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        match event {
            ExecutionEvent::Progress {
                execution_id,
                message,
                progress,
                current_stage,
            } => {
                assert_eq!(execution_id, "test-execution");
                assert!(message.contains("test-source"));
                assert_eq!(progress, 30); // 10% + (1/4 * 80%)
                assert_eq!(current_stage, Some("test-source".to_string()));
            }
            _ => panic!("Expected Progress event but got {:?}", event),
        }

        tracker.on_progress(ProgressEventType::StageStarted {
            name: "test-stage".to_string(),
            position: 1,
            sub_position: 0,
        });

        let event = tokio::time::timeout(Duration::from_millis(100), rx.recv())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        match event {
            ExecutionEvent::Progress {
                execution_id,
                message,
                progress: _,
                current_stage,
            } => {
                assert_eq!(execution_id, "test-execution");
                assert!(message.contains("test-stage"));
                assert_eq!(current_stage, Some("test-stage:1_0".to_string()));
            }
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
        #[case] expected_event_type: &str,
    ) {
        use aqueducts::ProgressTracker;
        let (tx, mut rx) = mpsc::channel::<Result<ExecutionEvent, Infallible>>(100);
        let tracker = ExecutorProgressTracker::new(
            tx,
            "test-execution".to_string(),
            4, // 4 total steps
        );

        tracker.on_progress(event_type);

        let event = tokio::time::timeout(Duration::from_millis(100), rx.recv())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        match event {
            ExecutionEvent::Started { .. } if expected_event_type == "Started" => {}
            ExecutionEvent::Progress { .. } if expected_event_type == "Progress" => {}
            ExecutionEvent::Completed { .. } if expected_event_type == "Completed" => {}
            ExecutionEvent::Error { .. } if expected_event_type == "Error" => {}
            ExecutionEvent::Cancelled { .. } if expected_event_type == "Cancelled" => {}
            _ => panic!("Expected {} event but got {:?}", expected_event_type, event),
        }
    }
}
