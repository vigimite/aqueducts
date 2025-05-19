use std::{collections::HashMap, sync::Arc};

use aqueducts_websockets::ExecutorMessage;
use futures::future::BoxFuture;
use tokio::sync::{broadcast, mpsc, Mutex, Semaphore};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn, Instrument};
use uuid::Uuid;

use super::{queue::ExecutionQueue, Execution, QueueUpdate};

/// Manages single-concurrency execution + queue
pub struct ExecutionManager {
    queue: Arc<Mutex<ExecutionQueue>>,
    semaphore: Arc<Semaphore>,
    cancellation_tokens: Arc<Mutex<HashMap<Uuid, CancellationToken>>>,
}

impl ExecutionManager {
    /// Create a new manager with broadcast capacity
    pub fn new(queue_capacity: usize) -> Self {
        Self {
            queue: Arc::new(Mutex::new(ExecutionQueue::new(queue_capacity))),
            semaphore: Arc::new(Semaphore::new(1)),
            cancellation_tokens: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Submit an execution, returning queue updates and progress streams
    pub async fn submit<F>(
        &self,
        f: F,
    ) -> (
        Uuid,
        broadcast::Receiver<QueueUpdate>,
        mpsc::Receiver<ExecutorMessage>,
    )
    where
        F: FnOnce(Uuid, mpsc::Sender<ExecutorMessage>) -> BoxFuture<'static, ()> + Send + 'static,
    {
        let id = Uuid::new_v4();
        let (client_tx, client_rx) = mpsc::channel::<ExecutorMessage>(16);

        let cancel_token = CancellationToken::new();

        {
            let mut map = self.cancellation_tokens.lock().await;
            map.insert(id, cancel_token);
        }

        let handler = f(id, client_tx.clone());
        let job = Execution { id, handler };

        debug!(execution_id = %id, "Submitting new execution to queue");
        let mut q = self.queue.lock().await;
        let queue_rx = q.enqueue(job);
        info!(execution_id = %id, "Execution submitted to queue");
        (id, queue_rx, client_rx)
    }

    /// Cancel a pending or running job
    pub async fn cancel(&self, job_id: Uuid) {
        debug!(execution_id = %job_id, "Attempting to cancel execution");
        if let Some(token) = self.cancellation_tokens.lock().await.remove(&job_id) {
            token.cancel();
            info!(execution_id = %job_id, "Execution cancelled");
        } else {
            warn!(execution_id = %job_id, "Cancellation requested for unknown execution");
        }
    }

    /// Background loop: one job at a time, with cancellation
    pub async fn start(&self) {
        info!("Starting execution manager background loop");
        loop {
            debug!("Acquiring execution semaphore");
            let permit = match self.semaphore.clone().acquire_owned().await {
                Ok(permit) => permit,
                Err(e) => {
                    error!("Failed to acquire semaphore lock: {}", e);
                    // Sleep before retrying
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                    continue;
                }
            };

            // dequeue next job
            let next = {
                let mut q = self.queue.lock().await;
                q.dequeue()
            };

            if let Some(execution) = next {
                let tokens = self.cancellation_tokens.clone();
                let execution_id = execution.id;

                debug!(execution_id = %execution_id, "Dequeued execution for processing");

                let cancellation_token = {
                    let tokens = tokens.lock().await;

                    tokens.get(&execution_id).cloned().unwrap_or_else(|| {
                        // Return a cancelled token if execution was previously cancelled by the client
                        let token = CancellationToken::new();
                        token.cancel();
                        token
                    })
                };

                // spawn job with cancellation and tracing context
                info!(execution_id = %execution_id, "Starting execution");
                tokio::spawn(
                    async move {
                        tokio::select! {
                            _ = cancellation_token.cancelled() => {
                                warn!("Execution cancelled");
                            }
                            _ = execution.handler => {
                                info!("Execution completed successfully")
                            }
                        }

                        debug!("Cleaning up execution resources");
                        drop(permit);
                        tokens.lock().await.remove(&execution_id);
                        debug!("Execution resources cleaned up");
                    }
                    .instrument(tracing::info_span!("execution", execution_id = %execution_id)),
                );
            } else {
                debug!("No jobs in queue, waiting");
                drop(permit);
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[tokio::test]
    async fn submit_returns_valid_ids_and_streams() {
        let manager = ExecutionManager::new(5);
        let (id, mut queue_rx, mut progress_rx) =
            manager.submit(|_id, _tx| Box::pin(async {})).await;

        // Should receive initial queue update
        let update_event = queue_rx.recv().await.unwrap();

        assert_eq!(update_event.execution_id, id);
        assert_eq!(update_event.position, 0);

        // progress receiver should be open but no messages
        assert!(progress_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn start_executes_jobs_one_by_one() {
        let manager = Arc::new(ExecutionManager::new(5));
        let mgr = manager.clone();
        // run execution manager in a task
        tokio::spawn(async move { mgr.start().await });

        // use channel to record execution order
        let (order_tx, mut order_rx) = mpsc::channel::<usize>(2);

        let tx_ = order_tx.clone();
        let _ = manager
            .submit(move |_id, _p| {
                let tx = tx_;
                Box::pin(async move {
                    tx.send(1).await.unwrap();
                })
            })
            .await;

        let tx_ = order_tx.clone();
        let _ = manager
            .submit(move |_id, _p| {
                let tx = tx_;
                Box::pin(async move {
                    tx.send(2).await.unwrap();
                })
            })
            .await;

        // collect both execution signals
        let first = order_rx.recv().await.unwrap();
        let second = order_rx.recv().await.unwrap();
        assert_eq!(first, 1);
        assert_eq!(second, 2);
    }

    #[tokio::test]
    async fn job_progress_is_sent_correctly() {
        let manager = Arc::new(ExecutionManager::new(5));
        let mgr = manager.clone();
        // run execution manager in a task
        tokio::spawn(async move { mgr.start().await });

        let expected_execution_id = Uuid::new_v4();
        let id_ = expected_execution_id;
        let (_id, _q, mut progress_rx) = manager
            .submit(move |_id, tx| {
                Box::pin(async move {
                    let _ = tx
                        .send(ExecutorMessage::ExecutionResponse { execution_id: id_ })
                        .await;
                })
            })
            .await;

        // receive progress output
        if let Some(ExecutorMessage::ExecutionResponse { execution_id }) = progress_rx.recv().await
        {
            assert_eq!(expected_execution_id, execution_id);
        } else {
            panic!("Expected progress output");
        }
    }

    #[tokio::test]
    async fn cancel_token_cancels_and_removes() {
        let manager = Arc::new(ExecutionManager::new(3));
        let mgr = manager.clone();
        // run execution manager in a task
        tokio::spawn(async move { mgr.start().await });

        // submit a dummy job, simulating work with sleep
        let (job_id, _qrx, _prx) = manager
            .submit(|_id, _tx| Box::pin(async { tokio::time::sleep(Duration::from_secs(5)).await }))
            .await;

        // grab the token
        let token = {
            let tokens = manager.cancellation_tokens.clone();
            let map = tokens.lock().await;
            map.get(&job_id)
                .cloned()
                .expect("Token should exist after submit")
        };

        // Initially, it should not be cancelled
        assert!(!token.is_cancelled());

        // Cancel the job
        manager.cancel(job_id).await;

        // The token we held should now report cancelled
        assert!(token.is_cancelled(), "Token was not signaled on cancel");

        // And it should be removed from the manager's map
        let still_exists = {
            let map = manager.cancellation_tokens.lock().await;
            map.contains_key(&job_id)
        };
        assert!(!still_exists, "Token was not removed from internal map");
    }
}
