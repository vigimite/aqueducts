use std::collections::VecDeque;

use tokio::sync::broadcast;

use super::{Execution, QueueUpdate};

/// Queue of pending jobs + broadcaster for queue updates
pub struct ExecutionQueue {
    queue: VecDeque<Execution>,
    broadcaster: broadcast::Sender<QueueUpdate>,
}

impl ExecutionQueue {
    pub fn new(capacity: usize) -> Self {
        let (tx, _rx) = broadcast::channel(capacity);
        Self {
            queue: VecDeque::new(),
            broadcaster: tx,
        }
    }

    pub fn enqueue(&mut self, job: Execution) -> broadcast::Receiver<QueueUpdate> {
        let rx = self.broadcaster.subscribe();
        self.queue.push_back(job);
        self.broadcast_positions();
        rx
    }

    pub fn dequeue(&mut self) -> Option<Execution> {
        let job = self.queue.pop_front();
        if job.is_some() {
            self.broadcast_positions();
        }
        job
    }

    fn broadcast_positions(&self) {
        for (idx, execution) in self.queue.iter().enumerate() {
            let _ = self.broadcaster.send(QueueUpdate {
                execution_id: execution.id,
                position: idx,
            });
        }
    }
}

// Unit tests for ExecutionQueue
#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    fn dummy_execution() -> (Uuid, Execution) {
        let id = Uuid::new_v4();
        let handler = Box::pin(async move {});
        let execution = Execution { id, handler };

        (id, execution)
    }

    #[tokio::test]
    async fn enqueue_broadcasts_position() {
        let mut queue = ExecutionQueue::new(10);
        let (execution_id, job) = dummy_execution();

        let mut rx = queue.enqueue(job);
        let update_event = rx.recv().await.unwrap();

        assert_eq!(update_event.execution_id, execution_id);
        assert_eq!(update_event.position, 0);
    }

    #[tokio::test]
    async fn enqueue_two_broadcasts_both_positions() {
        let mut queue = ExecutionQueue::new(10);
        let (execution_id_1, execution_1) = dummy_execution();
        let mut rx_1 = queue.enqueue(execution_1);

        // consume initial update for execution_1
        let _ = rx_1.recv().await.unwrap();

        let (execution_id_2, execution_2) = dummy_execution();
        let mut rx_2 = queue.enqueue(execution_2);

        // rx_1 should receive both execution_1 and execution_2 positions
        let update_event_1 = rx_1.recv().await.unwrap();
        let update_event_2 = rx_1.recv().await.unwrap();
        assert_eq!(update_event_1.execution_id, execution_id_1);
        assert_eq!(update_event_1.position, 0);
        assert_eq!(update_event_2.execution_id, execution_id_2);
        assert_eq!(update_event_2.position, 1);

        // rx_2 should also get both
        let update_event_1 = rx_2.recv().await.unwrap();
        let update_event_2 = rx_2.recv().await.unwrap();
        assert_eq!(update_event_1.execution_id, execution_id_1);
        assert_eq!(update_event_2.execution_id, execution_id_2);
    }

    #[tokio::test]
    async fn dequeue_broadcasts_updated_positions() {
        let mut queue = ExecutionQueue::new(10);
        let (execution_id_1, execution_1) = dummy_execution();
        let mut rx = queue.enqueue(execution_1);

        // consume initial update for execution_1
        let _ = rx.recv().await.unwrap();

        let (execution_id_2, execution_2) = dummy_execution();
        let _ = queue.enqueue(execution_2);

        // consume updates from enqueue execution_2
        let _ = rx.recv().await.unwrap();
        let _ = rx.recv().await.unwrap();

        let removed = queue.dequeue().unwrap();
        assert_eq!(removed.id, execution_id_1);

        // consume update from dequeue
        let upd = rx.recv().await.unwrap();
        assert_eq!(upd.execution_id, execution_id_2);
        assert_eq!(upd.position, 0);
    }
}
