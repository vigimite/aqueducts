use std::collections::VecDeque;

use tokio::sync::broadcast;

use super::{Execution, QueueUpdate};

/// Manages a FIFO queue of pending pipeline executions with position updates.
///
/// `ExecutionQueue` maintains:
///
/// 1. A double-ended queue of pending `Execution` instances
/// 2. A broadcast channel to notify clients of queue position changes
///
/// When executions are added or removed, the queue automatically broadcasts
/// position updates to all clients monitoring queue positions. This allows
/// clients to display accurate queue position information in real-time.
///
/// The queue operates in a first-in-first-out (FIFO) manner, processing
/// executions in the order they were submitted.
pub struct ExecutionQueue {
    /// Internal double-ended queue holding pending executions
    queue: VecDeque<Execution>,
    /// Broadcast channel for sending queue position updates to clients
    broadcaster: broadcast::Sender<QueueUpdate>,
}

impl ExecutionQueue {
    /// Creates a new execution queue with the specified broadcast capacity.
    ///
    /// # Arguments
    /// * `capacity` - Maximum number of `QueueUpdate` messages that can be buffered
    ///   in the broadcast channel. This should be sized appropriately for the
    ///   expected number of concurrent client connections.
    pub fn new(capacity: usize) -> Self {
        let (tx, _rx) = broadcast::channel(capacity);
        Self {
            queue: VecDeque::new(),
            broadcaster: tx,
        }
    }

    /// Adds a new execution to the end of the queue and returns a receiver for position updates.
    ///
    /// This method:
    /// 1. Creates a new receiver subscription to the position update channel
    /// 2. Adds the execution to the back of the queue (FIFO ordering)
    /// 3. Broadcasts updated queue positions to all listeners
    /// 4. Returns the receiver for the client to track its position
    ///
    /// # Arguments
    /// * `job` - The execution to add to the queue
    ///
    /// # Returns
    /// A broadcast receiver that the client can use to monitor queue position changes
    pub fn enqueue(&mut self, job: Execution) -> broadcast::Receiver<QueueUpdate> {
        let rx = self.broadcaster.subscribe();
        self.queue.push_back(job);
        self.broadcast_positions();
        rx
    }

    /// Removes and returns the next execution from the front of the queue.
    ///
    /// If an execution is removed, this method broadcasts updated queue positions
    /// to all listeners so they have accurate position information.
    ///
    /// # Returns
    /// Some(Execution) if the queue was not empty, or None if the queue was empty
    pub fn dequeue(&mut self) -> Option<Execution> {
        let job = self.queue.pop_front();
        if job.is_some() {
            self.broadcast_positions();
        }
        job
    }

    /// Broadcasts current queue positions to all subscribed clients.
    ///
    /// This method iterates through all executions in the queue and sends
    /// a QueueUpdate message for each one, containing:
    /// - The execution ID
    /// - Its current position in the queue (0 = next to be executed)
    ///
    /// The broadcast errors are ignored since they only occur when there are
    /// no active receivers or when the receivers' lagged too far behind.
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
