//! Output Commit Boundary: pending output batch buffer.
//!
//! Current scope: bounded in-memory queue for output requests waiting for
//! durable append. It is transfer state only; recovery must come from Journal
//! input/output records, not from this queue.

use super::output_journal_client::OutputCommitRequest;
use std::collections::VecDeque;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PendingOutputBufferError {
    BufferFull,
}

pub struct PendingOutputBuffer {
    capacity: usize,
    requests: VecDeque<OutputCommitRequest>,
}

impl PendingOutputBuffer {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            requests: VecDeque::new(),
        }
    }

    pub fn enqueue(
        &mut self,
        request: OutputCommitRequest,
    ) -> Result<(), PendingOutputBufferError> {
        if self.requests.len() >= self.capacity {
            return Err(PendingOutputBufferError::BufferFull);
        }

        self.requests.push_back(request);
        Ok(())
    }

    pub fn prepend_requests(&mut self, requests: Vec<OutputCommitRequest>) {
        for request in requests.into_iter().rev() {
            self.requests.push_front(request);
        }
    }

    pub fn drain_batch(&mut self, max_requests: usize) -> Vec<OutputCommitRequest> {
        let mut drained = Vec::new();

        for _ in 0..max_requests {
            match self.requests.pop_front() {
                Some(request) => drained.push(request),
                None => break,
            }
        }

        drained
    }

    pub fn len(&self) -> usize {
        self.requests.len()
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn is_empty(&self) -> bool {
        self.requests.is_empty()
    }

    pub fn is_full(&self) -> bool {
        self.requests.len() >= self.capacity
    }

    pub fn available_capacity(&self) -> usize {
        self.capacity - self.requests.len()
    }
}
