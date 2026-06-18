use crate::journal_adapter::JournalInputEntry;
use std::collections::VecDeque;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BoundedHandoffError {
    QueueFull,
}

pub struct BoundedHandoff {
    capacity: usize,
    entries: VecDeque<JournalInputEntry>,
}

impl BoundedHandoff {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            entries: VecDeque::new(),
        }
    }

    pub fn enqueue(&mut self, entry: JournalInputEntry) -> Result<(), BoundedHandoffError> {
        if self.entries.len() >= self.capacity {
            return Err(BoundedHandoffError::QueueFull);
        }

        self.entries.push_back(entry);
        Ok(())
    }

    pub fn prepend_entries(&mut self, entries: Vec<JournalInputEntry>) {
        for entry in entries.into_iter().rev() {
            self.entries.push_front(entry);
        }
    }

    pub fn drain_batch(&mut self, max_entries: usize) -> Vec<JournalInputEntry> {
        let mut drained = Vec::new();

        for _ in 0..max_entries {
            match self.entries.pop_front() {
                Some(entry) => drained.push(entry),
                None => break,
            }
        }

        drained
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn is_full(&self) -> bool {
        self.entries.len() >= self.capacity
    }

    pub fn available_capacity(&self) -> usize {
        self.capacity - self.entries.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::journal_adapter::JournalInputEntry;
    use crate::order::{Command, Order};
    use crate::types::{CommandId, JournalSeq, OrderId, Price, Quantity, Side, Symbol};

    fn symbol() -> Symbol {
        Symbol("BTC-USDT".to_string())
    }

    fn input_entry(seq: u64, command_id: u64, order_id: u64) -> JournalInputEntry {
        JournalInputEntry {
            seq: JournalSeq(seq),
            command_id: CommandId(command_id),
            command: Command::PlaceLimit(Order {
                order_id: OrderId(order_id),
                symbol: symbol(),
                side: Side::Buy,
                price: Price(100),
                quantity: Quantity(5),
            }),
        }
    }

    #[test]
    fn bounded_handoff_drains_entries_in_fifo_order() {
        let mut queue = BoundedHandoff::new(4);

        assert_eq!(queue.enqueue(input_entry(1, 10, 100)), Ok(()));
        assert_eq!(queue.enqueue(input_entry(2, 11, 101)), Ok(()));

        let entries = queue.drain_batch(10);

        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].seq, JournalSeq(1));
        assert_eq!(entries[1].seq, JournalSeq(2));
        assert_eq!(queue.len(), 0);
    }

    #[test]
    fn drain_batch_respects_max_entries_and_leaves_remaining_entries() {
        let mut queue = BoundedHandoff::new(4);

        assert_eq!(queue.enqueue(input_entry(1, 10, 100)), Ok(()));
        assert_eq!(queue.enqueue(input_entry(2, 11, 101)), Ok(()));
        assert_eq!(queue.enqueue(input_entry(3, 12, 102)), Ok(()));

        let first_batch = queue.drain_batch(2);

        assert_eq!(first_batch.len(), 2);
        assert_eq!(first_batch[0].seq, JournalSeq(1));
        assert_eq!(first_batch[1].seq, JournalSeq(2));
        assert_eq!(queue.len(), 1);

        let second_batch = queue.drain_batch(2);

        assert_eq!(second_batch.len(), 1);
        assert_eq!(second_batch[0].seq, JournalSeq(3));
        assert_eq!(queue.len(), 0);
    }

    #[test]
    fn bounded_handoff_exposes_capacity_and_watermark_state() {
        let mut queue = BoundedHandoff::new(2);

        assert_eq!(queue.capacity(), 2);
        assert_eq!(queue.len(), 0);
        assert_eq!(queue.available_capacity(), 2);
        assert!(queue.is_empty());
        assert!(!queue.is_full());

        assert_eq!(queue.enqueue(input_entry(1, 10, 100)), Ok(()));

        assert_eq!(queue.len(), 1);
        assert_eq!(queue.available_capacity(), 1);
        assert!(!queue.is_empty());
        assert!(!queue.is_full());

        assert_eq!(queue.enqueue(input_entry(2, 11, 101)), Ok(()));

        assert_eq!(queue.len(), 2);
        assert_eq!(queue.available_capacity(), 0);
        assert!(!queue.is_empty());
        assert!(queue.is_full());
    }

    #[test]
    fn bounded_handoff_returns_error_when_capacity_is_full() {
        let mut queue = BoundedHandoff::new(2);

        assert_eq!(queue.enqueue(input_entry(1, 10, 100)), Ok(()));
        assert_eq!(queue.enqueue(input_entry(2, 11, 101)), Ok(()));

        assert_eq!(
            queue.enqueue(input_entry(3, 12, 102)),
            Err(BoundedHandoffError::QueueFull)
        );

        assert_eq!(queue.len(), 2);

        let entries = queue.drain_batch(10);
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].seq, JournalSeq(1));
        assert_eq!(entries[1].seq, JournalSeq(2));
    }

    #[test]
    fn bounded_handoff_can_prepend_entries_back_to_front_in_original_order() {
        let mut queue = BoundedHandoff::new(4);

        assert_eq!(queue.enqueue(input_entry(3, 12, 102)), Ok(()));

        queue.prepend_entries(vec![input_entry(1, 10, 100), input_entry(2, 11, 101)]);

        let entries = queue.drain_batch(10);

        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].seq, JournalSeq(1));
        assert_eq!(entries[1].seq, JournalSeq(2));
        assert_eq!(entries[2].seq, JournalSeq(3));
    }
}
