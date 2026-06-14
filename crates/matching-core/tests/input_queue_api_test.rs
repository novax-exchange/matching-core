use matching_core::input_queue::{InputQueueError, PerSymbolInputQueue};
use matching_core::journal::InputJournalEntry;
use matching_core::order::{Command, Order};
use matching_core::types::{
    CommandId, JournalSeq, OrderId, Price, Quantity, Side, Symbol,
};

fn command_entry(seq: u64) -> InputJournalEntry {
    InputJournalEntry {
        seq: JournalSeq(seq),
        command_id: CommandId(seq),
        command: Command::PlaceLimit(Order {
            order_id: OrderId(seq),
            symbol: Symbol("BTC-USDT".to_string()),
            side: Side::Buy,
            price: Price(100),
            quantity: Quantity(1),
        }),
    }
}

#[test]
fn input_queue_is_available_from_public_api() {
    let mut queue = PerSymbolInputQueue::new(2);

    assert_eq!(queue.capacity(), 2);
    assert_eq!(queue.enqueue(command_entry(1)), Ok(()));
    assert_eq!(queue.enqueue(command_entry(2)), Ok(()));
    assert_eq!(queue.enqueue(command_entry(3)), Err(InputQueueError::QueueFull));

    let entries = queue.drain_batch(10);

    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].seq, JournalSeq(1));
    assert_eq!(entries[1].seq, JournalSeq(2));
}