use matching_core::bounded_handoff::{BoundedHandoff, BoundedHandoffError};
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
fn bounded_handoff_is_available_from_public_api() {
    let mut queue = BoundedHandoff::new(2);

    assert_eq!(queue.capacity(), 2);
    assert_eq!(queue.enqueue(command_entry(1)), Ok(()));
    assert_eq!(queue.enqueue(command_entry(2)), Ok(()));
    assert_eq!(queue.enqueue(command_entry(3)), Err(BoundedHandoffError::QueueFull));

    let entries = queue.drain_batch(10);

    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].seq, JournalSeq(1));
    assert_eq!(entries[1].seq, JournalSeq(2));
}

#[test]
fn bounded_handoff_can_prepend_entries_from_public_api() {
    let mut queue = BoundedHandoff::new(4);

    assert_eq!(queue.enqueue(command_entry(3)), Ok(()));

    queue.prepend_entries(vec![
        command_entry(1),
        command_entry(2),
    ]);

    let entries = queue.drain_batch(10);

    assert_eq!(entries.len(), 3);
    assert_eq!(entries[0].seq, JournalSeq(1));
    assert_eq!(entries[1].seq, JournalSeq(2));
    assert_eq!(entries[2].seq, JournalSeq(3));
}
