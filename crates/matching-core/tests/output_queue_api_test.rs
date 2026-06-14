use matching_core::engine::{EngineEvent, OrderAck};
use matching_core::output_committer::OutputCommitRequest;
use matching_core::output_queue::{OutputQueue, OutputQueueError};
use matching_core::types::{CommandId, JournalSeq, OrderId};

fn request(seq: u64) -> OutputCommitRequest {
    OutputCommitRequest {
        command_id: CommandId(seq),
        journal_seq: JournalSeq(seq),
        events: vec![EngineEvent::OrderAck(OrderAck::Accepted {
            command_id: CommandId(seq),
            order_id: OrderId(seq),
            journal_seq: JournalSeq(seq),
        })],
    }
}

#[test]
fn output_queue_is_available_from_public_api() {
    let mut queue = OutputQueue::new(2);

    assert_eq!(queue.capacity(), 2);
    assert_eq!(queue.enqueue(request(1)), Ok(()));
    assert_eq!(queue.enqueue(request(2)), Ok(()));
    assert_eq!(queue.enqueue(request(3)), Err(OutputQueueError::QueueFull));

    let requests = queue.drain_batch(10);

    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].journal_seq, JournalSeq(1));
    assert_eq!(requests[1].journal_seq, JournalSeq(2));
}

#[test]
fn output_queue_can_prepend_requests_from_public_api() {
    let mut queue = OutputQueue::new(4);

    assert_eq!(queue.enqueue(request(3)), Ok(()));

    queue.prepend_requests(vec![request(1), request(2)]);

    let requests = queue.drain_batch(10);

    assert_eq!(requests.len(), 3);
    assert_eq!(requests[0].journal_seq, JournalSeq(1));
    assert_eq!(requests[1].journal_seq, JournalSeq(2));
    assert_eq!(requests[2].journal_seq, JournalSeq(3));
}
