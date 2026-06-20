use matching_core::matching_engine::{EngineEvent, OrderAck};
use matching_core::output_commit_boundary::OutputCommitRequest;
use matching_core::output_commit_boundary::{PendingOutputBuffer, PendingOutputBufferError};
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
fn pending_output_buffer_is_available_from_public_api() {
    let mut buffer = PendingOutputBuffer::new(2);

    assert_eq!(buffer.capacity(), 2);
    assert_eq!(buffer.enqueue(request(1)), Ok(()));
    assert_eq!(buffer.enqueue(request(2)), Ok(()));
    assert_eq!(
        buffer.enqueue(request(3)),
        Err(PendingOutputBufferError::BufferFull)
    );

    let requests = buffer.drain_batch(10);

    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].journal_seq, JournalSeq(1));
    assert_eq!(requests[1].journal_seq, JournalSeq(2));
}

#[test]
fn pending_output_buffer_can_prepend_requests_from_public_api() {
    let mut buffer = PendingOutputBuffer::new(4);

    assert_eq!(buffer.enqueue(request(3)), Ok(()));

    buffer.prepend_requests(vec![request(1), request(2)]);

    let requests = buffer.drain_batch(10);

    assert_eq!(requests.len(), 3);
    assert_eq!(requests[0].journal_seq, JournalSeq(1));
    assert_eq!(requests[1].journal_seq, JournalSeq(2));
    assert_eq!(requests[2].journal_seq, JournalSeq(3));
}
