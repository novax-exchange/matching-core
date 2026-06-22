use matching_core::journal_adapter::{JournalOutputCommitMetadata, JournalOutputEntry};
use matching_core::matching_engine::{EngineEvent, OrderAck};
use matching_core::output_commit_boundary::{
    OutputCommitMetadataIndex, OutputCommitMetadataIndexError, OutputCommitMetadataLookup,
};
use matching_core::types::{CommandId, JournalSeq, OrderId, Symbol};

fn metadata(batch_id: &str, digest: u64) -> JournalOutputCommitMetadata {
    JournalOutputCommitMetadata {
        batch_id: batch_id.to_string(),
        symbol: Symbol("BTC-USDT".to_string()),
        shard_id: None,
        shard_sequence: None,
        input_seq_start: JournalSeq(1),
        input_seq_end: JournalSeq(2),
        entry_count: 2,
        matching_version: 1,
        output_digest: digest,
    }
}

fn output_entry(seq: u64, metadata: Option<JournalOutputCommitMetadata>) -> JournalOutputEntry {
    JournalOutputEntry {
        command_id: CommandId(seq),
        journal_seq: JournalSeq(seq),
        events: vec![EngineEvent::OrderAck(OrderAck::Accepted {
            command_id: CommandId(seq),
            order_id: OrderId(seq),
            journal_seq: JournalSeq(seq),
        })],
        output_commit_metadata: metadata,
    }
}

#[test]
fn output_commit_metadata_index_rebuilds_from_journal_output_entries_from_public_api() {
    let first = metadata("BTC-USDT:1-2:2:v1", 11);
    let second = metadata("BTC-USDT:3-4:2:v1", 22);
    let entries = vec![
        output_entry(1, Some(first.clone())),
        output_entry(2, None),
        output_entry(3, Some(second.clone())),
    ];

    let index = OutputCommitMetadataIndex::rebuild_from_entries(&entries)
        .expect("metadata index should rebuild from durable output entries");

    assert_eq!(index.get("BTC-USDT:1-2:2:v1"), Some(&first));
    assert_eq!(index.get("BTC-USDT:3-4:2:v1"), Some(&second));
    assert_eq!(index.get("BTC-USDT:5-6:2:v1"), None);
    assert_eq!(index.observed_entry_count("BTC-USDT:1-2:2:v1"), 1);
    assert!(!index.is_complete("BTC-USDT:1-2:2:v1"));
    assert_eq!(index.observed_entry_count("BTC-USDT:3-4:2:v1"), 1);
    assert!(!index.is_complete("BTC-USDT:3-4:2:v1"));
    assert_eq!(
        index.lookup("BTC-USDT:1-2:2:v1"),
        OutputCommitMetadataLookup::Incomplete {
            metadata: first,
            observed_entry_count: 1,
        }
    );
    assert_eq!(
        index.lookup("BTC-USDT:5-6:2:v1"),
        OutputCommitMetadataLookup::Missing
    );
}

#[test]
fn output_commit_metadata_index_reports_complete_batch_when_entry_count_is_observed_from_public_api(
) {
    let batch_metadata = metadata("BTC-USDT:1-2:2:v1", 11);
    let entries = vec![
        output_entry(1, Some(batch_metadata.clone())),
        output_entry(2, Some(batch_metadata.clone())),
    ];

    let index = OutputCommitMetadataIndex::rebuild_from_entries(&entries)
        .expect("metadata index should rebuild from durable output entries");

    assert_eq!(index.get("BTC-USDT:1-2:2:v1"), Some(&batch_metadata));
    assert_eq!(index.observed_entry_count("BTC-USDT:1-2:2:v1"), 2);
    assert!(index.is_complete("BTC-USDT:1-2:2:v1"));
    assert_eq!(
        index.lookup("BTC-USDT:1-2:2:v1"),
        OutputCommitMetadataLookup::Complete {
            metadata: batch_metadata,
        }
    );
}

#[test]
fn output_commit_metadata_index_rejects_same_batch_id_with_different_digest_from_public_api() {
    let original = metadata("BTC-USDT:1-2:2:v1", 11);
    let drifted = metadata("BTC-USDT:1-2:2:v1", 99);
    let entries = vec![
        output_entry(1, Some(original.clone())),
        output_entry(2, Some(drifted.clone())),
    ];

    assert_eq!(
        OutputCommitMetadataIndex::rebuild_from_entries(&entries),
        Err(OutputCommitMetadataIndexError::Conflict {
            batch_id: "BTC-USDT:1-2:2:v1".to_string(),
            existing: original,
            incoming: drifted,
        })
    );
}
