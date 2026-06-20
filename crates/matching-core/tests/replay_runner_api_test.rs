use matching_core::journal_adapter::{JournalInputEntry, JournalInputReader, JournalOutputEntry};
use matching_core::matching_engine::{EngineEvent, OrderAck, RejectReason};
use matching_core::order::{Command, Order};
use matching_core::per_symbol_execution_loop::SymbolRuntime;
use matching_core::replay_runner::{ReplayComparisonResult, ReplayRunner};
use matching_core::snapshot_restore::{OrderBookSnapshot, SymbolRuntimeSnapshot};
use matching_core::types::{
    Checksum, CommandId, JournalSeq, MarketSeq, OrderId, Price, Quantity, Side, Symbol, TradeId,
};

struct TestJournalInputReader {
    entries: Vec<JournalInputEntry>,
}

impl TestJournalInputReader {
    fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }
}

impl JournalInputReader for TestJournalInputReader {
    fn append(&mut self, command_id: CommandId, command: Command) -> JournalSeq {
        let seq = JournalSeq(self.entries.len() as u64 + 1);

        self.entries.push(JournalInputEntry {
            seq,
            command_id,
            command,
        });

        seq
    }

    fn read_from(&self, from: JournalSeq) -> Vec<JournalInputEntry> {
        self.entries
            .iter()
            .filter(|entry| entry.seq >= from)
            .cloned()
            .collect()
    }

    fn latest_seq(&self) -> Option<JournalSeq> {
        self.entries.last().map(|entry| entry.seq)
    }
}

fn symbol() -> Symbol {
    Symbol("BTC-USDT".to_string())
}

fn limit_order(order_id: u64, side: Side, price: u64, quantity: u64) -> Command {
    Command::PlaceLimit(Order {
        order_id: OrderId(order_id),
        symbol: symbol(),
        side,
        price: Price(price),
        quantity: Quantity(quantity),
    })
}

fn accepted_output_entry(command_id: u64, order_id: u64, journal_seq: u64) -> JournalOutputEntry {
    JournalOutputEntry {
        command_id: CommandId(command_id),
        journal_seq: JournalSeq(journal_seq),
        events: vec![EngineEvent::OrderAck(OrderAck::Accepted {
            command_id: CommandId(command_id),
            order_id: OrderId(order_id),
            journal_seq: JournalSeq(journal_seq),
        })],
        output_commit_metadata: None,
    }
}

fn snapshot_after(journal: &TestJournalInputReader, count: usize) -> SymbolRuntimeSnapshot {
    let mut runtime = SymbolRuntime::new(symbol());

    for entry in journal.read_from(JournalSeq(1)).into_iter().take(count) {
        let request = runtime.process_entry_to_output_request(entry);
        runtime
            .mark_output_committed(request.journal_seq)
            .expect("test fixture should commit a contiguous prefix");
    }

    runtime.snapshot().expect("snapshot requires a safe point")
}

#[test]
fn replay_runner_is_available_from_public_api() {
    let mut journal = TestJournalInputReader::new();

    journal.append(CommandId(1), limit_order(1, Side::Buy, 100, 5));

    let checksum = ReplayRunner::new(symbol()).replay(&journal);

    assert_ne!(checksum, Checksum(0));
}

#[test]
fn replay_result_is_available_from_public_api() {
    let mut journal = TestJournalInputReader::new();

    journal.append(CommandId(1), limit_order(1, Side::Buy, 100, 5));

    let result = ReplayRunner::new(symbol()).replay_result(&journal);

    assert_ne!(result.checksum, Checksum(0));
    assert_eq!(result.last_replayed_seq, Some(JournalSeq(1)));
    assert_eq!(result.output_entries.len(), 1);
    assert_eq!(result.output_entries[0].journal_seq, JournalSeq(1));

    let comparison = result.compare_with(&result);

    assert_eq!(
        comparison,
        ReplayComparisonResult {
            output_entries_match: true,
            checksum_match: true,
            last_replayed_seq_match: true,
            first_output_mismatch_index: None,
            output_mismatch_window: None,
            actual_output_digest: comparison.actual_output_digest,
            expected_output_digest: comparison.expected_output_digest,
            actual_checksum: result.checksum,
            expected_checksum: result.checksum,
            actual_last_replayed_seq: result.last_replayed_seq,
            expected_last_replayed_seq: result.last_replayed_seq,
            actual_output_entry_at_mismatch: None,
            expected_output_entry_at_mismatch: None,
        }
    );
    assert!(comparison.is_match());
}

#[test]
fn replay_result_from_snapshot_is_available_from_public_api() {
    let mut journal = TestJournalInputReader::new();

    journal.append(CommandId(1), limit_order(1, Side::Buy, 100, 5));
    journal.append(CommandId(2), limit_order(2, Side::Sell, 100, 5));

    let snapshot = SymbolRuntimeSnapshot {
        order_book_snapshot: OrderBookSnapshot {
            symbol: symbol(),
            last_input_seq: JournalSeq(1),
            checksum: Checksum(0),
            resting_orders: vec![Order {
                order_id: OrderId(1),
                symbol: symbol(),
                side: Side::Buy,
                price: Price(100),
                quantity: Quantity(5),
            }],
        },
        next_trade_seq: 1,
        next_market_seq: 1,
        seen_command_ids: vec![CommandId(1)],
        seen_order_ids: vec![OrderId(1)],
    };

    let result = ReplayRunner::new(symbol()).replay_result_from_snapshot(snapshot, &journal);

    assert_eq!(result.last_replayed_seq, Some(JournalSeq(2)));
    assert_eq!(result.output_entries.len(), 1);
    assert_eq!(result.output_entries[0].journal_seq, JournalSeq(2));
}

#[test]
fn replay_result_from_snapshot_preserves_seen_order_ids_across_recovery() {
    let mut journal = TestJournalInputReader::new();

    journal.append(CommandId(10), limit_order(100, Side::Sell, 100, 1));
    journal.append(CommandId(11), limit_order(101, Side::Buy, 100, 1));
    journal.append(CommandId(12), limit_order(100, Side::Buy, 99, 1));

    let full_result = ReplayRunner::new(symbol()).replay_result(&journal);
    let expected_tail_result = matching_core::replay_runner::ReplayResult {
        checksum: full_result.checksum,
        last_replayed_seq: full_result.last_replayed_seq,
        output_entries: full_result.output_entries[2..].to_vec(),
    };

    let restored_tail_result = ReplayRunner::new(symbol())
        .replay_result_from_snapshot(snapshot_after(&journal, 2), &journal);

    assert!(restored_tail_result
        .compare_with(&expected_tail_result)
        .is_match());
    assert_eq!(
        restored_tail_result.output_entries[0].events,
        vec![EngineEvent::OrderAck(OrderAck::Rejected {
            command_id: CommandId(12),
            order_id: Some(OrderId(100)),
            journal_seq: JournalSeq(3),
            reason: RejectReason::DuplicateOrderId,
        })]
    );
}

#[test]
fn replay_result_from_snapshot_continues_trade_and_market_sequences_across_recovery() {
    let mut journal = TestJournalInputReader::new();

    journal.append(CommandId(10), limit_order(100, Side::Sell, 100, 1));
    journal.append(CommandId(11), limit_order(101, Side::Buy, 100, 1));
    journal.append(CommandId(12), limit_order(102, Side::Sell, 101, 1));
    journal.append(CommandId(13), limit_order(103, Side::Buy, 101, 1));

    let full_result = ReplayRunner::new(symbol()).replay_result(&journal);
    let expected_tail_result = matching_core::replay_runner::ReplayResult {
        checksum: full_result.checksum,
        last_replayed_seq: full_result.last_replayed_seq,
        output_entries: full_result.output_entries[2..].to_vec(),
    };

    let restored_tail_result = ReplayRunner::new(symbol())
        .replay_result_from_snapshot(snapshot_after(&journal, 2), &journal);

    assert!(restored_tail_result
        .compare_with(&expected_tail_result)
        .is_match());

    let tail_trade = restored_tail_result.output_entries[1]
        .events
        .iter()
        .find_map(|event| match event {
            EngineEvent::Trade(trade) => Some(trade),
            EngineEvent::OrderAck(_) => None,
        })
        .expect("tail buy should trade against the restored book");

    assert_eq!(tail_trade.trade_id, TradeId(2));
    assert_eq!(tail_trade.market_seq, MarketSeq(2));
}

#[test]
fn replay_result_comparison_reports_output_digests() {
    let expected = matching_core::replay_runner::ReplayResult {
        checksum: Checksum(1),
        last_replayed_seq: Some(JournalSeq(1)),
        output_entries: vec![accepted_output_entry(1, 100, 1)],
    };
    let actual = matching_core::replay_runner::ReplayResult {
        checksum: Checksum(1),
        last_replayed_seq: Some(JournalSeq(1)),
        output_entries: vec![accepted_output_entry(2, 101, 1)],
    };

    let same_comparison = expected.compare_with(&expected);
    let mismatch_comparison = actual.compare_with(&expected);

    assert_eq!(
        same_comparison.actual_output_digest,
        same_comparison.expected_output_digest
    );
    assert_ne!(
        mismatch_comparison.actual_output_digest,
        mismatch_comparison.expected_output_digest
    );
}

#[test]
fn replay_result_comparison_reports_output_mismatch_window() {
    let expected_entries = vec![
        accepted_output_entry(1, 101, 1),
        accepted_output_entry(2, 102, 2),
        accepted_output_entry(3, 103, 3),
        accepted_output_entry(4, 104, 4),
        accepted_output_entry(5, 105, 5),
        accepted_output_entry(6, 106, 6),
    ];
    let mut actual_entries = expected_entries.clone();
    actual_entries[3] = accepted_output_entry(40, 140, 4);

    let expected = matching_core::replay_runner::ReplayResult {
        checksum: Checksum(1),
        last_replayed_seq: Some(JournalSeq(6)),
        output_entries: expected_entries.clone(),
    };
    let actual = matching_core::replay_runner::ReplayResult {
        checksum: Checksum(1),
        last_replayed_seq: Some(JournalSeq(6)),
        output_entries: actual_entries.clone(),
    };

    let comparison = actual.compare_with(&expected);
    let window = comparison
        .output_mismatch_window
        .expect("mismatch should include surrounding output evidence");

    assert_eq!(window.start_index, 1);
    assert_eq!(window.actual_entries, actual_entries[1..6].to_vec());
    assert_eq!(window.expected_entries, expected_entries[1..6].to_vec());
}
