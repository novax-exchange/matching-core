use crate::journal_adapter::{JournalInputReader, JournalOutputEntry};
use crate::order::Command;
use crate::order_book::OrderBook;
use crate::output_commit_boundary::{digest_journal_output_entries, OutputDigest};
use crate::per_symbol_execution_loop::SymbolRuntime;
use crate::snapshot_restore::SymbolRuntimeSnapshot;
use crate::types::{Checksum, JournalSeq, Symbol};

pub struct ReplayRunner {
    symbol: Symbol,
}

pub struct ReplayResult {
    pub checksum: Checksum,
    pub last_replayed_seq: Option<JournalSeq>,
    pub output_entries: Vec<JournalOutputEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplayOutputMismatchWindow {
    pub start_index: usize,
    pub actual_entries: Vec<JournalOutputEntry>,
    pub expected_entries: Vec<JournalOutputEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplayComparisonResult {
    pub output_entries_match: bool,
    pub checksum_match: bool,
    pub last_replayed_seq_match: bool,
    pub first_output_mismatch_index: Option<usize>,
    pub output_mismatch_window: Option<ReplayOutputMismatchWindow>,
    pub actual_output_digest: OutputDigest,
    pub expected_output_digest: OutputDigest,
    pub actual_checksum: Checksum,
    pub expected_checksum: Checksum,
    pub actual_last_replayed_seq: Option<JournalSeq>,
    pub expected_last_replayed_seq: Option<JournalSeq>,
    pub actual_output_entry_at_mismatch: Option<JournalOutputEntry>,
    pub expected_output_entry_at_mismatch: Option<JournalOutputEntry>,
}

impl ReplayComparisonResult {
    pub fn is_match(&self) -> bool {
        self.output_entries_match && self.checksum_match && self.last_replayed_seq_match
    }
}

impl ReplayResult {
    pub fn compare_with(&self, expected: &ReplayResult) -> ReplayComparisonResult {
        let first_output_mismatch_index =
            first_output_mismatch_index(&self.output_entries, &expected.output_entries);
        let output_mismatch_window = first_output_mismatch_index.map(|index| {
            output_mismatch_window(&self.output_entries, &expected.output_entries, index)
        });

        ReplayComparisonResult {
            output_entries_match: self.output_entries == expected.output_entries,
            checksum_match: self.checksum == expected.checksum,
            last_replayed_seq_match: self.last_replayed_seq == expected.last_replayed_seq,
            first_output_mismatch_index,
            output_mismatch_window,
            actual_output_digest: digest_journal_output_entries(&self.output_entries),
            expected_output_digest: digest_journal_output_entries(&expected.output_entries),
            actual_checksum: self.checksum,
            expected_checksum: expected.checksum,
            actual_last_replayed_seq: self.last_replayed_seq,
            expected_last_replayed_seq: expected.last_replayed_seq,
            actual_output_entry_at_mismatch: first_output_mismatch_index
                .and_then(|index| self.output_entries.get(index).cloned()),
            expected_output_entry_at_mismatch: first_output_mismatch_index
                .and_then(|index| expected.output_entries.get(index).cloned()),
        }
    }
}

const OUTPUT_MISMATCH_WINDOW_RADIUS: usize = 2;

fn first_output_mismatch_index(
    actual: &[JournalOutputEntry],
    expected: &[JournalOutputEntry],
) -> Option<usize> {
    let shared_len = actual.len().min(expected.len());

    for index in 0..shared_len {
        if actual[index] != expected[index] {
            return Some(index);
        }
    }

    if actual.len() != expected.len() {
        Some(shared_len)
    } else {
        None
    }
}

fn output_mismatch_window(
    actual: &[JournalOutputEntry],
    expected: &[JournalOutputEntry],
    mismatch_index: usize,
) -> ReplayOutputMismatchWindow {
    let start_index = mismatch_index.saturating_sub(OUTPUT_MISMATCH_WINDOW_RADIUS);
    let end_index = actual
        .len()
        .max(expected.len())
        .min(mismatch_index + OUTPUT_MISMATCH_WINDOW_RADIUS + 1);

    ReplayOutputMismatchWindow {
        start_index,
        actual_entries: actual
            .get(start_index..end_index.min(actual.len()))
            .unwrap_or(&[])
            .to_vec(),
        expected_entries: expected
            .get(start_index..end_index.min(expected.len()))
            .unwrap_or(&[])
            .to_vec(),
    }
}

impl ReplayRunner {
    pub fn new(symbol: Symbol) -> Self {
        ReplayRunner { symbol }
    }

    pub fn replay(&self, journal: &dyn JournalInputReader) -> Checksum {
        self.replay_from(journal, JournalSeq(1))
    }

    pub fn replay_from(&self, journal: &dyn JournalInputReader, from: JournalSeq) -> Checksum {
        let book = OrderBook::new(self.symbol.clone());
        self.replay_from_order_book(book, journal, from)
    }

    pub fn replay_from_order_book(
        &self,
        mut book: OrderBook,
        journal: &dyn JournalInputReader,
        from: JournalSeq,
    ) -> Checksum {
        for entry in journal.read_from(from) {
            match entry.command {
                Command::PlaceLimit(order) => {
                    book.place_limit(order);
                }
                Command::Cancel { order_id, .. } => {
                    let _ = book.cancel(order_id);
                }
            }
        }

        book.checksum()
    }

    pub fn replay_result(&self, journal: &dyn JournalInputReader) -> ReplayResult {
        let mut runtime = SymbolRuntime::new(self.symbol.clone());
        let mut output_entries = Vec::new();

        for entry in journal.read_from(JournalSeq(1)) {
            let request = runtime.process_entry_to_output_request(entry);

            output_entries.push(JournalOutputEntry {
                command_id: request.command_id,
                journal_seq: request.journal_seq,
                events: request.events,
                output_commit_metadata: None,
            });
        }

        ReplayResult {
            checksum: runtime.checksum(),
            last_replayed_seq: journal.latest_seq(),
            output_entries,
        }
    }

    pub fn replay_result_from_snapshot(
        &self,
        snapshot: SymbolRuntimeSnapshot,
        journal: &dyn JournalInputReader,
    ) -> ReplayResult {
        let from = JournalSeq(snapshot.order_book_snapshot.last_input_seq.0 + 1);
        let mut last_replayed_seq = Some(snapshot.order_book_snapshot.last_input_seq);
        let mut runtime = SymbolRuntime::restore_from_snapshot(snapshot);
        let mut output_entries = Vec::new();

        for entry in journal.read_from(from) {
            let request = runtime.process_entry_to_output_request(entry);
            last_replayed_seq = Some(request.journal_seq);

            output_entries.push(JournalOutputEntry {
                command_id: request.command_id,
                journal_seq: request.journal_seq,
                events: request.events,
                output_commit_metadata: None,
            });
        }

        ReplayResult {
            checksum: runtime.checksum(),
            last_replayed_seq,
            output_entries,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::journal_adapter::{
        JournalAdapterError, JournalInputEntry, JournalInputReader, JournalOutputAppender,
        JournalOutputEntry,
    };
    use crate::matching_engine::EngineEvent;
    use crate::order::{Command, Order};
    use crate::output_commit_boundary::{
        run_output_batch_commit_step_report, OutputJournalClient, PendingOutputBuffer,
    };
    use crate::per_symbol_execution_loop::{
        advance_runtime_safe_point_from_output_commit, SymbolRuntime,
    };
    use crate::types::{Checksum, CommandId, JournalSeq, OrderId, Price, Quantity, Side, Symbol};

    struct InMemoryJournalInputReader {
        entries: Vec<JournalInputEntry>,
    }

    impl InMemoryJournalInputReader {
        fn new() -> Self {
            Self {
                entries: Vec::new(),
            }
        }
    }

    impl JournalInputReader for InMemoryJournalInputReader {
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

    struct InMemoryJournalOutputAppender {
        entries: Vec<JournalOutputEntry>,
    }

    impl InMemoryJournalOutputAppender {
        fn new() -> Self {
            Self {
                entries: Vec::new(),
            }
        }
    }

    impl JournalOutputAppender for InMemoryJournalOutputAppender {
        fn append(
            &mut self,
            command_id: CommandId,
            journal_seq: JournalSeq,
            events: Vec<EngineEvent>,
        ) -> Result<(), JournalAdapterError> {
            self.entries.push(JournalOutputEntry {
                command_id,
                journal_seq,
                events,
                output_commit_metadata: None,
            });

            Ok(())
        }

        fn read_all(&self) -> Vec<JournalOutputEntry> {
            self.entries.clone()
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

    fn cancel_order(order_id: u64) -> Command {
        Command::Cancel {
            order_id: OrderId(order_id),
            symbol: symbol(),
        }
    }

    fn live_result_through_async_output_commit(journal: &dyn JournalInputReader) -> ReplayResult {
        let entries = journal.read_from(JournalSeq(1));
        let (runtime, output_entries) = process_entries_through_async_output_commit(entries);

        ReplayResult {
            checksum: runtime.checksum(),
            last_replayed_seq: runtime.last_input_seq(),
            output_entries,
        }
    }

    fn process_entries_through_async_output_commit(
        entries: Vec<JournalInputEntry>,
    ) -> (SymbolRuntime, Vec<JournalOutputEntry>) {
        let mut runtime = SymbolRuntime::new(symbol());
        let mut pending_output_buffer = PendingOutputBuffer::new(entries.len());

        for entry in entries {
            assert_eq!(
                runtime.process_entry_into_pending_output_buffer(entry, &mut pending_output_buffer),
                Ok(())
            );
        }
        assert_eq!(runtime.last_input_seq(), None);

        let mut journal_client = OutputJournalClient::new();
        let mut live_output = InMemoryJournalOutputAppender::new();
        let report = run_output_batch_commit_step_report(
            &mut journal_client,
            &mut pending_output_buffer,
            &mut live_output,
            usize::MAX,
        );

        assert_eq!(report.blocking_seq, None);
        assert_eq!(report.blocking_outcome, None);
        assert_eq!(
            advance_runtime_safe_point_from_output_commit(&mut runtime, &report.commit_result),
            Ok(report.commit_result.committed_count)
        );

        (runtime, live_output.read_all())
    }

    #[test]
    fn replaying_same_journal_input_sequence_produces_same_checksum() {
        let mut journal = InMemoryJournalInputReader::new();

        journal.append(CommandId(1), limit_order(1, Side::Buy, 100, 5));
        journal.append(CommandId(2), limit_order(2, Side::Sell, 101, 3));
        journal.append(CommandId(3), limit_order(3, Side::Buy, 99, 2));

        let first = ReplayRunner::new(symbol()).replay(&journal);
        let second = ReplayRunner::new(symbol()).replay(&journal);

        assert_eq!(first, second);
        assert_ne!(first, Checksum(0));
    }

    #[test]
    fn replay_applies_cancel_commands_before_calculating_checksum() {
        let mut with_cancel = InMemoryJournalInputReader::new();

        with_cancel.append(CommandId(1), limit_order(1, Side::Buy, 100, 5));
        with_cancel.append(
            CommandId(2),
            Command::Cancel {
                order_id: OrderId(1),
                symbol: symbol(),
            },
        );

        let empty = InMemoryJournalInputReader::new();

        let cancelled_checksum = ReplayRunner::new(symbol()).replay(&with_cancel);
        let empty_checksum = ReplayRunner::new(symbol()).replay(&empty);

        assert_eq!(cancelled_checksum, empty_checksum);
    }

    #[test]
    fn replay_from_starts_at_requested_sequence() {
        let mut journal = InMemoryJournalInputReader::new();

        journal.append(CommandId(1), limit_order(1, Side::Buy, 100, 5));
        journal.append(CommandId(2), limit_order(2, Side::Buy, 101, 3));

        let replay_from_second = ReplayRunner::new(symbol()).replay_from(&journal, JournalSeq(2));

        let mut expected_journal = InMemoryJournalInputReader::new();
        expected_journal.append(CommandId(2), limit_order(2, Side::Buy, 101, 3));

        let expected = ReplayRunner::new(symbol()).replay(&expected_journal);

        assert_eq!(replay_from_second, expected);
    }

    #[test]
    fn replay_result_exposes_final_checksum() {
        let mut journal = InMemoryJournalInputReader::new();

        journal.append(CommandId(1), limit_order(1, Side::Buy, 100, 5));

        let result = ReplayRunner::new(symbol()).replay_result(&journal);

        assert_ne!(result.checksum, Checksum(0));
    }

    #[test]
    fn replay_result_exposes_last_replayed_sequence() {
        let mut journal = InMemoryJournalInputReader::new();

        journal.append(CommandId(1), limit_order(1, Side::Buy, 100, 5));
        journal.append(CommandId(2), limit_order(2, Side::Sell, 101, 3));

        let result = ReplayRunner::new(symbol()).replay_result(&journal);

        assert_eq!(result.last_replayed_seq, Some(JournalSeq(2)));
    }

    #[test]
    fn replay_result_has_no_last_replayed_sequence_for_empty_journal() {
        let journal = InMemoryJournalInputReader::new();

        let result = ReplayRunner::new(symbol()).replay_result(&journal);

        assert_eq!(result.last_replayed_seq, None);
    }

    #[test]
    fn replay_result_generates_same_output_entries_as_live_runtime() {
        let mut journal = InMemoryJournalInputReader::new();

        journal.append(CommandId(10), limit_order(100, Side::Sell, 100, 3));
        journal.append(CommandId(11), limit_order(101, Side::Buy, 100, 3));

        let live_result = live_result_through_async_output_commit(&journal);

        let replay_result = ReplayRunner::new(symbol()).replay_result(&journal);

        assert_eq!(replay_result.output_entries, live_result.output_entries);
        assert_eq!(
            replay_result.last_replayed_seq,
            live_result.last_replayed_seq
        );
    }

    #[test]
    fn replay_result_matches_live_output_for_reject_cancel_duplicate_and_trade_scenarios() {
        let mut journal = InMemoryJournalInputReader::new();

        journal.append(CommandId(10), limit_order(100, Side::Sell, 100, 3));
        journal.append(CommandId(11), limit_order(101, Side::Buy, 100, 2));
        journal.append(CommandId(12), cancel_order(100));
        journal.append(CommandId(13), cancel_order(999));
        journal.append(CommandId(14), limit_order(102, Side::Buy, 0, 1));
        journal.append(CommandId(15), limit_order(101, Side::Sell, 101, 1));
        journal.append(CommandId(11), limit_order(103, Side::Buy, 99, 1));
        journal.append(CommandId(16), limit_order(104, Side::Buy, 99, 1));

        let live_result = live_result_through_async_output_commit(&journal);

        let replay_result = ReplayRunner::new(symbol()).replay_result(&journal);
        let comparison = replay_result.compare_with(&live_result);

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
                actual_checksum: live_result.checksum,
                expected_checksum: live_result.checksum,
                actual_last_replayed_seq: live_result.last_replayed_seq,
                expected_last_replayed_seq: live_result.last_replayed_seq,
                actual_output_entry_at_mismatch: None,
                expected_output_entry_at_mismatch: None,
            }
        );
        assert!(comparison.is_match());
    }

    #[test]
    fn replay_result_from_snapshot_matches_full_replay_tail_output_checksum_and_safe_point() {
        let mut journal = InMemoryJournalInputReader::new();

        journal.append(CommandId(10), limit_order(100, Side::Sell, 100, 1));
        journal.append(CommandId(11), limit_order(101, Side::Buy, 100, 1));
        journal.append(CommandId(12), limit_order(102, Side::Sell, 100, 1));
        journal.append(CommandId(13), limit_order(103, Side::Buy, 100, 1));

        let (runtime_to_snapshot, _) = process_entries_through_async_output_commit(
            journal
                .read_from(JournalSeq(1))
                .into_iter()
                .take(2)
                .collect(),
        );

        let snapshot = runtime_to_snapshot
            .snapshot()
            .expect("snapshot requires a safe point");
        let full_result = ReplayRunner::new(symbol()).replay_result(&journal);
        let expected_tail_result = ReplayResult {
            checksum: full_result.checksum,
            last_replayed_seq: full_result.last_replayed_seq,
            output_entries: full_result.output_entries[2..].to_vec(),
        };

        let restored_tail_result =
            ReplayRunner::new(symbol()).replay_result_from_snapshot(snapshot, &journal);

        let comparison = restored_tail_result.compare_with(&expected_tail_result);

        assert!(comparison.is_match());
    }

    #[test]
    fn replay_result_comparison_reports_mismatched_dimensions() {
        let expected = ReplayResult {
            checksum: Checksum(1),
            last_replayed_seq: Some(JournalSeq(1)),
            output_entries: Vec::new(),
        };
        let actual = ReplayResult {
            checksum: Checksum(2),
            last_replayed_seq: Some(JournalSeq(2)),
            output_entries: Vec::new(),
        };

        let comparison = actual.compare_with(&expected);

        assert_eq!(
            comparison,
            ReplayComparisonResult {
                output_entries_match: true,
                checksum_match: false,
                last_replayed_seq_match: false,
                first_output_mismatch_index: None,
                output_mismatch_window: None,
                actual_output_digest: comparison.actual_output_digest,
                expected_output_digest: comparison.expected_output_digest,
                actual_checksum: Checksum(2),
                expected_checksum: Checksum(1),
                actual_last_replayed_seq: Some(JournalSeq(2)),
                expected_last_replayed_seq: Some(JournalSeq(1)),
                actual_output_entry_at_mismatch: None,
                expected_output_entry_at_mismatch: None,
            }
        );
        assert!(!comparison.is_match());
    }

    #[test]
    fn replay_result_comparison_reports_first_output_mismatch_index() {
        let expected_entry = JournalOutputEntry {
            command_id: CommandId(1),
            journal_seq: JournalSeq(1),
            events: Vec::new(),
            output_commit_metadata: None,
        };
        let expected = ReplayResult {
            checksum: Checksum(1),
            last_replayed_seq: Some(JournalSeq(1)),
            output_entries: vec![expected_entry.clone()],
        };
        let actual = ReplayResult {
            checksum: Checksum(1),
            last_replayed_seq: Some(JournalSeq(1)),
            output_entries: Vec::new(),
        };

        let comparison = actual.compare_with(&expected);

        assert!(!comparison.output_entries_match);
        assert_eq!(comparison.first_output_mismatch_index, Some(0));
        assert_eq!(comparison.actual_output_entry_at_mismatch, None);
        assert_eq!(
            comparison.expected_output_entry_at_mismatch,
            Some(expected_entry)
        );
        assert!(!comparison.is_match());
    }

    #[test]
    fn replay_result_comparison_reports_actual_and_expected_output_entry_at_mismatch() {
        let expected_entry = JournalOutputEntry {
            command_id: CommandId(1),
            journal_seq: JournalSeq(1),
            events: Vec::new(),
            output_commit_metadata: None,
        };
        let actual_entry = JournalOutputEntry {
            command_id: CommandId(2),
            journal_seq: JournalSeq(1),
            events: Vec::new(),
            output_commit_metadata: None,
        };
        let expected = ReplayResult {
            checksum: Checksum(1),
            last_replayed_seq: Some(JournalSeq(1)),
            output_entries: vec![expected_entry.clone()],
        };
        let actual = ReplayResult {
            checksum: Checksum(1),
            last_replayed_seq: Some(JournalSeq(1)),
            output_entries: vec![actual_entry.clone()],
        };

        let comparison = actual.compare_with(&expected);

        assert!(!comparison.output_entries_match);
        assert_eq!(comparison.first_output_mismatch_index, Some(0));
        assert_eq!(
            comparison.actual_output_entry_at_mismatch,
            Some(actual_entry)
        );
        assert_eq!(
            comparison.expected_output_entry_at_mismatch,
            Some(expected_entry)
        );
        assert!(!comparison.is_match());
    }
}
