use crate::journal_adapter::JournalInputReader;
use crate::order::Order;
use crate::order_book::OrderBook;
use crate::output_commit_boundary::OutputDigest;
use crate::replay_runner::{ReplayComparisonResult, ReplayResult, ReplayRunner};
use crate::runtime_config::SnapshotVerificationConfig;
use crate::snapshot_store::{FileSnapshotStore, SnapshotManifestSigner, SnapshotStoreError};
use crate::types::*;
use std::collections::HashMap;

const SNAPSHOT_MAGIC: &[u8; 8] = b"NVXSNP01";
pub const SYMBOL_RUNTIME_SNAPSHOT_FORMAT_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SymbolRuntimeSnapshot {
    pub order_book_snapshot: OrderBookSnapshot,
    pub next_trade_seq: u64,
    pub next_market_seq: u64,
    pub seen_command_ids: Vec<CommandId>,
    pub seen_order_ids: Vec<OrderId>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderBookSnapshot {
    pub symbol: Symbol,
    pub last_input_seq: JournalSeq,
    pub checksum: Checksum,
    pub resting_orders: Vec<Order>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotVerificationReport {
    pub symbol: Symbol,
    pub safe_point: JournalSeq,
    pub comparison: ReplayComparisonResult,
    pub verified_manifest_written: bool,
    pub failure_evidence: Option<SnapshotVerificationFailureEvidence>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotVerificationFailureEvidence {
    pub symbol: Symbol,
    pub safe_point: JournalSeq,
    pub mismatched_dimensions: Vec<SnapshotVerificationMismatchDimension>,
    pub first_output_mismatch_index: Option<usize>,
    pub actual_output_digest: OutputDigest,
    pub expected_output_digest: OutputDigest,
    pub actual_checksum: Checksum,
    pub expected_checksum: Checksum,
    pub actual_last_replayed_seq: Option<JournalSeq>,
    pub expected_last_replayed_seq: Option<JournalSeq>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SnapshotVerificationMismatchDimension {
    OutputEntries,
    Checksum,
    SafePoint,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotVerificationSchedulingReport {
    pub symbol: Symbol,
    pub outcome: SnapshotVerificationSchedulingOutcome,
    pub candidate_safe_point: Option<JournalSeq>,
    pub skipped_already_verified_safe_points: Vec<JournalSeq>,
    pub verification: Option<SnapshotVerificationReport>,
    pub failure_evidence: Option<SnapshotVerificationFailureEvidence>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SnapshotVerificationSchedulingOutcome {
    NoCandidate,
    Verified,
    Mismatch,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotVerificationRetryDecision {
    pub action: SnapshotVerificationRetryAction,
    pub symbol: Symbol,
    pub safe_point: Option<JournalSeq>,
    pub attempt_count: usize,
    pub failure_evidence: Option<SnapshotVerificationFailureEvidence>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotVerificationRetryAction {
    NoAction,
    Retry,
    Escalate,
    Clear,
}

#[derive(Debug, Clone)]
pub struct SnapshotVerificationRetryTracker {
    config: SnapshotVerificationConfig,
    mismatch_attempts: HashMap<(Symbol, JournalSeq), usize>,
}

impl SnapshotVerificationRetryTracker {
    pub fn new(config: SnapshotVerificationConfig) -> Self {
        Self {
            config,
            mismatch_attempts: HashMap::new(),
        }
    }

    pub fn record_scheduling_report(
        &mut self,
        report: &SnapshotVerificationSchedulingReport,
    ) -> SnapshotVerificationRetryDecision {
        match report.outcome {
            SnapshotVerificationSchedulingOutcome::NoCandidate => {
                SnapshotVerificationRetryDecision {
                    action: SnapshotVerificationRetryAction::NoAction,
                    symbol: report.symbol.clone(),
                    safe_point: report.candidate_safe_point,
                    attempt_count: 0,
                    failure_evidence: None,
                }
            }
            SnapshotVerificationSchedulingOutcome::Verified => {
                if let Some(safe_point) = report.candidate_safe_point {
                    self.mismatch_attempts
                        .remove(&(report.symbol.clone(), safe_point));
                }

                SnapshotVerificationRetryDecision {
                    action: SnapshotVerificationRetryAction::Clear,
                    symbol: report.symbol.clone(),
                    safe_point: report.candidate_safe_point,
                    attempt_count: 0,
                    failure_evidence: None,
                }
            }
            SnapshotVerificationSchedulingOutcome::Mismatch => {
                let Some(safe_point) = report.candidate_safe_point else {
                    return SnapshotVerificationRetryDecision {
                        action: SnapshotVerificationRetryAction::NoAction,
                        symbol: report.symbol.clone(),
                        safe_point: None,
                        attempt_count: 0,
                        failure_evidence: report.failure_evidence.clone(),
                    };
                };

                let key = (report.symbol.clone(), safe_point);
                let attempt_count = self.mismatch_attempts.entry(key).or_insert(0);
                *attempt_count += 1;

                let action = if *attempt_count >= self.config.max_mismatch_attempts {
                    SnapshotVerificationRetryAction::Escalate
                } else {
                    SnapshotVerificationRetryAction::Retry
                };

                SnapshotVerificationRetryDecision {
                    action,
                    symbol: report.symbol.clone(),
                    safe_point: Some(safe_point),
                    attempt_count: *attempt_count,
                    failure_evidence: report.failure_evidence.clone(),
                }
            }
        }
    }

    pub fn mismatch_attempt_count(&self, symbol: &Symbol, safe_point: JournalSeq) -> usize {
        self.mismatch_attempts
            .get(&(symbol.clone(), safe_point))
            .copied()
            .unwrap_or(0)
    }
}

#[derive(Debug, Clone)]
pub struct SnapshotVerificationOrchestrator {
    symbol: Symbol,
}

impl SnapshotVerificationOrchestrator {
    pub fn new(symbol: Symbol) -> Self {
        Self { symbol }
    }

    pub fn verify_and_sign_snapshot_candidate(
        &self,
        snapshot: &SymbolRuntimeSnapshot,
        journal: &dyn JournalInputReader,
        expected: &ReplayResult,
        snapshot_store: &FileSnapshotStore,
        signer: &SnapshotManifestSigner,
    ) -> Result<SnapshotVerificationReport, SnapshotStoreError> {
        let safe_point = snapshot.order_book_snapshot.last_input_seq;
        let actual = ReplayRunner::new(self.symbol.clone())
            .replay_result_from_snapshot(snapshot.clone(), journal);
        let comparison = actual.compare_with(expected);
        let failure_evidence =
            snapshot_verification_failure_evidence(&self.symbol, safe_point, &comparison);
        let verified_manifest_written = if comparison.is_match() {
            snapshot_store
                .mark_symbol_snapshot_verified_by(&self.symbol, safe_point, signer)?
                .is_some()
        } else {
            false
        };

        Ok(SnapshotVerificationReport {
            symbol: self.symbol.clone(),
            safe_point,
            comparison,
            verified_manifest_written,
            failure_evidence,
        })
    }

    pub fn run_once(
        &self,
        journal: &dyn JournalInputReader,
        snapshot_store: &FileSnapshotStore,
        signer: &SnapshotManifestSigner,
    ) -> Result<SnapshotVerificationSchedulingReport, SnapshotStoreError> {
        let mut skipped_already_verified_safe_points = Vec::new();

        for record in snapshot_store
            .symbol_snapshot_records(&self.symbol)?
            .into_iter()
            .rev()
        {
            if snapshot_store
                .load_symbol_snapshot_verification_manifest(&self.symbol, record.safe_point)?
                .is_some()
            {
                skipped_already_verified_safe_points.push(record.safe_point);
                continue;
            }

            let snapshot = SymbolRuntimeSnapshot::from_canonical_bytes(&record.bytes)
                .map_err(SnapshotStoreError::SnapshotSerialization)?;
            let full_replay_result = ReplayRunner::new(self.symbol.clone()).replay_result(journal);
            let expected = expected_replay_tail_result_after_safe_point(
                &full_replay_result,
                record.safe_point,
            );
            let verification = self.verify_and_sign_snapshot_candidate(
                &snapshot,
                journal,
                &expected,
                snapshot_store,
                signer,
            )?;
            let outcome = if verification.comparison.is_match() {
                SnapshotVerificationSchedulingOutcome::Verified
            } else {
                SnapshotVerificationSchedulingOutcome::Mismatch
            };

            return Ok(SnapshotVerificationSchedulingReport {
                symbol: self.symbol.clone(),
                outcome,
                candidate_safe_point: Some(record.safe_point),
                skipped_already_verified_safe_points,
                failure_evidence: verification.failure_evidence.clone(),
                verification: Some(verification),
            });
        }

        Ok(SnapshotVerificationSchedulingReport {
            symbol: self.symbol.clone(),
            outcome: SnapshotVerificationSchedulingOutcome::NoCandidate,
            candidate_safe_point: None,
            skipped_already_verified_safe_points,
            verification: None,
            failure_evidence: None,
        })
    }
}

fn snapshot_verification_failure_evidence(
    symbol: &Symbol,
    safe_point: JournalSeq,
    comparison: &ReplayComparisonResult,
) -> Option<SnapshotVerificationFailureEvidence> {
    if comparison.is_match() {
        return None;
    }

    let mut mismatched_dimensions = Vec::new();

    if !comparison.output_entries_match {
        mismatched_dimensions.push(SnapshotVerificationMismatchDimension::OutputEntries);
    }
    if !comparison.checksum_match {
        mismatched_dimensions.push(SnapshotVerificationMismatchDimension::Checksum);
    }
    if !comparison.last_replayed_seq_match {
        mismatched_dimensions.push(SnapshotVerificationMismatchDimension::SafePoint);
    }

    Some(SnapshotVerificationFailureEvidence {
        symbol: symbol.clone(),
        safe_point,
        mismatched_dimensions,
        first_output_mismatch_index: comparison.first_output_mismatch_index,
        actual_output_digest: comparison.actual_output_digest,
        expected_output_digest: comparison.expected_output_digest,
        actual_checksum: comparison.actual_checksum,
        expected_checksum: comparison.expected_checksum,
        actual_last_replayed_seq: comparison.actual_last_replayed_seq,
        expected_last_replayed_seq: comparison.expected_last_replayed_seq,
    })
}

fn expected_replay_tail_result_after_safe_point(
    full_replay_result: &ReplayResult,
    safe_point: JournalSeq,
) -> ReplayResult {
    ReplayResult {
        checksum: full_replay_result.checksum,
        last_replayed_seq: full_replay_result.last_replayed_seq,
        output_entries: full_replay_result
            .output_entries
            .iter()
            .filter(|entry| entry.journal_seq.0 > safe_point.0)
            .cloned()
            .collect(),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SnapshotSerializationError {
    InvalidMagic,
    UnsupportedVersion(u32),
    UnexpectedEof,
    InvalidUtf8,
    InvalidSide(u8),
    TrailingBytes(usize),
}

impl SymbolRuntimeSnapshot {
    pub fn to_canonical_bytes(&self) -> Vec<u8> {
        let mut writer = SnapshotWriter::new();
        let mut seen_command_ids = self.seen_command_ids.clone();
        let mut seen_order_ids = self.seen_order_ids.clone();

        seen_command_ids.sort_by_key(|command_id| command_id.0);
        seen_order_ids.sort_by_key(|order_id| order_id.0);

        writer.write_bytes(SNAPSHOT_MAGIC);
        writer.write_u32(SYMBOL_RUNTIME_SNAPSHOT_FORMAT_VERSION);
        writer.write_string(&self.order_book_snapshot.symbol.0);
        writer.write_u64(self.order_book_snapshot.last_input_seq.0);
        writer.write_u64(self.order_book_snapshot.checksum.0);
        writer.write_orders(&self.order_book_snapshot.resting_orders);
        writer.write_u64(self.next_trade_seq);
        writer.write_u64(self.next_market_seq);
        writer.write_command_ids(&seen_command_ids);
        writer.write_order_ids(&seen_order_ids);

        writer.into_bytes()
    }

    pub fn from_canonical_bytes(bytes: &[u8]) -> Result<Self, SnapshotSerializationError> {
        let mut reader = SnapshotReader::new(bytes);

        if reader.read_bytes(SNAPSHOT_MAGIC.len())? != SNAPSHOT_MAGIC {
            return Err(SnapshotSerializationError::InvalidMagic);
        }

        let version = reader.read_u32()?;
        if version != SYMBOL_RUNTIME_SNAPSHOT_FORMAT_VERSION {
            return Err(SnapshotSerializationError::UnsupportedVersion(version));
        }

        let symbol = Symbol(reader.read_string()?);
        let last_input_seq = JournalSeq(reader.read_u64()?);
        let checksum = Checksum(reader.read_u64()?);
        let resting_orders = reader.read_orders()?;
        let next_trade_seq = reader.read_u64()?;
        let next_market_seq = reader.read_u64()?;
        let seen_command_ids = reader.read_command_ids()?;
        let seen_order_ids = reader.read_order_ids()?;

        if reader.remaining_len() != 0 {
            return Err(SnapshotSerializationError::TrailingBytes(
                reader.remaining_len(),
            ));
        }

        Ok(Self {
            order_book_snapshot: OrderBookSnapshot {
                symbol,
                last_input_seq,
                checksum,
                resting_orders,
            },
            next_trade_seq,
            next_market_seq,
            seen_command_ids,
            seen_order_ids,
        })
    }
}

impl OrderBookSnapshot {
    pub fn from_order_book(book: &OrderBook, last_input_seq: JournalSeq) -> Self {
        Self {
            symbol: book.symbol().clone(),
            last_input_seq,
            checksum: book.checksum(),
            resting_orders: book.resting_orders(),
        }
    }

    pub fn restore_order_book(&self) -> OrderBook {
        let mut book = OrderBook::new(self.symbol.clone());

        for order in self.resting_orders.clone() {
            book.insert(order);
        }

        book
    }
}

struct SnapshotWriter {
    bytes: Vec<u8>,
}

impl SnapshotWriter {
    fn new() -> Self {
        Self { bytes: Vec::new() }
    }

    fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }

    fn write_bytes(&mut self, value: &[u8]) {
        self.bytes.extend_from_slice(value);
    }

    fn write_u8(&mut self, value: u8) {
        self.bytes.push(value);
    }

    fn write_u32(&mut self, value: u32) {
        self.bytes.extend_from_slice(&value.to_le_bytes());
    }

    fn write_u64(&mut self, value: u64) {
        self.bytes.extend_from_slice(&value.to_le_bytes());
    }

    fn write_string(&mut self, value: &str) {
        self.write_u32(u32::try_from(value.len()).expect("snapshot string must fit in u32"));
        self.write_bytes(value.as_bytes());
    }

    fn write_orders(&mut self, orders: &[Order]) {
        self.write_u32(u32::try_from(orders.len()).expect("snapshot orders must fit in u32"));

        for order in orders {
            self.write_u64(order.order_id.0);
            self.write_string(&order.symbol.0);
            self.write_side(order.side);
            self.write_u64(order.price.0);
            self.write_u64(order.quantity.0);
        }
    }

    fn write_command_ids(&mut self, command_ids: &[CommandId]) {
        self.write_u32(
            u32::try_from(command_ids.len()).expect("snapshot command ids must fit in u32"),
        );

        for command_id in command_ids {
            self.write_u64(command_id.0);
        }
    }

    fn write_order_ids(&mut self, order_ids: &[OrderId]) {
        self.write_u32(u32::try_from(order_ids.len()).expect("snapshot order ids must fit in u32"));

        for order_id in order_ids {
            self.write_u64(order_id.0);
        }
    }

    fn write_side(&mut self, side: Side) {
        match side {
            Side::Buy => self.write_u8(1),
            Side::Sell => self.write_u8(2),
        }
    }
}

struct SnapshotReader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> SnapshotReader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn remaining_len(&self) -> usize {
        self.bytes.len() - self.offset
    }

    fn read_bytes(&mut self, len: usize) -> Result<&'a [u8], SnapshotSerializationError> {
        let end = self
            .offset
            .checked_add(len)
            .ok_or(SnapshotSerializationError::UnexpectedEof)?;

        if end > self.bytes.len() {
            return Err(SnapshotSerializationError::UnexpectedEof);
        }

        let result = &self.bytes[self.offset..end];
        self.offset = end;
        Ok(result)
    }

    fn read_u8(&mut self) -> Result<u8, SnapshotSerializationError> {
        Ok(self.read_bytes(1)?[0])
    }

    fn read_u32(&mut self) -> Result<u32, SnapshotSerializationError> {
        let mut bytes = [0_u8; 4];
        bytes.copy_from_slice(self.read_bytes(4)?);
        Ok(u32::from_le_bytes(bytes))
    }

    fn read_u64(&mut self) -> Result<u64, SnapshotSerializationError> {
        let mut bytes = [0_u8; 8];
        bytes.copy_from_slice(self.read_bytes(8)?);
        Ok(u64::from_le_bytes(bytes))
    }

    fn read_string(&mut self) -> Result<String, SnapshotSerializationError> {
        let len = self.read_u32()? as usize;
        let bytes = self.read_bytes(len)?;

        String::from_utf8(bytes.to_vec()).map_err(|_| SnapshotSerializationError::InvalidUtf8)
    }

    fn read_orders(&mut self) -> Result<Vec<Order>, SnapshotSerializationError> {
        let count = self.read_u32()? as usize;
        let mut orders = Vec::with_capacity(count);

        for _ in 0..count {
            orders.push(Order {
                order_id: OrderId(self.read_u64()?),
                symbol: Symbol(self.read_string()?),
                side: self.read_side()?,
                price: Price(self.read_u64()?),
                quantity: Quantity(self.read_u64()?),
            });
        }

        Ok(orders)
    }

    fn read_command_ids(&mut self) -> Result<Vec<CommandId>, SnapshotSerializationError> {
        let count = self.read_u32()? as usize;
        let mut command_ids = Vec::with_capacity(count);

        for _ in 0..count {
            command_ids.push(CommandId(self.read_u64()?));
        }

        Ok(command_ids)
    }

    fn read_order_ids(&mut self) -> Result<Vec<OrderId>, SnapshotSerializationError> {
        let count = self.read_u32()? as usize;
        let mut order_ids = Vec::with_capacity(count);

        for _ in 0..count {
            order_ids.push(OrderId(self.read_u64()?));
        }

        Ok(order_ids)
    }

    fn read_side(&mut self) -> Result<Side, SnapshotSerializationError> {
        match self.read_u8()? {
            1 => Ok(Side::Buy),
            2 => Ok(Side::Sell),
            value => Err(SnapshotSerializationError::InvalidSide(value)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::journal_adapter::{JournalInputEntry, JournalInputReader};
    use crate::order::Command;
    use crate::order::Order;
    use crate::order_book::OrderBook;
    use crate::replay_runner::ReplayRunner;
    use crate::types::CommandId;

    #[test]
    fn order_book_snapshot_captures_symbol_sequence_and_checksum() {
        let snapshot = OrderBookSnapshot {
            symbol: Symbol("BTC-USDT".to_string()),
            last_input_seq: JournalSeq(10),
            checksum: Checksum(123),
            resting_orders: Vec::new(),
        };

        assert_eq!(snapshot.symbol, Symbol("BTC-USDT".to_string()));
        assert_eq!(snapshot.last_input_seq, JournalSeq(10));
        assert_eq!(snapshot.checksum, Checksum(123));
    }

    #[test]
    fn order_book_snapshot_captures_resting_orders() {
        let order = Order {
            order_id: OrderId(1),
            symbol: Symbol("BTC-USDT".to_string()),
            side: Side::Buy,
            price: Price(100),
            quantity: Quantity(5),
        };

        let snapshot = OrderBookSnapshot {
            symbol: Symbol("BTC-USDT".to_string()),
            last_input_seq: JournalSeq(10),
            checksum: Checksum(123),
            resting_orders: vec![order.clone()],
        };

        assert_eq!(snapshot.resting_orders, vec![order]);
    }

    #[test]
    fn snapshot_can_be_created_from_order_book() {
        let symbol = Symbol("BTC-USDT".to_string());
        let mut book = OrderBook::new(symbol.clone());

        let order = Order {
            order_id: OrderId(1),
            symbol: symbol.clone(),
            side: Side::Buy,
            price: Price(100),
            quantity: Quantity(5),
        };

        book.insert(order.clone());

        let snapshot = OrderBookSnapshot::from_order_book(&book, JournalSeq(10));

        assert_eq!(snapshot.symbol, symbol);
        assert_eq!(snapshot.last_input_seq, JournalSeq(10));
        assert_eq!(snapshot.checksum, book.checksum());
        assert_eq!(snapshot.resting_orders, vec![order]);
    }

    #[test]
    fn snapshot_can_restore_order_book_with_same_checksum() {
        let symbol = Symbol("BTC-USDT".to_string());
        let mut original = OrderBook::new(symbol.clone());

        original.insert(Order {
            order_id: OrderId(1),
            symbol: symbol.clone(),
            side: Side::Buy,
            price: Price(100),
            quantity: Quantity(5),
        });

        original.insert(Order {
            order_id: OrderId(2),
            symbol: symbol.clone(),
            side: Side::Sell,
            price: Price(105),
            quantity: Quantity(3),
        });

        let snapshot = OrderBookSnapshot::from_order_book(&original, JournalSeq(10));

        let restored = snapshot.restore_order_book();

        assert_eq!(restored.symbol(), &symbol);
        assert_eq!(restored.checksum(), snapshot.checksum);
        assert_eq!(restored.checksum(), original.checksum());
        assert_eq!(restored.resting_orders(), snapshot.resting_orders);
    }

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

    #[test]
    fn snapshot_restore_then_replay_from_next_sequence_matches_full_replay() {
        let symbol = Symbol("BTC-USDT".to_string());
        let mut journal = InMemoryJournalInputReader::new();

        journal.append(
            CommandId(1),
            Command::PlaceLimit(Order {
                order_id: OrderId(1),
                symbol: symbol.clone(),
                side: Side::Buy,
                price: Price(100),
                quantity: Quantity(5),
            }),
        );

        journal.append(
            CommandId(2),
            Command::PlaceLimit(Order {
                order_id: OrderId(2),
                symbol: symbol.clone(),
                side: Side::Sell,
                price: Price(105),
                quantity: Quantity(3),
            }),
        );

        let full_checksum = ReplayRunner::new(symbol.clone()).replay(&journal);

        let mut partial_book = OrderBook::new(symbol.clone());
        partial_book.insert(Order {
            order_id: OrderId(1),
            symbol: symbol.clone(),
            side: Side::Buy,
            price: Price(100),
            quantity: Quantity(5),
        });

        let snapshot = OrderBookSnapshot::from_order_book(&partial_book, JournalSeq(1));
        let restored = snapshot.restore_order_book();

        let resumed_checksum = ReplayRunner::new(symbol.clone()).replay_from_order_book(
            restored,
            &journal,
            JournalSeq(snapshot.last_input_seq.0 + 1),
        );

        assert_eq!(resumed_checksum, full_checksum);
    }
}
