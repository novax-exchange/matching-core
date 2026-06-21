//! Output Commit Boundary: stable output batch identity.
//!
//! Current scope: create deterministic identity metadata for a batch of
//! output requests. The identity is not yet written through the Journal
//! adapter; it gives the commit path a stable value to expose and test first.

use super::output_journal_client::OutputCommitRequest;
use crate::journal_adapter::JournalOutputEntry;
use crate::matching_engine::{EngineEvent, MarketEvent, OrderAck, RejectReason};
use crate::types::{JournalSeq, Side, Symbol};

pub const MATCHING_OUTPUT_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutputBatchId(pub String);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OutputDigest(pub u64);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutputBatchIdentity {
    pub batch_id: OutputBatchId,
    pub symbol: Symbol,
    pub input_seq_start: JournalSeq,
    pub input_seq_end: JournalSeq,
    pub entry_count: usize,
    pub matching_version: u32,
    pub output_digest: OutputDigest,
}

pub fn build_output_batch_identity(
    symbol: &Symbol,
    matching_version: u32,
    requests: &[OutputCommitRequest],
) -> Option<OutputBatchIdentity> {
    let first = requests.first()?;
    let last = requests.last()?;
    let output_digest = digest_output_requests(requests);
    let batch_id = OutputBatchId(format!(
        "{}:{}-{}:{}:v{}",
        symbol.0,
        first.journal_seq.0,
        last.journal_seq.0,
        requests.len(),
        matching_version
    ));

    Some(OutputBatchIdentity {
        batch_id,
        symbol: symbol.clone(),
        input_seq_start: first.journal_seq,
        input_seq_end: last.journal_seq,
        entry_count: requests.len(),
        matching_version,
        output_digest,
    })
}

fn digest_output_requests(requests: &[OutputCommitRequest]) -> OutputDigest {
    let mut hasher = StableDigest::new();

    for request in requests {
        hasher.write_tag("request");
        hasher.write_u64(request.command_id.0);
        hasher.write_u64(request.journal_seq.0);
        hasher.write_usize(request.events.len());

        for event in &request.events {
            digest_engine_event(&mut hasher, event);
        }
    }

    OutputDigest(hasher.finish())
}

pub fn digest_journal_output_entries(entries: &[JournalOutputEntry]) -> OutputDigest {
    let mut hasher = StableDigest::new();

    for entry in entries {
        hasher.write_tag("journal_output_entry");
        hasher.write_u64(entry.command_id.0);
        hasher.write_u64(entry.journal_seq.0);
        hasher.write_usize(entry.events.len());

        for event in &entry.events {
            digest_engine_event(&mut hasher, event);
        }
    }

    OutputDigest(hasher.finish())
}

fn digest_engine_event(hasher: &mut StableDigest, event: &EngineEvent) {
    match event {
        EngineEvent::OrderAck(ack) => digest_order_ack(hasher, ack),
        EngineEvent::Trade(trade) => digest_trade_event(hasher, "trade", trade),
        EngineEvent::Market(market_event) => digest_market_event(hasher, market_event),
    }
}

fn digest_market_event(hasher: &mut StableDigest, event: &MarketEvent) {
    match event {
        MarketEvent::OrderAdded(added) => {
            hasher.write_tag("market.order_added");
            hasher.write_u64(added.market_seq.0);
            hasher.write_u64(added.command_id.0);
            hasher.write_u64(added.journal_seq.0);
            hasher.write_u64(added.order_id.0);
            hasher.write_u64(side_code(added.side));
            hasher.write_u64(added.price.0);
            hasher.write_u64(added.quantity.0);
        }
        MarketEvent::OrderCancelled(cancelled) => {
            hasher.write_tag("market.order_cancelled");
            hasher.write_u64(cancelled.market_seq.0);
            hasher.write_u64(cancelled.command_id.0);
            hasher.write_u64(cancelled.journal_seq.0);
            hasher.write_u64(cancelled.order_id.0);
            hasher.write_u64(side_code(cancelled.side));
            hasher.write_u64(cancelled.price.0);
            hasher.write_u64(cancelled.quantity.0);
        }
        MarketEvent::PriceLevelChanged(changed) => {
            hasher.write_tag("market.price_level_changed");
            hasher.write_u64(changed.market_seq.0);
            hasher.write_u64(changed.command_id.0);
            hasher.write_u64(changed.journal_seq.0);
            hasher.write_u64(side_code(changed.side));
            hasher.write_u64(changed.price.0);
            hasher.write_u64(changed.quantity_after.0);
        }
    }
}

fn digest_trade_event(
    hasher: &mut StableDigest,
    tag: &str,
    trade: &crate::matching_engine::TradeEvent,
) {
    hasher.write_tag(tag);
    hasher.write_u64(trade.trade_id.0);
    hasher.write_u64(trade.market_seq.0);
    hasher.write_u64(trade.command_id.0);
    hasher.write_u64(trade.journal_seq.0);
    hasher.write_u64(trade.maker_order_id.0);
    hasher.write_u64(trade.taker_order_id.0);
    hasher.write_u64(trade.price.0);
    hasher.write_u64(trade.quantity.0);
}

fn side_code(side: Side) -> u64 {
    match side {
        Side::Buy => 1,
        Side::Sell => 2,
    }
}

fn digest_order_ack(hasher: &mut StableDigest, ack: &OrderAck) {
    match ack {
        OrderAck::Accepted {
            command_id,
            order_id,
            journal_seq,
        } => {
            hasher.write_tag("ack.accepted");
            hasher.write_u64(command_id.0);
            hasher.write_u64(order_id.0);
            hasher.write_u64(journal_seq.0);
        }
        OrderAck::Rejected {
            command_id,
            order_id,
            journal_seq,
            reason,
        } => {
            hasher.write_tag("ack.rejected");
            hasher.write_u64(command_id.0);
            hasher.write_option_u64(order_id.map(|value| value.0));
            hasher.write_u64(journal_seq.0);
            hasher.write_u64(reject_reason_code(reason));
        }
        OrderAck::Cancelled {
            command_id,
            order_id,
            journal_seq,
        } => {
            hasher.write_tag("ack.cancelled");
            hasher.write_u64(command_id.0);
            hasher.write_u64(order_id.0);
            hasher.write_u64(journal_seq.0);
        }
    }
}

fn reject_reason_code(reason: &RejectReason) -> u64 {
    match reason {
        RejectReason::InvalidPrice => 1,
        RejectReason::InvalidQuantity => 2,
        RejectReason::SymbolMismatch => 3,
        RejectReason::DuplicateCommandId => 4,
        RejectReason::DuplicateOrderId => 5,
        RejectReason::OrderNotFound => 6,
    }
}

struct StableDigest {
    value: u64,
}

impl StableDigest {
    fn new() -> Self {
        Self {
            value: 0xcbf29ce484222325,
        }
    }

    fn write_tag(&mut self, value: &str) {
        self.write_usize(value.len());
        self.write_bytes(value.as_bytes());
    }

    fn write_option_u64(&mut self, value: Option<u64>) {
        match value {
            Some(value) => {
                self.write_u64(1);
                self.write_u64(value);
            }
            None => self.write_u64(0),
        }
    }

    fn write_usize(&mut self, value: usize) {
        self.write_u64(value as u64);
    }

    fn write_u64(&mut self, value: u64) {
        self.write_bytes(&value.to_le_bytes());
    }

    fn write_bytes(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.value ^= u64::from(*byte);
            self.value = self.value.wrapping_mul(0x100000001b3);
        }
    }

    fn finish(self) -> u64 {
        self.value
    }
}
