use crate::engine::result::{MarketDataEvent, OrderAck, TradeEvent};
use crate::types::{CommandId, JournalSeq, MatchingCommand, Symbol};

#[derive(Debug, Clone, PartialEq)]
pub enum AppendResult {
    Accepted { seq: JournalSeq },
    DuplicateAccepted { seq: JournalSeq },
    Rejected { reason: String },
    Unavailable,
}

#[derive(Debug, Clone)]
pub struct JournalInputEntry {
    pub seq: JournalSeq,
    pub command_id: CommandId,
    pub command: MatchingCommand,
}

pub trait InputJournal: Send + Sync {
    fn read_from(&self, symbol: &Symbol, start_seq: JournalSeq) -> Vec<JournalInputEntry>;
    fn latest_confirmed_seq(&self, symbol: &Symbol) -> Option<JournalSeq>;
}

pub trait OutputJournal: Send + Sync {
    fn append_output(
        &self,
        command_id: CommandId,
        ack: &OrderAck,
        trades: &[TradeEvent],
        market: &Option<MarketDataEvent>,
    ) -> AppendResult;

    fn read_output_at(&self, seq: JournalSeq) -> Option<(OrderAck, Vec<TradeEvent>)>;
}
