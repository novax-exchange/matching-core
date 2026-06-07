//! In-memory Journal stubs for tests only. They are not production Journal boundaries.

use matching_core::engine::result::{MarketDataEvent, OrderAck, TradeEvent};
use matching_core::journal::traits::{
    AppendResult, InputJournal, JournalInputEntry, OutputJournal,
};
use matching_core::types::{CommandId, JournalSeq, MatchingCommand, Symbol};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

#[derive(Default, Clone)]
pub struct InMemoryInputJournal {
    entries: Arc<Mutex<Vec<JournalInputEntry>>>,
}

impl InMemoryInputJournal {
    pub fn append(&self, _symbol: Symbol, command: MatchingCommand) -> JournalSeq {
        let mut entries = self.entries.lock().unwrap();
        let seq = JournalSeq(entries.len() as u64 + 1);
        let command_id = match &command {
            MatchingCommand::PlaceOrder(command) => command.command_id.clone(),
            MatchingCommand::CancelOrder(command) => command.command_id.clone(),
        };
        entries.push(JournalInputEntry {
            seq,
            command_id,
            command,
        });
        seq
    }
}

impl InputJournal for InMemoryInputJournal {
    fn read_from(&self, _symbol: &Symbol, start_seq: JournalSeq) -> Vec<JournalInputEntry> {
        self.entries
            .lock()
            .unwrap()
            .iter()
            .filter(|entry| entry.seq >= start_seq)
            .cloned()
            .collect()
    }

    fn latest_confirmed_seq(&self, _symbol: &Symbol) -> Option<JournalSeq> {
        self.entries.lock().unwrap().last().map(|entry| entry.seq)
    }
}

#[derive(Default, Clone)]
pub struct InMemoryOutputJournal {
    outputs: Arc<Mutex<HashMap<JournalSeq, (OrderAck, Vec<TradeEvent>)>>>,
    command_index: Arc<Mutex<HashMap<CommandId, JournalSeq>>>,
    next_seq: Arc<Mutex<u64>>,
}

impl OutputJournal for InMemoryOutputJournal {
    fn append_output(
        &self,
        command_id: CommandId,
        ack: &OrderAck,
        trades: &[TradeEvent],
        _market: &Option<MarketDataEvent>,
    ) -> AppendResult {
        if let Some(seq) = self.command_index.lock().unwrap().get(&command_id).copied() {
            return AppendResult::DuplicateAccepted { seq };
        }

        let mut next = self.next_seq.lock().unwrap();
        *next += 1;
        let seq = JournalSeq(*next);
        self.command_index.lock().unwrap().insert(command_id, seq);
        self.outputs
            .lock()
            .unwrap()
            .insert(seq, (ack.clone(), trades.to_vec()));
        AppendResult::Accepted { seq }
    }

    fn read_output_at(&self, seq: JournalSeq) -> Option<(OrderAck, Vec<TradeEvent>)> {
        self.outputs.lock().unwrap().get(&seq).cloned()
    }
}
