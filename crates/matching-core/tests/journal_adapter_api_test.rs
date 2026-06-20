use matching_core::journal_adapter::{
    JournalAdapterError, JournalOutputAppender, JournalOutputEntry,
};
use matching_core::journal_adapter::{JournalInputEntry, JournalInputReader};
use matching_core::matching_engine::{EngineEvent, OrderAck};
use matching_core::order::{Command, Order};
use matching_core::types::{CommandId, JournalSeq, OrderId, Price, Quantity, Side, Symbol};

struct TestJournal {
    entries: Vec<JournalInputEntry>,
}

impl TestJournal {
    fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }
}

impl JournalInputReader for TestJournal {
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

fn limit_command(order_id: u64) -> Command {
    Command::PlaceLimit(Order {
        order_id: OrderId(order_id),
        symbol: symbol(),
        side: Side::Buy,
        price: Price(100),
        quantity: Quantity(10),
    })
}

struct TestJournalOutputAppender {
    entries: Vec<JournalOutputEntry>,
}

impl TestJournalOutputAppender {
    fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }
}

impl JournalOutputAppender for TestJournalOutputAppender {
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
        });

        Ok(())
    }

    fn read_all(&self) -> Vec<JournalOutputEntry> {
        self.entries.clone()
    }
}

#[test]
fn journal_input_reader_contract_is_available_from_public_api() {
    let mut journal = TestJournal::new();

    let seq = journal.append(CommandId(1), limit_command(1));

    assert_eq!(seq, JournalSeq(1));
    assert_eq!(journal.latest_seq(), Some(JournalSeq(1)));

    let entries = journal.read_from(JournalSeq(1));
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].command_id, CommandId(1));
}

#[test]
fn journal_output_appender_contract_is_available_from_public_api() {
    let mut journal = TestJournalOutputAppender::new();

    let events = vec![EngineEvent::OrderAck(OrderAck::Accepted {
        command_id: CommandId(1),
        order_id: OrderId(1),
        journal_seq: JournalSeq(1),
    })];

    assert_eq!(
        journal.append(CommandId(1), JournalSeq(1), events.clone()),
        Ok(())
    );

    let entries = journal.read_all();

    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].command_id, CommandId(1));
    assert_eq!(entries[0].journal_seq, JournalSeq(1));
    assert_eq!(entries[0].events, events);
}
