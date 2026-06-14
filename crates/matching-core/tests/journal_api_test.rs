use matching_core::engine::{EngineEvent, OrderAck};
use matching_core::journal::{InputJournal, InputJournalEntry};
use matching_core::journal::{OutputJournal, OutputJournalEntry, OutputJournalError};
use matching_core::order::{Command, Order};
use matching_core::types::{CommandId, JournalSeq, OrderId, Price, Quantity, Side, Symbol};

struct TestJournal {
    entries: Vec<InputJournalEntry>,
}

impl TestJournal {
    fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }
}

impl InputJournal for TestJournal {
    fn append(&mut self, command_id: CommandId, command: Command) -> JournalSeq {
        let seq = JournalSeq(self.entries.len() as u64 + 1);

        self.entries.push(InputJournalEntry {
            seq,
            command_id,
            command,
        });

        seq
    }

    fn read_from(&self, from: JournalSeq) -> Vec<InputJournalEntry> {
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

struct TestOutputJournal {
    entries: Vec<OutputJournalEntry>,
}

impl TestOutputJournal {
    fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }
}

impl OutputJournal for TestOutputJournal {
    fn append(
        &mut self,
        command_id: CommandId,
        journal_seq: JournalSeq,
        events: Vec<EngineEvent>,
    ) -> Result<(), OutputJournalError> {
        self.entries.push(OutputJournalEntry {
            command_id,
            journal_seq,
            events,
        });

        Ok(())
    }

    fn read_all(&self) -> Vec<OutputJournalEntry> {
        self.entries.clone()
    }
}

#[test]
fn input_journal_contract_is_available_from_public_api() {
    let mut journal = TestJournal::new();

    let seq = journal.append(CommandId(1), limit_command(1));

    assert_eq!(seq, JournalSeq(1));
    assert_eq!(journal.latest_seq(), Some(JournalSeq(1)));

    let entries = journal.read_from(JournalSeq(1));
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].command_id, CommandId(1));
}

#[test]
fn output_journal_contract_is_available_from_public_api() {
    let mut journal = TestOutputJournal::new();

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
