use crate::engine::EngineEvent;
use crate::order::Command;
use crate::types::{CommandId, JournalSeq};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InputJournalEntry {
    pub seq: JournalSeq,
    pub command_id: CommandId,
    pub command: Command,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutputJournalEntry {
    pub command_id: CommandId,
    pub journal_seq: JournalSeq,
    pub events: Vec<EngineEvent>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OutputJournalError {
    AppendFailed,
}

pub trait InputJournal {
    fn append(&mut self, command_id: CommandId, command: Command) -> JournalSeq;
    fn read_from(&self, from: JournalSeq) -> Vec<InputJournalEntry>;
    fn latest_seq(&self) -> Option<JournalSeq>;
}

pub trait OutputJournal {
    fn append(
        &mut self,
        command_id: CommandId,
        journal_seq: JournalSeq,
        events: Vec<EngineEvent>,
    ) -> Result<(), OutputJournalError>;

    fn read_all(&self) -> Vec<OutputJournalEntry>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::OrderAck;
    use crate::order::{Command, Order};
    use crate::types::{CommandId, JournalSeq, OrderId, Price, Quantity, Side, Symbol};

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

    #[test]
    fn append_assigns_increasing_sequence_and_read_from_returns_ordered_entries() {
        let mut journal = InMemoryInputJournal::new();

        let first_seq = journal.append(CommandId(1), limit_command(1));
        let second_seq = journal.append(CommandId(2), limit_command(2));

        assert_eq!(first_seq, JournalSeq(1));
        assert_eq!(second_seq, JournalSeq(2));

        let all = journal.read_from(JournalSeq(1));
        assert_eq!(all.len(), 2);
        assert_eq!(all[0].seq, JournalSeq(1));
        assert_eq!(all[0].command_id, CommandId(1));
        assert_eq!(all[1].seq, JournalSeq(2));
        assert_eq!(all[1].command_id, CommandId(2));

        let from_second = journal.read_from(JournalSeq(2));
        assert_eq!(from_second.len(), 1);
        assert_eq!(from_second[0].seq, JournalSeq(2));
        assert_eq!(from_second[0].command_id, CommandId(2));
    }

    struct InMemoryInputJournal {
        entries: Vec<InputJournalEntry>,
    }

    impl InMemoryInputJournal {
        fn new() -> Self {
            Self {
                entries: Vec::new(),
            }
        }
    }

    impl InputJournal for InMemoryInputJournal {
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

    #[test]
    fn read_from_future_sequence_returns_empty_entries() {
        let mut journal = InMemoryInputJournal::new();

        journal.append(CommandId(1), limit_command(1));

        let entries = journal.read_from(JournalSeq(99));

        assert!(entries.is_empty());
    }

    #[test]
    fn latest_seq_tracks_most_recent_appended_entry() {
        let mut journal = InMemoryInputJournal::new();

        assert_eq!(journal.latest_seq(), None);

        journal.append(CommandId(1), limit_command(1));
        assert_eq!(journal.latest_seq(), Some(JournalSeq(1)));

        journal.append(CommandId(2), limit_command(2));
        assert_eq!(journal.latest_seq(), Some(JournalSeq(2)));
    }

    #[test]
    fn output_journal_appends_and_reads_output_entries_in_order() {
        let mut journal = InMemoryOutputJournal::new();

        let first_events = vec![EngineEvent::OrderAck(OrderAck::Accepted {
            command_id: CommandId(1),
            order_id: OrderId(1),
            journal_seq: JournalSeq(1),
        })];

        let second_events = vec![EngineEvent::OrderAck(OrderAck::Cancelled {
            command_id: CommandId(2),
            order_id: OrderId(1),
            journal_seq: JournalSeq(2),
        })];

        assert_eq!(
            journal.append(CommandId(1), JournalSeq(1), first_events.clone()),
            Ok(())
        );
        assert_eq!(
            journal.append(CommandId(2), JournalSeq(2), second_events.clone()),
            Ok(())
        );

        let entries = journal.read_all();

        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].command_id, CommandId(1));
        assert_eq!(entries[0].journal_seq, JournalSeq(1));
        assert_eq!(entries[0].events, first_events);
        assert_eq!(entries[1].command_id, CommandId(2));
        assert_eq!(entries[1].journal_seq, JournalSeq(2));
        assert_eq!(entries[1].events, second_events);
    }

    struct InMemoryOutputJournal {
        entries: Vec<OutputJournalEntry>,
    }

    impl InMemoryOutputJournal {
        fn new() -> Self {
            Self {
                entries: Vec::new(),
            }
        }
    }

    impl OutputJournal for InMemoryOutputJournal {
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
}
