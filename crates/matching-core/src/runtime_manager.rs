use crate::journal_adapter::{JournalInputEntry, JournalOutputAppender};
use crate::symbol_runtime::SymbolRuntime;
use crate::types::{JournalSeq, Symbol};
use std::collections::HashMap;

pub struct RuntimeManager {
    runtimes: HashMap<Symbol, SymbolRuntime>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RuntimeManagerError {
    UnknownSymbol,
    OutputAppendFailed,
}

impl RuntimeManager {
    pub fn new() -> Self {
        Self {
            runtimes: HashMap::new(),
        }
    }

    pub fn add_symbol(&mut self, symbol: Symbol) {
        self.runtimes
            .entry(symbol.clone())
            .or_insert_with(|| SymbolRuntime::new(symbol));
    }

    pub fn last_input_seq(&self, symbol: &Symbol) -> Option<Option<JournalSeq>> {
        self.runtimes
            .get(symbol)
            .map(|runtime| runtime.last_input_seq())
    }

    pub fn process_batch(
        &mut self,
        entries: Vec<JournalInputEntry>,
        output: &mut dyn JournalOutputAppender,
    ) -> Result<usize, RuntimeManagerError> {
        let mut processed = 0;

        for entry in entries {
            self.process_entry(entry, output)?;
            processed += 1;
        }

        Ok(processed)
    }

    pub fn process_entry(
        &mut self,
        entry: JournalInputEntry,
        output: &mut dyn JournalOutputAppender,
    ) -> Result<(), RuntimeManagerError> {
        let symbol = entry.command.symbol().clone();
        let runtime = self
            .runtimes
            .get_mut(&symbol)
            .ok_or(RuntimeManagerError::UnknownSymbol)?;

        runtime
            .process_entry(entry, output)
            .map_err(|_| RuntimeManagerError::OutputAppendFailed)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::{EngineEvent, OrderAck};
    use crate::journal_adapter::{
        JournalAdapterError, JournalInputEntry, JournalOutputAppender, JournalOutputEntry,
    };
    use crate::order::{Command, Order};
    use crate::types::{CommandId, JournalSeq, OrderId, Price, Quantity, Side, Symbol};

    fn btc() -> Symbol {
        Symbol("BTC-USDT".to_string())
    }

    fn eth() -> Symbol {
        Symbol("ETH-USDT".to_string())
    }

    #[test]
    fn manager_can_register_multiple_symbol_runtimes() {
        let mut manager = RuntimeManager::new();

        manager.add_symbol(btc());
        manager.add_symbol(eth());

        assert_eq!(manager.last_input_seq(&btc()), Some(None));
        assert_eq!(manager.last_input_seq(&eth()), Some(None));
    }

    #[test]
    fn manager_returns_none_for_unknown_symbol() {
        let manager = RuntimeManager::new();

        assert_eq!(manager.last_input_seq(&btc()), None);
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
            });

            Ok(())
        }

        fn read_all(&self) -> Vec<JournalOutputEntry> {
            self.entries.clone()
        }
    }

    fn input_entry(seq: u64, command_id: u64, order_id: u64, symbol: Symbol) -> JournalInputEntry {
        JournalInputEntry {
            seq: JournalSeq(seq),
            command_id: CommandId(command_id),
            command: Command::PlaceLimit(Order {
                order_id: OrderId(order_id),
                symbol,
                side: Side::Buy,
                price: Price(100),
                quantity: Quantity(5),
            }),
        }
    }

    #[test]
    fn manager_routes_entry_to_matching_symbol_runtime() {
        let mut manager = RuntimeManager::new();
        manager.add_symbol(btc());
        manager.add_symbol(eth());

        let mut output = InMemoryJournalOutputAppender::new();

        assert_eq!(
            manager.process_entry(input_entry(1, 10, 100, btc()), &mut output),
            Ok(())
        );

        assert_eq!(manager.last_input_seq(&btc()), Some(Some(JournalSeq(1))));
        assert_eq!(manager.last_input_seq(&eth()), Some(None));

        let entries = output.read_all();
        assert_eq!(entries.len(), 1);
        assert_eq!(
            entries[0].events,
            vec![EngineEvent::OrderAck(OrderAck::Accepted {
                command_id: CommandId(10),
                order_id: OrderId(100),
                journal_seq: JournalSeq(1),
            })]
        );
    }

    #[test]
    fn manager_returns_error_for_unknown_symbol_entry() {
        let mut manager = RuntimeManager::new();
        manager.add_symbol(btc());

        let mut output = InMemoryJournalOutputAppender::new();

        let result = manager.process_entry(input_entry(1, 10, 100, eth()), &mut output);

        assert_eq!(result, Err(RuntimeManagerError::UnknownSymbol));
        assert_eq!(manager.last_input_seq(&btc()), Some(None));
        assert_eq!(output.read_all(), Vec::new());
    }

    struct FailingJournalOutputAppender;

    impl JournalOutputAppender for FailingJournalOutputAppender {
        fn append(
            &mut self,
            _command_id: CommandId,
            _journal_seq: JournalSeq,
            _events: Vec<EngineEvent>,
        ) -> Result<(), JournalAdapterError> {
            Err(JournalAdapterError::AppendFailed)
        }

        fn read_all(&self) -> Vec<JournalOutputEntry> {
            Vec::new()
        }
    }

    #[test]
    fn manager_maps_output_append_failure_and_does_not_advance_runtime() {
        let mut manager = RuntimeManager::new();
        manager.add_symbol(btc());

        let mut output = FailingJournalOutputAppender;

        let result = manager.process_entry(input_entry(1, 10, 100, btc()), &mut output);

        assert_eq!(result, Err(RuntimeManagerError::OutputAppendFailed));
        assert_eq!(manager.last_input_seq(&btc()), Some(None));
    }

    #[test]
    fn manager_processes_batch_across_multiple_symbols() {
        let mut manager = RuntimeManager::new();
        manager.add_symbol(btc());
        manager.add_symbol(eth());

        let mut output = InMemoryJournalOutputAppender::new();

        let entries = vec![
            input_entry(1, 10, 100, btc()),
            input_entry(2, 11, 200, eth()),
            input_entry(3, 12, 101, btc()),
        ];

        assert_eq!(manager.process_batch(entries, &mut output), Ok(3));

        assert_eq!(manager.last_input_seq(&btc()), Some(Some(JournalSeq(3))));
        assert_eq!(manager.last_input_seq(&eth()), Some(Some(JournalSeq(2))));

        let output_entries = output.read_all();
        assert_eq!(output_entries.len(), 3);
        assert_eq!(output_entries[0].journal_seq, JournalSeq(1));
        assert_eq!(output_entries[1].journal_seq, JournalSeq(2));
        assert_eq!(output_entries[2].journal_seq, JournalSeq(3));
    }

    #[test]
    fn manager_batch_stops_at_unknown_symbol_and_does_not_process_later_entries() {
        let mut manager = RuntimeManager::new();
        manager.add_symbol(btc());

        let mut output = InMemoryJournalOutputAppender::new();

        let entries = vec![
            input_entry(1, 10, 100, btc()),
            input_entry(2, 11, 200, eth()),
            input_entry(3, 12, 101, btc()),
        ];

        assert_eq!(
            manager.process_batch(entries, &mut output),
            Err(RuntimeManagerError::UnknownSymbol)
        );

        assert_eq!(manager.last_input_seq(&btc()), Some(Some(JournalSeq(1))));

        let output_entries = output.read_all();
        assert_eq!(output_entries.len(), 1);
        assert_eq!(output_entries[0].journal_seq, JournalSeq(1));
    }

    struct FailOnSecondAppendJournalOutputAppender {
        entries: Vec<JournalOutputEntry>,
        append_count: usize,
    }

    impl FailOnSecondAppendJournalOutputAppender {
        fn new() -> Self {
            Self {
                entries: Vec::new(),
                append_count: 0,
            }
        }
    }

    impl JournalOutputAppender for FailOnSecondAppendJournalOutputAppender {
        fn append(
            &mut self,
            command_id: CommandId,
            journal_seq: JournalSeq,
            events: Vec<EngineEvent>,
        ) -> Result<(), JournalAdapterError> {
            self.append_count += 1;

            if self.append_count == 2 {
                return Err(JournalAdapterError::AppendFailed);
            }

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
    fn manager_batch_stops_at_output_append_failure_and_does_not_process_later_entries() {
        let mut manager = RuntimeManager::new();
        manager.add_symbol(btc());
        manager.add_symbol(eth());

        let mut output = FailOnSecondAppendJournalOutputAppender::new();

        let entries = vec![
            input_entry(1, 10, 100, btc()),
            input_entry(2, 11, 200, eth()),
            input_entry(3, 12, 101, btc()),
        ];

        assert_eq!(
            manager.process_batch(entries, &mut output),
            Err(RuntimeManagerError::OutputAppendFailed)
        );

        assert_eq!(manager.last_input_seq(&btc()), Some(Some(JournalSeq(1))));
        assert_eq!(manager.last_input_seq(&eth()), Some(None));

        let output_entries = output.read_all();
        assert_eq!(output_entries.len(), 1);
        assert_eq!(output_entries[0].journal_seq, JournalSeq(1));
    }
}
