use matching_core::engine::gateway::{CommandGateway, GatewayResult};
use matching_core::engine::matching::MatchingEngine;
use matching_core::engine::result::{MatchResult, OrderAck};
use matching_core::journal::traits::{AppendResult, InputJournal, OutputJournal};
use matching_core::types::*;
use std::sync::Arc;

#[derive(Debug, PartialEq)]
pub enum RuntimeTickResult {
    Idle,
    BackoffAndRetry { retry_from: JournalSeq },
    Fatal { reason: String },
}

pub struct SymbolRuntime {
    pub symbol: Symbol,
    engine: MatchingEngine,
    gateway: CommandGateway,
    input_journal: Arc<dyn InputJournal>,
    output_journal: Arc<dyn OutputJournal>,
    pub last_input_seq: JournalSeq,
}

impl SymbolRuntime {
    pub fn new(
        symbol: Symbol,
        config: SymbolConfig,
        input_journal: Arc<dyn InputJournal>,
        output_journal: Arc<dyn OutputJournal>,
        start_seq: JournalSeq,
    ) -> Self {
        SymbolRuntime {
            engine: MatchingEngine::new(symbol.clone()),
            gateway: CommandGateway::new(symbol.clone(), config),
            input_journal,
            output_journal,
            last_input_seq: start_seq,
            symbol,
        }
    }

    pub fn run_once(&mut self) -> RuntimeTickResult {
        let entries = self
            .input_journal
            .read_from(&self.symbol, self.last_input_seq.next());

        for entry in entries {
            let result = match self.gateway.validate(entry.command, entry.seq) {
                GatewayResult::Accept(command) => self.engine.process(command, entry.seq),
                GatewayResult::Reject { command_id, reason } => MatchResult {
                    order_ack: OrderAck::Rejected {
                        order_id: None,
                        command_id,
                        reason,
                        journal_seq: entry.seq,
                    },
                    trades: Vec::new(),
                    market_event: None,
                },
                GatewayResult::Duplicate { .. } => {
                    self.last_input_seq = entry.seq;
                    continue;
                }
            };

            match self.output_journal.append_output(
                entry.command_id,
                &result.order_ack,
                &result.trades,
                &result.market_event,
            ) {
                AppendResult::Accepted { .. } | AppendResult::DuplicateAccepted { .. } => {
                    self.last_input_seq = entry.seq;
                }
                AppendResult::Unavailable => {
                    return RuntimeTickResult::BackoffAndRetry {
                        retry_from: entry.seq,
                    };
                }
                AppendResult::Rejected { reason } => {
                    return RuntimeTickResult::Fatal { reason };
                }
            }
        }

        RuntimeTickResult::Idle
    }

    #[allow(dead_code)]
    pub fn order_book_checksum(&self) -> u64 {
        self.engine.order_book().checksum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use matching_core::engine::result::{MarketDataEvent, OrderAck, TradeEvent};
    use matching_core::journal::traits::{
        AppendResult, InputJournal, JournalInputEntry, OutputJournal,
    };
    use rust_decimal_macros::dec;
    use std::sync::{Arc, Mutex};

    #[derive(Default)]
    struct InputStub {
        entries: Mutex<Vec<JournalInputEntry>>,
    }

    impl InputStub {
        fn append(&self, command: MatchingCommand) {
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
        }
    }

    impl InputJournal for InputStub {
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

    #[derive(Default)]
    struct OutputStub {
        unavailable: bool,
    }

    impl OutputJournal for OutputStub {
        fn append_output(
            &self,
            _command_id: CommandId,
            _ack: &OrderAck,
            _trades: &[TradeEvent],
            _market: &Option<MarketDataEvent>,
        ) -> AppendResult {
            if self.unavailable {
                AppendResult::Unavailable
            } else {
                AppendResult::Accepted { seq: JournalSeq(1) }
            }
        }

        fn read_output_at(&self, _seq: JournalSeq) -> Option<(OrderAck, Vec<TradeEvent>)> {
            None
        }
    }

    fn config() -> SymbolConfig {
        SymbolConfig {
            price_tick: dec!(0.01),
            quantity_tick: dec!(0.001),
            min_quantity: dec!(0.001),
            config_version: ConfigVersion(1),
        }
    }

    fn command() -> MatchingCommand {
        MatchingCommand::PlaceOrder(OrderCommand {
            command_id: CommandId(1),
            order_id: OrderId(1),
            symbol: Symbol("BTCUSDT".into()),
            side: Side::Bid,
            order_type: OrderType::Limit,
            price: dec!(100),
            quantity: dec!(1),
            config_version: ConfigVersion(1),
            timestamp_ns: 0,
        })
    }

    #[test]
    fn run_once_processes_input_and_advances_seq() {
        let input = Arc::new(InputStub::default());
        let output = Arc::new(OutputStub::default());
        input.append(command());
        let symbol = Symbol("BTCUSDT".into());
        let mut runtime =
            SymbolRuntime::new(symbol, config(), input, output, JournalSeq(0));

        let result = runtime.run_once();

        assert_eq!(result, RuntimeTickResult::Idle);
        assert_eq!(runtime.last_input_seq, JournalSeq(1));
    }

    #[test]
    fn seq_does_not_advance_when_output_unavailable() {
        let input = Arc::new(InputStub::default());
        let output = Arc::new(OutputStub { unavailable: true });
        input.append(command());
        let symbol = Symbol("BTCUSDT".into());
        let mut runtime =
            SymbolRuntime::new(symbol, config(), input, output, JournalSeq(0));

        let result = runtime.run_once();

        assert_eq!(
            result,
            RuntimeTickResult::BackoffAndRetry {
                retry_from: JournalSeq(1)
            }
        );
        assert_eq!(runtime.last_input_seq, JournalSeq(0));
    }
}
