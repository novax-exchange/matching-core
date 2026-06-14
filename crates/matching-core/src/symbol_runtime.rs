use crate::engine::{EngineEvent, OrderAck, RejectReason, TradeEvent};
use crate::command_ingress::{CommandIngress, IngressError};
use crate::journal::{InputJournalEntry, OutputJournal, OutputJournalError};
use crate::order::Command;
use crate::order_book::OrderBook;
use crate::types::{JournalSeq, Symbol, TradeId};


pub struct SymbolRuntime {
    ingress: CommandIngress,
    order_book: OrderBook,
    last_input_seq: Option<JournalSeq>,
    next_trade_id: u64,
}

fn reject_reason_from_ingress_error(error: IngressError) -> RejectReason {
    match error {
        IngressError::InvalidPrice => RejectReason::InvalidPrice,
        IngressError::InvalidQuantity => RejectReason::InvalidQuantity,
        IngressError::SymbolMismatch => RejectReason::SymbolMismatch,
    }
}

impl SymbolRuntime {
    pub fn new(symbol: Symbol) -> Self {
        Self {
            ingress: CommandIngress::new(symbol.clone()),
            order_book: OrderBook::new(symbol.clone()),
            last_input_seq: None,
            next_trade_id: 1,
        }
    }

    pub fn last_input_seq(&self) -> Option<JournalSeq> {
        self.last_input_seq
    }

    pub fn process_batch(
        &mut self,
        entries: Vec<InputJournalEntry>,
        output: &mut dyn OutputJournal,
    ) -> Result<usize, OutputJournalError> {
        let mut processed = 0; 

        for entry in entries {
            self.process_entry(entry, output)?;
            processed += 1;
        }

        Ok(processed)
    }

    pub fn process_entry(
        &mut self,
        entry: InputJournalEntry,
        output: &mut dyn OutputJournal,
    ) -> Result<(), OutputJournalError> {
        let command = match self.ingress.validate(entry.command) {
            Ok(command) => command, 
            Err(error) => {
                let events = vec![EngineEvent::OrderAck(OrderAck::Rejected { 
                    command_id: entry.command_id, 
                    order_id: None, 
                    journal_seq: entry.seq, 
                    reason: reject_reason_from_ingress_error(error),
                })];

                output.append(entry.command_id, entry.seq, events)?;
                self.last_input_seq = Some(entry.seq);
                return Ok(());
            }
        };
        match command {
            Command::PlaceLimit(order) => {
                let order_id = order.order_id;
                let order_book_before = self.order_book.clone();
                let nex_trade_id_before = self.next_trade_id;

                let result = self.order_book.place_limit(order);

                let mut events = vec![EngineEvent::OrderAck(OrderAck::Accepted { 
                    command_id: entry.command_id, 
                    order_id: order_id, 
                    journal_seq: entry.seq, 
                })];

                for trade in result.trades {
                    let trade_id = TradeId(self.next_trade_id);
                    self.next_trade_id += 1;


                    events.push(EngineEvent::Trade(TradeEvent {
                        trade_id, 
                        command_id: entry.command_id, 
                        journal_seq: entry.seq, 
                        maker_order_id: trade.maker_order_id, 
                        taker_order_id: trade.taker_order_id, 
                        price: trade.price, 
                        quantity: trade.quantity 
                    }));
                }

                if let Err(error) = output.append(entry.command_id, entry.seq, events) {
                    self.order_book = order_book_before;
                    self.next_trade_id = nex_trade_id_before;
                    return Err(error);
                }

                self.last_input_seq = Some(entry.seq);
                Ok(())
            },
            Command::Cancel { order_id, .. } => {
                let order_book_before = self.order_book.clone();

                let events = match self.order_book.cancel(order_id) {
                    Ok(_) => vec![EngineEvent::OrderAck(OrderAck::Cancelled { 
                        command_id: entry.command_id, 
                        order_id, 
                        journal_seq: entry.seq
                    })],
                    Err(_) => vec![EngineEvent::OrderAck(OrderAck::Rejected { 
                        command_id: entry.command_id, 
                        order_id: Some(order_id), 
                        journal_seq: entry.seq, 
                        reason: RejectReason::OrderNotFound 
                    })],
                };

                if let Err(error) = output.append(entry.command_id, entry.seq, events) {
                    self.order_book = order_book_before;
                    return Err(error);
                }

                self.last_input_seq = Some(entry.seq);
                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::journal::{
        InputJournalEntry, OutputJournal, OutputJournalEntry, OutputJournalError,
    };
    use crate::engine::{EngineEvent, OrderAck, TradeEvent};
    use crate::order::{Command, Order};
    use crate::types::{
        CommandId, JournalSeq, OrderId, Price, Quantity, Side, Symbol,
    };

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

    fn symbol() -> Symbol {
        Symbol("BTC-USDT".to_string())
    }

    fn input_entry(seq: u64, command_id: u64, order_id: u64) -> InputJournalEntry {
        InputJournalEntry {
            seq: JournalSeq(seq),
            command_id: CommandId(command_id),
            command: Command::PlaceLimit(Order {
                order_id: OrderId(order_id),
                symbol: symbol(),
                side: Side::Buy,
                price: Price(100),
                quantity: Quantity(5),
            }),
        }
    }

    #[test]
    fn processing_valid_entry_commits_output_and_advances_last_input_seq() {
        let mut runtime = SymbolRuntime::new(symbol());
        let mut output = InMemoryOutputJournal::new();

        assert_eq!(runtime.process_entry(input_entry(1, 10, 100), &mut output), Ok(()));

        assert_eq!(runtime.last_input_seq(), Some(JournalSeq(1)));

        let entries = output.read_all();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].command_id, CommandId(10));
        assert_eq!(entries[0].journal_seq, JournalSeq(1));
        assert_eq!(
            entries[0].events,
            vec![EngineEvent::OrderAck(OrderAck::Accepted {
                command_id: CommandId(10),
                order_id: OrderId(100),
                journal_seq: JournalSeq(1),
            })]
        );
    }

    struct FailingOutputJournal;

    impl OutputJournal for FailingOutputJournal {
        fn append(
            &mut self,
            _command_id: CommandId,
            _journal_seq: JournalSeq,
            _events: Vec<EngineEvent>,
        ) -> Result<(), OutputJournalError> {
            Err(OutputJournalError::AppendFailed)
        }

        fn read_all(&self) -> Vec<OutputJournalEntry> {
            Vec::new()
        }
    }

    #[test]
    fn output_append_failure_does_not_advance_last_input_seq() {
        let mut runtime = SymbolRuntime::new(symbol());
        let mut output = FailingOutputJournal;

        assert_eq!(runtime.process_entry(input_entry(1, 10, 100), &mut output), Err(OutputJournalError::AppendFailed));

        assert_eq!(runtime.last_input_seq(), None);
    }    

    #[test]
    fn processing_multiple_successful_entries_advances_last_input_seq_to_latest_entry() {
        let mut runtime = SymbolRuntime::new(symbol());
        let mut output = InMemoryOutputJournal::new();

        assert_eq!(
            runtime.process_entry(input_entry(1, 10, 100), &mut output), 
            Ok(())
        );
        assert_eq!(
            runtime.process_entry(input_entry(2, 11, 101), &mut output), 
            Ok(())
        );

        assert_eq!(runtime.last_input_seq(), Some(JournalSeq(2)));

        let entries = output.read_all();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].journal_seq, JournalSeq(1));
        assert_eq!(entries[1].journal_seq, JournalSeq(2));
    }

    struct FailOnSecondAppendOutputJournal {
        entries: Vec<OutputJournalEntry>,
        append_count: usize,
    }
    
    impl FailOnSecondAppendOutputJournal {
        fn new() -> Self {
            Self {
                entries: Vec::new(),
                append_count: 0,
            }
        }
    }
    
    impl OutputJournal for FailOnSecondAppendOutputJournal {
        fn append(
            &mut self,
            command_id: CommandId,
            journal_seq: JournalSeq,
            events: Vec<EngineEvent>,
        ) -> Result<(), OutputJournalError> {
            self.append_count += 1;
    
            if self.append_count == 2 {
                return Err(OutputJournalError::AppendFailed);
            }
    
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
    fn failed_second_output_append_keeps_last_input_seq_at_first_entry() {
        let mut runtime = SymbolRuntime::new(symbol());
        let mut output = FailOnSecondAppendOutputJournal::new();

        assert_eq!(
            runtime.process_entry(input_entry(1, 10, 100), &mut output), 
            Ok(()),
        );
        assert_eq!(
            runtime.process_entry(input_entry(2, 11, 101), &mut output), 
            Err(OutputJournalError::AppendFailed)
        );

        assert_eq!(runtime.last_input_seq(), Some(JournalSeq(1)));

        let entries = output.read_all();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].journal_seq, JournalSeq(1));
    }

    #[test]
    fn invalid_price_order_writes_rejected_output_and_advances_last_input_seq() {
        let mut runtime = SymbolRuntime::new(symbol());
        let mut output = InMemoryOutputJournal::new();

        let entry = InputJournalEntry {
            seq: JournalSeq(1),
            command_id: CommandId(10),
            command: Command::PlaceLimit(Order {
                order_id: OrderId(100),
                symbol: symbol(),
                side: Side::Buy,
                price: Price(0),
                quantity: Quantity(5),
            }),
        };

        assert_eq!(runtime.process_entry(entry, &mut output), Ok(()));
        assert_eq!(runtime.last_input_seq(), Some(JournalSeq(1)));

        let entries = output.read_all();
        assert_eq!(entries.len(), 1);
        assert_eq!(
            entries[0].events,
            vec![EngineEvent::OrderAck(OrderAck::Rejected {
                command_id: CommandId(10),
                order_id: None,
                journal_seq: JournalSeq(1),
                reason: RejectReason::InvalidPrice,
            })]
        );
    }

        #[test]
    fn invalid_quantity_order_writes_rejected_output_and_advances_last_input_seq() {
        let mut runtime = SymbolRuntime::new(symbol());
        let mut output = InMemoryOutputJournal::new();

        let entry = InputJournalEntry {
            seq: JournalSeq(1),
            command_id: CommandId(10),
            command: Command::PlaceLimit(Order {
                order_id: OrderId(100),
                symbol: symbol(),
                side: Side::Buy,
                price: Price(100),
                quantity: Quantity(0),
            }),
        };

        assert_eq!(runtime.process_entry(entry, &mut output), Ok(()));
        assert_eq!(runtime.last_input_seq(), Some(JournalSeq(1)));

        let entries = output.read_all();
        assert_eq!(entries.len(), 1);
        assert_eq!(
            entries[0].events,
            vec![EngineEvent::OrderAck(OrderAck::Rejected {
                command_id: CommandId(10),
                order_id: None,
                journal_seq: JournalSeq(1),
                reason: RejectReason::InvalidQuantity,
            })]
        );
    }

    #[test]
    fn command_for_different_symbol_writes_rejected_output_and_advances_last_input_seq() {
        let mut runtime = SymbolRuntime::new(symbol());
        let mut output = InMemoryOutputJournal::new();

        let entry = InputJournalEntry {
            seq: JournalSeq(1),
            command_id: CommandId(10),
            command: Command::PlaceLimit(Order {
                order_id: OrderId(100),
                symbol: Symbol("ETH-USDT".to_string()),
                side: Side::Buy,
                price: Price(100),
                quantity: Quantity(5),
            }),
        };

        assert_eq!(runtime.process_entry(entry, &mut output), Ok(()));
        assert_eq!(runtime.last_input_seq(), Some(JournalSeq(1)));

        let entries = output.read_all();
        assert_eq!(entries.len(), 1);
        assert_eq!(
            entries[0].events,
            vec![EngineEvent::OrderAck(OrderAck::Rejected {
                command_id: CommandId(10),
                order_id: None,
                journal_seq: JournalSeq(1),
                reason: RejectReason::SymbolMismatch,
            })]
        );
    }

    #[test]
    fn cancel_unknown_order_writes_rejected_order_not_found_and_advances_last_input_seq() {
        let mut runtime = SymbolRuntime::new(symbol());
        let mut output = InMemoryOutputJournal::new();

        let entry = InputJournalEntry {
            seq: JournalSeq(1),
            command_id: CommandId(10),
            command: Command::Cancel {
                order_id: OrderId(999),
                symbol: symbol(),
            },
        };

        assert_eq!(runtime.process_entry(entry, &mut output), Ok(()));
        assert_eq!(runtime.last_input_seq(), Some(JournalSeq(1)));

        let entries = output.read_all();
        assert_eq!(entries.len(), 1);
        assert_eq!(
            entries[0].events,
            vec![EngineEvent::OrderAck(OrderAck::Rejected {
                command_id: CommandId(10),
                order_id: Some(OrderId(999)),
                journal_seq: JournalSeq(1),
                reason: RejectReason::OrderNotFound,
            })]
        );
    }

    #[test]
    fn cancel_existing_order_writes_cancelled_output_and_advances_last_input_seq() {
        let mut runtime = SymbolRuntime::new(symbol());
        let mut output = InMemoryOutputJournal::new();

        assert_eq!(
            runtime.process_entry(input_entry(1, 10, 100), &mut output),
            Ok(())
        );

        let cancel_entry = InputJournalEntry {
            seq: JournalSeq(2),
            command_id: CommandId(11),
            command: Command::Cancel {
                order_id: OrderId(100),
                symbol: symbol(),
            },
        };

        assert_eq!(runtime.process_entry(cancel_entry, &mut output), Ok(()));
        assert_eq!(runtime.last_input_seq(), Some(JournalSeq(2)));

        let entries = output.read_all();
        assert_eq!(entries.len(), 2);
        assert_eq!(
            entries[1].events,
            vec![EngineEvent::OrderAck(OrderAck::Cancelled {
                command_id: CommandId(11),
                order_id: OrderId(100),
                journal_seq: JournalSeq(2),
            })]
        );
    }

    #[test]
    fn matching_order_writes_trade_event_to_output_journal() {
        let mut runtime = SymbolRuntime::new(symbol());
        let mut output = InMemoryOutputJournal::new();

        let resting_sell = InputJournalEntry {
            seq: JournalSeq(1),
            command_id: CommandId(10),
            command: Command::PlaceLimit(Order {
                order_id: OrderId(100),
                symbol: symbol(),
                side: Side::Sell,
                price: Price(100),
                quantity: Quantity(3),
            }),
        };

        let crossing_buy = InputJournalEntry {
            seq: JournalSeq(2),
            command_id: CommandId(11),
            command: Command::PlaceLimit(Order {
                order_id: OrderId(101),
                symbol: symbol(),
                side: Side::Buy,
                price: Price(100),
                quantity: Quantity(3),
            }),
        };

        assert_eq!(runtime.process_entry(resting_sell, &mut output), Ok(()));
        assert_eq!(runtime.process_entry(crossing_buy, &mut output), Ok(()));
        assert_eq!(runtime.last_input_seq(), Some(JournalSeq(2)));

        let entries = output.read_all();
        assert_eq!(entries.len(), 2);
        assert_eq!(
            entries[1].events,
            vec![
                EngineEvent::OrderAck(OrderAck::Accepted {
                    command_id: CommandId(11),
                    order_id: OrderId(101),
                    journal_seq: JournalSeq(2),
                }),
                EngineEvent::Trade(TradeEvent {
                    trade_id: TradeId(1),
                    command_id: CommandId(11),
                    journal_seq: JournalSeq(2),
                    maker_order_id: OrderId(100),
                    taker_order_id: OrderId(101),
                    price: Price(100),
                    quantity: Quantity(3),
                }),
            ]
        );
    }

    #[test]
    fn failed_trade_output_append_does_not_consume_trade_id() {
        let mut runtime = SymbolRuntime::new(symbol());

        let mut successful_output = InMemoryOutputJournal::new();

        let resting_sell = InputJournalEntry {
            seq: JournalSeq(1),
            command_id: CommandId(10),
            command: Command::PlaceLimit(Order {
                order_id: OrderId(100),
                symbol: symbol(),
                side: Side::Sell,
                price: Price(100),
                quantity: Quantity(3),
            }),
        };

        assert_eq!(runtime.process_entry(resting_sell, &mut successful_output), Ok(()));

        let crossing_buy = InputJournalEntry {
            seq: JournalSeq(2),
            command_id: CommandId(11),
            command: Command::PlaceLimit(Order {
                order_id: OrderId(101),
                symbol: symbol(),
                side: Side::Buy,
                price: Price(100),
                quantity: Quantity(3),
            }),
        };

        let mut failing_output = FailingOutputJournal;
        assert_eq!(
            runtime.process_entry(crossing_buy.clone(), &mut failing_output),
            Err(OutputJournalError::AppendFailed)
        );
        assert_eq!(runtime.last_input_seq(), Some(JournalSeq(1)));

        let mut retry_output = InMemoryOutputJournal::new();
        assert_eq!(runtime.process_entry(crossing_buy, &mut retry_output), Ok(()));

        let entries = retry_output.read_all();
        assert_eq!(entries.len(), 1);
        assert_eq!(
            entries[0].events,
            vec![
                EngineEvent::OrderAck(OrderAck::Accepted {
                    command_id: CommandId(11),
                    order_id: OrderId(101),
                    journal_seq: JournalSeq(2),
                }),
                EngineEvent::Trade(TradeEvent {
                    trade_id: TradeId(1),
                    command_id: CommandId(11),
                    journal_seq: JournalSeq(2),
                    maker_order_id: OrderId(100),
                    taker_order_id: OrderId(101),
                    price: Price(100),
                    quantity: Quantity(3),
                }),
            ]
        );
    }

    #[test]
    fn failed_cancel_output_append_does_not_remove_order() {
        let mut runtime = SymbolRuntime::new(symbol());

        let mut successful_output = InMemoryOutputJournal::new();
        assert_eq!(
            runtime.process_entry(input_entry(1, 10, 100), &mut successful_output),
            Ok(())
        );

        let cancel_entry = InputJournalEntry {
            seq: JournalSeq(2),
            command_id: CommandId(11),
            command: Command::Cancel {
                order_id: OrderId(100),
                symbol: symbol(),
            },
        };

        let mut failing_output = FailingOutputJournal;
        assert_eq!(
            runtime.process_entry(cancel_entry.clone(), &mut failing_output),
            Err(OutputJournalError::AppendFailed)
        );
        assert_eq!(runtime.last_input_seq(), Some(JournalSeq(1)));

        let mut retry_output = InMemoryOutputJournal::new();
        assert_eq!(runtime.process_entry(cancel_entry, &mut retry_output), Ok(()));

        let entries = retry_output.read_all();
        assert_eq!(entries.len(), 1);
        assert_eq!(
            entries[0].events,
            vec![EngineEvent::OrderAck(OrderAck::Cancelled {
                command_id: CommandId(11),
                order_id: OrderId(100),
                journal_seq: JournalSeq(2),
            })]
        );
    }

    #[test]
    fn rejected_command_output_append_failure_does_not_advance_last_input_seq() {
        let mut runtime = SymbolRuntime::new(symbol());
        let mut output = FailingOutputJournal;

        let entry = InputJournalEntry {
            seq: JournalSeq(1),
            command_id: CommandId(10),
            command: Command::PlaceLimit(Order {
                order_id: OrderId(100),
                symbol: symbol(),
                side: Side::Buy,
                price: Price(0),
                quantity: Quantity(5),
            }),
        };

        assert_eq!(
            runtime.process_entry(entry, &mut output),
            Err(OutputJournalError::AppendFailed)
        );

        assert_eq!(runtime.last_input_seq(), None);
    }

    #[test]
    fn processing_successful_batch_advances_last_input_seq_to_latest_entry() {
        let mut runtime = SymbolRuntime::new(symbol());
        let mut output = InMemoryOutputJournal::new();

        let entries = vec![
            input_entry(1, 10, 100),
            input_entry(2, 11, 101),
            input_entry(3, 12, 102),
        ];

        assert_eq!(runtime.process_batch(entries, &mut output), Ok(3));
        assert_eq!(runtime.last_input_seq(), Some(JournalSeq(3)));

        let output_entries = output.read_all();
        assert_eq!(output_entries.len(), 3);
        assert_eq!(output_entries[0].journal_seq, JournalSeq(1));
        assert_eq!(output_entries[1].journal_seq, JournalSeq(2));
        assert_eq!(output_entries[2].journal_seq, JournalSeq(3));
    }

    #[test]
    fn batch_stops_at_first_output_append_failure() {
        let mut runtime = SymbolRuntime::new(symbol());
        let mut output = FailOnSecondAppendOutputJournal::new();

        let entries = vec![
            input_entry(1, 10, 100),
            input_entry(2, 11, 101),
            input_entry(3, 12, 102),
        ];

        assert_eq!(
            runtime.process_batch(entries, &mut output),
            Err(OutputJournalError::AppendFailed)
        );

        assert_eq!(runtime.last_input_seq(), Some(JournalSeq(1)));

        let output_entries = output.read_all();
        assert_eq!(output_entries.len(), 1);
        assert_eq!(output_entries[0].journal_seq, JournalSeq(1));
    }

    #[test]
    fn failed_batch_entry_can_be_retried_after_output_append_failure() {
        let mut runtime = SymbolRuntime::new(symbol());
        let mut output = FailOnSecondAppendOutputJournal::new();

        let failed_entry = input_entry(2, 11, 101);

        let entries = vec![
            input_entry(1, 10, 100),
            failed_entry.clone(),
            input_entry(3, 12, 102),
        ];

        assert_eq!(
            runtime.process_batch(entries, &mut output),
            Err(OutputJournalError::AppendFailed)
        );

        assert_eq!(runtime.last_input_seq(), Some(JournalSeq(1)));

        let mut retry_output = InMemoryOutputJournal::new();
        assert_eq!(runtime.process_entry(failed_entry, &mut retry_output), Ok(()));
        assert_eq!(runtime.last_input_seq(), Some(JournalSeq(2)));

        let retry_entries = retry_output.read_all();
        assert_eq!(retry_entries.len(), 1);
        assert_eq!(retry_entries[0].journal_seq, JournalSeq(2));
        assert_eq!(
            retry_entries[0].events,
            vec![EngineEvent::OrderAck(OrderAck::Accepted {
                command_id: CommandId(11),
                order_id: OrderId(101),
                journal_seq: JournalSeq(2),
            })]
        );
    }

    #[test]
fn failed_batch_trade_entry_can_be_retried_with_same_trade_id() {
    let mut runtime = SymbolRuntime::new(symbol());

    let resting_sell = InputJournalEntry {
        seq: JournalSeq(1),
        command_id: CommandId(10),
        command: Command::PlaceLimit(Order {
            order_id: OrderId(100),
            symbol: symbol(),
            side: Side::Sell,
            price: Price(100),
            quantity: Quantity(3),
        }),
    };

    let crossing_buy = InputJournalEntry {
        seq: JournalSeq(2),
        command_id: CommandId(11),
        command: Command::PlaceLimit(Order {
            order_id: OrderId(101),
            symbol: symbol(),
            side: Side::Buy,
            price: Price(100),
            quantity: Quantity(3),
        }),
    };

    let later_entry = input_entry(3, 12, 102);

    let mut output = FailOnSecondAppendOutputJournal::new();
    assert_eq!(
        runtime.process_batch(
            vec![resting_sell, crossing_buy.clone(), later_entry],
            &mut output
        ),
        Err(OutputJournalError::AppendFailed)
    );

    assert_eq!(runtime.last_input_seq(), Some(JournalSeq(1)));

    let output_entries = output.read_all();
    assert_eq!(output_entries.len(), 1);
    assert_eq!(output_entries[0].journal_seq, JournalSeq(1));

    let mut retry_output = InMemoryOutputJournal::new();
    assert_eq!(runtime.process_entry(crossing_buy, &mut retry_output), Ok(()));
    assert_eq!(runtime.last_input_seq(), Some(JournalSeq(2)));

    let retry_entries = retry_output.read_all();
    assert_eq!(retry_entries.len(), 1);
    assert_eq!(
        retry_entries[0].events,
        vec![
            EngineEvent::OrderAck(OrderAck::Accepted {
                command_id: CommandId(11),
                order_id: OrderId(101),
                journal_seq: JournalSeq(2),
            }),
            EngineEvent::Trade(TradeEvent {
                trade_id: TradeId(1),
                command_id: CommandId(11),
                journal_seq: JournalSeq(2),
                maker_order_id: OrderId(100),
                taker_order_id: OrderId(101),
                price: Price(100),
                quantity: Quantity(3),
            }),
        ]
    );
}
}