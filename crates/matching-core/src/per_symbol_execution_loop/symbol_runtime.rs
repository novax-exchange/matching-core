use crate::journal_adapter::{JournalAdapterError, JournalInputEntry, JournalOutputAppender};
use crate::matching_engine::{CommandIngress, IngressError};
use crate::matching_engine::{EngineEvent, OrderAck, RejectReason, TradeEvent};
use crate::order::Command;
use crate::order_book::OrderBook;
use crate::output_commit_boundary::OutputCommitRequest;
use crate::output_commit_boundary::{PendingOutputBuffer, PendingOutputBufferError};
use crate::types::{JournalSeq, Symbol, TradeId};

pub struct SymbolRuntime {
    ingress: CommandIngress,
    order_book: OrderBook,
    last_input_seq: Option<JournalSeq>,
    next_trade_id: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SafePointError {
    NonContiguousCommit {
        expected: JournalSeq,
        actual: JournalSeq,
    },
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

    pub fn mark_output_committed(&mut self, journal_seq: JournalSeq) -> Result<(), SafePointError> {
        let expected = match self.last_input_seq {
            Some(last_input_seq) => JournalSeq(last_input_seq.0 + 1),
            None => JournalSeq(1),
        };

        if journal_seq != expected {
            return Err(SafePointError::NonContiguousCommit {
                expected,
                actual: journal_seq,
            });
        }

        self.last_input_seq = Some(journal_seq);
        Ok(())
    }

    pub fn process_batch(
        &mut self,
        entries: Vec<JournalInputEntry>,
        output: &mut dyn JournalOutputAppender,
    ) -> Result<usize, JournalAdapterError> {
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
    ) -> Result<(), JournalAdapterError> {
        let order_book_before = self.order_book.clone();
        let next_trade_id_before = self.next_trade_id;
        let request = self.process_entry_to_output_request(entry);

        if let Err(error) = output.append(request.command_id, request.journal_seq, request.events) {
            self.order_book = order_book_before;
            self.next_trade_id = next_trade_id_before;
            return Err(error);
        }

        self.last_input_seq = Some(request.journal_seq);
        Ok(())
    }

    pub fn process_entry_into_pending_output_buffer(
        &mut self,
        entry: JournalInputEntry,
        pending_output_buffer: &mut PendingOutputBuffer,
    ) -> Result<(), PendingOutputBufferError> {
        let order_book_before = self.order_book.clone();
        let next_trade_id_before = self.next_trade_id;
        let request = self.process_entry_to_output_request(entry);

        if let Err(error) = pending_output_buffer.enqueue(request) {
            self.order_book = order_book_before;
            self.next_trade_id = next_trade_id_before;
            return Err(error);
        }

        Ok(())
    }

    pub fn process_entry_to_output_request(
        &mut self,
        entry: JournalInputEntry,
    ) -> OutputCommitRequest {
        let command = match self.ingress.validate(entry.command) {
            Ok(command) => command,
            Err(error) => {
                let events = vec![EngineEvent::OrderAck(OrderAck::Rejected {
                    command_id: entry.command_id,
                    order_id: None,
                    journal_seq: entry.seq,
                    reason: reject_reason_from_ingress_error(error),
                })];

                return OutputCommitRequest {
                    command_id: entry.command_id,
                    journal_seq: entry.seq,
                    events,
                };
            }
        };
        match command {
            Command::PlaceLimit(order) => {
                let order_id = order.order_id;

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
                        quantity: trade.quantity,
                    }));
                }

                OutputCommitRequest {
                    command_id: entry.command_id,
                    journal_seq: entry.seq,
                    events,
                }
            }
            Command::Cancel { order_id, .. } => {
                let events = match self.order_book.cancel(order_id) {
                    Ok(_) => vec![EngineEvent::OrderAck(OrderAck::Cancelled {
                        command_id: entry.command_id,
                        order_id,
                        journal_seq: entry.seq,
                    })],
                    Err(_) => vec![EngineEvent::OrderAck(OrderAck::Rejected {
                        command_id: entry.command_id,
                        order_id: Some(order_id),
                        journal_seq: entry.seq,
                        reason: RejectReason::OrderNotFound,
                    })],
                };

                OutputCommitRequest {
                    command_id: entry.command_id,
                    journal_seq: entry.seq,
                    events,
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::journal_adapter::{
        JournalAdapterError, JournalInputEntry, JournalOutputAppender, JournalOutputEntry,
    };
    use crate::matching_engine::{EngineEvent, OrderAck, TradeEvent};
    use crate::order::{Command, Order};
    use crate::output_commit_boundary::{PendingOutputBuffer, PendingOutputBufferError};
    use crate::types::{CommandId, JournalSeq, OrderId, Price, Quantity, Side, Symbol};

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

    fn symbol() -> Symbol {
        Symbol("BTC-USDT".to_string())
    }

    fn input_entry(seq: u64, command_id: u64, order_id: u64) -> JournalInputEntry {
        JournalInputEntry {
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

    fn limit_entry(
        seq: u64,
        command_id: u64,
        order_id: u64,
        side: Side,
        price: u64,
        quantity: u64,
    ) -> JournalInputEntry {
        JournalInputEntry {
            seq: JournalSeq(seq),
            command_id: CommandId(command_id),
            command: Command::PlaceLimit(Order {
                order_id: OrderId(order_id),
                symbol: symbol(),
                side,
                price: Price(price),
                quantity: Quantity(quantity),
            }),
        }
    }

    fn cancel_entry(seq: u64, command_id: u64, order_id: u64) -> JournalInputEntry {
        JournalInputEntry {
            seq: JournalSeq(seq),
            command_id: CommandId(command_id),
            command: Command::Cancel {
                order_id: OrderId(order_id),
                symbol: symbol(),
            },
        }
    }

    fn process_entries_on_fresh_runtime(
        entries: Vec<JournalInputEntry>,
    ) -> (Vec<JournalOutputEntry>, Option<JournalSeq>) {
        let mut runtime = SymbolRuntime::new(symbol());
        let mut output = InMemoryJournalOutputAppender::new();
        let expected_processed = entries.len();

        assert_eq!(
            runtime.process_batch(entries, &mut output),
            Ok(expected_processed)
        );

        (output.read_all(), runtime.last_input_seq())
    }

    #[test]
    fn processing_valid_entry_commits_output_and_advances_last_input_seq() {
        let mut runtime = SymbolRuntime::new(symbol());
        let mut output = InMemoryJournalOutputAppender::new();

        assert_eq!(
            runtime.process_entry(input_entry(1, 10, 100), &mut output),
            Ok(())
        );

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

    #[test]
    fn same_input_sequence_on_two_fresh_runtimes_produces_identical_output_entries_and_safe_point()
    {
        let entries = vec![
            limit_entry(1, 10, 100, Side::Sell, 100, 3),
            limit_entry(2, 11, 101, Side::Buy, 100, 3),
            limit_entry(3, 12, 102, Side::Buy, 99, 5),
            cancel_entry(4, 13, 102),
        ];

        let (first_output, first_safe_point) = process_entries_on_fresh_runtime(entries.clone());
        let (second_output, second_safe_point) = process_entries_on_fresh_runtime(entries);

        assert_eq!(first_output, second_output);
        assert_eq!(first_safe_point, second_safe_point);
        assert_eq!(first_safe_point, Some(JournalSeq(4)));
        assert_eq!(
            first_output,
            vec![
                JournalOutputEntry {
                    command_id: CommandId(10),
                    journal_seq: JournalSeq(1),
                    events: vec![EngineEvent::OrderAck(OrderAck::Accepted {
                        command_id: CommandId(10),
                        order_id: OrderId(100),
                        journal_seq: JournalSeq(1),
                    })],
                },
                JournalOutputEntry {
                    command_id: CommandId(11),
                    journal_seq: JournalSeq(2),
                    events: vec![
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
                    ],
                },
                JournalOutputEntry {
                    command_id: CommandId(12),
                    journal_seq: JournalSeq(3),
                    events: vec![EngineEvent::OrderAck(OrderAck::Accepted {
                        command_id: CommandId(12),
                        order_id: OrderId(102),
                        journal_seq: JournalSeq(3),
                    })],
                },
                JournalOutputEntry {
                    command_id: CommandId(13),
                    journal_seq: JournalSeq(4),
                    events: vec![EngineEvent::OrderAck(OrderAck::Cancelled {
                        command_id: CommandId(13),
                        order_id: OrderId(102),
                        journal_seq: JournalSeq(4),
                    })],
                },
            ]
        );
    }

    #[test]
    fn process_entry_to_output_request_does_not_advance_last_input_seq_before_commit() {
        let mut runtime = SymbolRuntime::new(symbol());

        let request = runtime.process_entry_to_output_request(input_entry(1, 10, 100));

        assert_eq!(request.command_id, CommandId(10));
        assert_eq!(request.journal_seq, JournalSeq(1));
        assert_eq!(
            request.events,
            vec![EngineEvent::OrderAck(OrderAck::Accepted {
                command_id: CommandId(10),
                order_id: OrderId(100),
                journal_seq: JournalSeq(1),
            })]
        );
        assert_eq!(runtime.last_input_seq(), None);
    }

    #[test]
    fn mark_output_committed_advances_last_input_seq_after_request_is_committed() {
        let mut runtime = SymbolRuntime::new(symbol());

        let request = runtime.process_entry_to_output_request(input_entry(1, 10, 100));
        assert_eq!(runtime.last_input_seq(), None);

        assert_eq!(runtime.mark_output_committed(request.journal_seq), Ok(()));

        assert_eq!(runtime.last_input_seq(), Some(JournalSeq(1)));
    }

    #[test]
    fn mark_output_committed_rejects_non_contiguous_safe_point() {
        let mut runtime = SymbolRuntime::new(symbol());

        assert_eq!(
            runtime.mark_output_committed(JournalSeq(2)),
            Err(SafePointError::NonContiguousCommit {
                expected: JournalSeq(1),
                actual: JournalSeq(2),
            })
        );
        assert_eq!(runtime.last_input_seq(), None);

        assert_eq!(runtime.mark_output_committed(JournalSeq(1)), Ok(()));
        assert_eq!(
            runtime.mark_output_committed(JournalSeq(3)),
            Err(SafePointError::NonContiguousCommit {
                expected: JournalSeq(2),
                actual: JournalSeq(3),
            })
        );
        assert_eq!(runtime.last_input_seq(), Some(JournalSeq(1)));
    }

    #[test]
    fn pending_output_buffer_full_rolls_back_runtime_state_before_retry() {
        let mut runtime = SymbolRuntime::new(symbol());
        let mut full_buffer = PendingOutputBuffer::new(0);

        assert_eq!(
            runtime.process_entry_into_pending_output_buffer(
                input_entry(1, 10, 100),
                &mut full_buffer
            ),
            Err(PendingOutputBufferError::BufferFull)
        );
        assert_eq!(runtime.last_input_seq(), None);

        let mut retry_buffer = PendingOutputBuffer::new(1);

        assert_eq!(
            runtime.process_entry_into_pending_output_buffer(
                input_entry(1, 10, 100),
                &mut retry_buffer
            ),
            Ok(())
        );

        let requests = retry_buffer.drain_batch(10);
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].journal_seq, JournalSeq(1));
        assert_eq!(
            requests[0].events,
            vec![EngineEvent::OrderAck(OrderAck::Accepted {
                command_id: CommandId(10),
                order_id: OrderId(100),
                journal_seq: JournalSeq(1),
            })]
        );
        assert_eq!(runtime.last_input_seq(), None);
    }

    #[test]
    fn pending_output_buffer_full_for_crossing_trade_rolls_back_trade_id_before_retry() {
        let mut runtime = SymbolRuntime::new(symbol());
        let mut output = InMemoryJournalOutputAppender::new();

        assert_eq!(
            runtime.process_entry(limit_entry(1, 10, 100, Side::Sell, 100, 3), &mut output),
            Ok(())
        );
        assert_eq!(runtime.last_input_seq(), Some(JournalSeq(1)));

        let crossing_buy = limit_entry(2, 11, 101, Side::Buy, 100, 3);
        let mut full_buffer = PendingOutputBuffer::new(0);

        assert_eq!(
            runtime
                .process_entry_into_pending_output_buffer(crossing_buy.clone(), &mut full_buffer),
            Err(PendingOutputBufferError::BufferFull)
        );
        assert_eq!(runtime.last_input_seq(), Some(JournalSeq(1)));

        let mut retry_buffer = PendingOutputBuffer::new(1);

        assert_eq!(
            runtime.process_entry_into_pending_output_buffer(crossing_buy, &mut retry_buffer),
            Ok(())
        );

        let requests = retry_buffer.drain_batch(10);
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].journal_seq, JournalSeq(2));
        assert_eq!(
            requests[0].events,
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
        assert_eq!(runtime.last_input_seq(), Some(JournalSeq(1)));
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
    fn output_append_failure_does_not_advance_last_input_seq() {
        let mut runtime = SymbolRuntime::new(symbol());
        let mut output = FailingJournalOutputAppender;

        assert_eq!(
            runtime.process_entry(input_entry(1, 10, 100), &mut output),
            Err(JournalAdapterError::AppendFailed)
        );

        assert_eq!(runtime.last_input_seq(), None);
    }

    #[test]
    fn processing_multiple_successful_entries_advances_last_input_seq_to_latest_entry() {
        let mut runtime = SymbolRuntime::new(symbol());
        let mut output = InMemoryJournalOutputAppender::new();

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
    fn failed_second_output_append_keeps_last_input_seq_at_first_entry() {
        let mut runtime = SymbolRuntime::new(symbol());
        let mut output = FailOnSecondAppendJournalOutputAppender::new();

        assert_eq!(
            runtime.process_entry(input_entry(1, 10, 100), &mut output),
            Ok(()),
        );
        assert_eq!(
            runtime.process_entry(input_entry(2, 11, 101), &mut output),
            Err(JournalAdapterError::AppendFailed)
        );

        assert_eq!(runtime.last_input_seq(), Some(JournalSeq(1)));

        let entries = output.read_all();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].journal_seq, JournalSeq(1));
    }

    #[test]
    fn invalid_price_order_writes_rejected_output_and_advances_last_input_seq() {
        let mut runtime = SymbolRuntime::new(symbol());
        let mut output = InMemoryJournalOutputAppender::new();

        let entry = JournalInputEntry {
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
        let mut output = InMemoryJournalOutputAppender::new();

        let entry = JournalInputEntry {
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
        let mut output = InMemoryJournalOutputAppender::new();

        let entry = JournalInputEntry {
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
        let mut output = InMemoryJournalOutputAppender::new();

        let entry = JournalInputEntry {
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
        let mut output = InMemoryJournalOutputAppender::new();

        assert_eq!(
            runtime.process_entry(input_entry(1, 10, 100), &mut output),
            Ok(())
        );

        let cancel_entry = JournalInputEntry {
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
    fn matching_order_writes_trade_event_to_journal_output_appender() {
        let mut runtime = SymbolRuntime::new(symbol());
        let mut output = InMemoryJournalOutputAppender::new();

        let resting_sell = JournalInputEntry {
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

        let crossing_buy = JournalInputEntry {
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

        let mut successful_output = InMemoryJournalOutputAppender::new();

        let resting_sell = JournalInputEntry {
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

        assert_eq!(
            runtime.process_entry(resting_sell, &mut successful_output),
            Ok(())
        );

        let crossing_buy = JournalInputEntry {
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

        let mut failing_output = FailingJournalOutputAppender;
        assert_eq!(
            runtime.process_entry(crossing_buy.clone(), &mut failing_output),
            Err(JournalAdapterError::AppendFailed)
        );
        assert_eq!(runtime.last_input_seq(), Some(JournalSeq(1)));

        let mut retry_output = InMemoryJournalOutputAppender::new();
        assert_eq!(
            runtime.process_entry(crossing_buy, &mut retry_output),
            Ok(())
        );

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

        let mut successful_output = InMemoryJournalOutputAppender::new();
        assert_eq!(
            runtime.process_entry(input_entry(1, 10, 100), &mut successful_output),
            Ok(())
        );

        let cancel_entry = JournalInputEntry {
            seq: JournalSeq(2),
            command_id: CommandId(11),
            command: Command::Cancel {
                order_id: OrderId(100),
                symbol: symbol(),
            },
        };

        let mut failing_output = FailingJournalOutputAppender;
        assert_eq!(
            runtime.process_entry(cancel_entry.clone(), &mut failing_output),
            Err(JournalAdapterError::AppendFailed)
        );
        assert_eq!(runtime.last_input_seq(), Some(JournalSeq(1)));

        let mut retry_output = InMemoryJournalOutputAppender::new();
        assert_eq!(
            runtime.process_entry(cancel_entry, &mut retry_output),
            Ok(())
        );

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
        let mut output = FailingJournalOutputAppender;

        let entry = JournalInputEntry {
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
            Err(JournalAdapterError::AppendFailed)
        );

        assert_eq!(runtime.last_input_seq(), None);
    }

    #[test]
    fn processing_successful_batch_advances_last_input_seq_to_latest_entry() {
        let mut runtime = SymbolRuntime::new(symbol());
        let mut output = InMemoryJournalOutputAppender::new();

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
        let mut output = FailOnSecondAppendJournalOutputAppender::new();

        let entries = vec![
            input_entry(1, 10, 100),
            input_entry(2, 11, 101),
            input_entry(3, 12, 102),
        ];

        assert_eq!(
            runtime.process_batch(entries, &mut output),
            Err(JournalAdapterError::AppendFailed)
        );

        assert_eq!(runtime.last_input_seq(), Some(JournalSeq(1)));

        let output_entries = output.read_all();
        assert_eq!(output_entries.len(), 1);
        assert_eq!(output_entries[0].journal_seq, JournalSeq(1));
    }

    #[test]
    fn failed_batch_entry_can_be_retried_after_output_append_failure() {
        let mut runtime = SymbolRuntime::new(symbol());
        let mut output = FailOnSecondAppendJournalOutputAppender::new();

        let failed_entry = input_entry(2, 11, 101);

        let entries = vec![
            input_entry(1, 10, 100),
            failed_entry.clone(),
            input_entry(3, 12, 102),
        ];

        assert_eq!(
            runtime.process_batch(entries, &mut output),
            Err(JournalAdapterError::AppendFailed)
        );

        assert_eq!(runtime.last_input_seq(), Some(JournalSeq(1)));

        let mut retry_output = InMemoryJournalOutputAppender::new();
        assert_eq!(
            runtime.process_entry(failed_entry, &mut retry_output),
            Ok(())
        );
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

        let resting_sell = JournalInputEntry {
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

        let crossing_buy = JournalInputEntry {
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

        let mut output = FailOnSecondAppendJournalOutputAppender::new();
        assert_eq!(
            runtime.process_batch(
                vec![resting_sell, crossing_buy.clone(), later_entry],
                &mut output
            ),
            Err(JournalAdapterError::AppendFailed)
        );

        assert_eq!(runtime.last_input_seq(), Some(JournalSeq(1)));

        let output_entries = output.read_all();
        assert_eq!(output_entries.len(), 1);
        assert_eq!(output_entries[0].journal_seq, JournalSeq(1));

        let mut retry_output = InMemoryJournalOutputAppender::new();
        assert_eq!(
            runtime.process_entry(crossing_buy, &mut retry_output),
            Ok(())
        );
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
