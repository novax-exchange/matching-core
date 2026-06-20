use crate::journal_adapter::{JournalAdapterError, JournalInputEntry, JournalOutputAppender};
use crate::matching_engine::{CommandIngress, IngressError};
use crate::matching_engine::{
    EngineEvent, MarketEvent, OrderAck, OrderAddedEvent, OrderCancelledEvent, RejectReason,
    TradeEvent,
};
use crate::order::Command;
use crate::order_book::OrderBook;
use crate::output_commit_boundary::OutputCommitRequest;
use crate::output_commit_boundary::{PendingOutputBuffer, PendingOutputBufferError};
use crate::snapshot_restore::{OrderBookSnapshot, SymbolRuntimeSnapshot};
use crate::types::{Checksum, CommandId, JournalSeq, MarketSeq, OrderId, Symbol, TradeId};
use std::collections::HashSet;

pub struct SymbolRuntime {
    ingress: CommandIngress,
    order_book: OrderBook,
    last_input_seq: Option<JournalSeq>,
    next_trade_seq: u64,
    next_market_seq: u64,
    seen_command_ids: HashSet<CommandId>,
    seen_order_ids: HashSet<OrderId>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SafePointError {
    NonMonotonicCommit {
        last_committed: Option<JournalSeq>,
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
            next_trade_seq: 1,
            next_market_seq: 1,
            seen_command_ids: HashSet::new(),
            seen_order_ids: HashSet::new(),
        }
    }

    pub fn last_input_seq(&self) -> Option<JournalSeq> {
        self.last_input_seq
    }

    pub fn symbol(&self) -> &Symbol {
        self.order_book.symbol()
    }

    pub fn checksum(&self) -> Checksum {
        self.order_book.checksum()
    }

    pub fn snapshot(&self) -> Option<SymbolRuntimeSnapshot> {
        let last_input_seq = self.last_input_seq?;
        let mut seen_command_ids: Vec<CommandId> = self.seen_command_ids.iter().copied().collect();
        let mut seen_order_ids: Vec<OrderId> = self.seen_order_ids.iter().copied().collect();

        seen_command_ids.sort_by_key(|command_id| command_id.0);
        seen_order_ids.sort_by_key(|order_id| order_id.0);

        Some(SymbolRuntimeSnapshot {
            order_book_snapshot: OrderBookSnapshot::from_order_book(
                &self.order_book,
                last_input_seq,
            ),
            next_trade_seq: self.next_trade_seq,
            next_market_seq: self.next_market_seq,
            seen_command_ids,
            seen_order_ids,
        })
    }

    pub fn restore_from_snapshot(snapshot: SymbolRuntimeSnapshot) -> Self {
        let symbol = snapshot.order_book_snapshot.symbol.clone();
        let last_input_seq = snapshot.order_book_snapshot.last_input_seq;
        let order_book = snapshot.order_book_snapshot.restore_order_book();

        Self {
            ingress: CommandIngress::new(symbol),
            order_book,
            last_input_seq: Some(last_input_seq),
            next_trade_seq: snapshot.next_trade_seq,
            next_market_seq: snapshot.next_market_seq,
            seen_command_ids: snapshot.seen_command_ids.into_iter().collect(),
            seen_order_ids: snapshot.seen_order_ids.into_iter().collect(),
        }
    }

    pub fn mark_output_committed(&mut self, journal_seq: JournalSeq) -> Result<(), SafePointError> {
        if self
            .last_input_seq
            .map(|last_input_seq| journal_seq <= last_input_seq)
            .unwrap_or(false)
        {
            return Err(SafePointError::NonMonotonicCommit {
                last_committed: self.last_input_seq,
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
        let next_trade_seq_before = self.next_trade_seq;
        let next_market_seq_before = self.next_market_seq;
        let seen_command_ids_before = self.seen_command_ids.clone();
        let seen_order_ids_before = self.seen_order_ids.clone();
        let request = self.process_entry_to_output_request(entry);

        if let Err(error) = output.append(request.command_id, request.journal_seq, request.events) {
            self.order_book = order_book_before;
            self.next_trade_seq = next_trade_seq_before;
            self.next_market_seq = next_market_seq_before;
            self.seen_command_ids = seen_command_ids_before;
            self.seen_order_ids = seen_order_ids_before;
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
        let next_trade_seq_before = self.next_trade_seq;
        let next_market_seq_before = self.next_market_seq;
        let seen_command_ids_before = self.seen_command_ids.clone();
        let seen_order_ids_before = self.seen_order_ids.clone();
        let request = self.process_entry_to_output_request(entry);

        if let Err(error) = pending_output_buffer.enqueue(request) {
            self.order_book = order_book_before;
            self.next_trade_seq = next_trade_seq_before;
            self.next_market_seq = next_market_seq_before;
            self.seen_command_ids = seen_command_ids_before;
            self.seen_order_ids = seen_order_ids_before;
            return Err(error);
        }

        Ok(())
    }

    pub fn process_entry_to_output_request(
        &mut self,
        entry: JournalInputEntry,
    ) -> OutputCommitRequest {
        let duplicate_order_id = match &entry.command {
            Command::PlaceLimit(order) => Some(order.order_id),
            Command::Cancel { order_id, .. } => Some(*order_id),
        };

        if self.seen_command_ids.contains(&entry.command_id) {
            return OutputCommitRequest {
                command_id: entry.command_id,
                journal_seq: entry.seq,
                events: vec![EngineEvent::OrderAck(OrderAck::Rejected {
                    command_id: entry.command_id,
                    order_id: duplicate_order_id,
                    journal_seq: entry.seq,
                    reason: RejectReason::DuplicateCommandId,
                })],
            };
        }

        self.seen_command_ids.insert(entry.command_id);

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

                if self.seen_order_ids.contains(&order_id) {
                    return OutputCommitRequest {
                        command_id: entry.command_id,
                        journal_seq: entry.seq,
                        events: vec![EngineEvent::OrderAck(OrderAck::Rejected {
                            command_id: entry.command_id,
                            order_id: Some(order_id),
                            journal_seq: entry.seq,
                            reason: RejectReason::DuplicateOrderId,
                        })],
                    };
                }

                self.seen_order_ids.insert(order_id);

                let result = self.order_book.place_limit(order);

                let mut events = vec![EngineEvent::OrderAck(OrderAck::Accepted {
                    command_id: entry.command_id,
                    order_id: order_id,
                    journal_seq: entry.seq,
                })];

                for trade in result.trades {
                    let trade_id = TradeId(self.next_trade_seq);
                    let market_seq = MarketSeq(self.next_market_seq);
                    self.next_trade_seq += 1;
                    self.next_market_seq += 1;

                    events.push(EngineEvent::Trade(TradeEvent {
                        trade_id,
                        market_seq,
                        command_id: entry.command_id,
                        journal_seq: entry.seq,
                        maker_order_id: trade.maker_order_id,
                        taker_order_id: trade.taker_order_id,
                        price: trade.price,
                        quantity: trade.quantity,
                    }));
                }

                if let Some(resting_order) = result.resting_order {
                    let market_seq = MarketSeq(self.next_market_seq);
                    self.next_market_seq += 1;

                    events.push(EngineEvent::Market(MarketEvent::OrderAdded(
                        OrderAddedEvent {
                            market_seq,
                            command_id: entry.command_id,
                            journal_seq: entry.seq,
                            order_id: resting_order.order_id,
                            side: resting_order.side,
                            price: resting_order.price,
                            quantity: resting_order.quantity,
                        },
                    )));
                }

                OutputCommitRequest {
                    command_id: entry.command_id,
                    journal_seq: entry.seq,
                    events,
                }
            }
            Command::Cancel { order_id, .. } => {
                let events = match self.order_book.cancel(order_id) {
                    Ok(cancelled_order) => {
                        let market_seq = MarketSeq(self.next_market_seq);
                        self.next_market_seq += 1;

                        vec![
                            EngineEvent::OrderAck(OrderAck::Cancelled {
                                command_id: entry.command_id,
                                order_id,
                                journal_seq: entry.seq,
                            }),
                            EngineEvent::Market(MarketEvent::OrderCancelled(OrderCancelledEvent {
                                market_seq,
                                command_id: entry.command_id,
                                journal_seq: entry.seq,
                                order_id: cancelled_order.order_id,
                                side: cancelled_order.side,
                                price: cancelled_order.price,
                                quantity: cancelled_order.quantity,
                            })),
                        ]
                    }
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
    use crate::matching_engine::{
        EngineEvent, MarketEvent, OrderAck, OrderAddedEvent, OrderCancelledEvent, TradeEvent,
    };
    use crate::order::{Command, Order};
    use crate::output_commit_boundary::{
        run_output_batch_commit_step_report, OutputJournalClient, PendingOutputBuffer,
        PendingOutputBufferError,
    };
    use crate::types::{CommandId, JournalSeq, MarketSeq, OrderId, Price, Quantity, Side, Symbol};

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
                output_commit_metadata: None,
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

    fn accepted_event(command_id: u64, order_id: u64, journal_seq: u64) -> EngineEvent {
        EngineEvent::OrderAck(OrderAck::Accepted {
            command_id: CommandId(command_id),
            order_id: OrderId(order_id),
            journal_seq: JournalSeq(journal_seq),
        })
    }

    fn cancelled_ack_event(command_id: u64, order_id: u64, journal_seq: u64) -> EngineEvent {
        EngineEvent::OrderAck(OrderAck::Cancelled {
            command_id: CommandId(command_id),
            order_id: OrderId(order_id),
            journal_seq: JournalSeq(journal_seq),
        })
    }

    fn added_event(
        market_seq: u64,
        command_id: u64,
        journal_seq: u64,
        order_id: u64,
        side: Side,
        price: u64,
        quantity: u64,
    ) -> EngineEvent {
        EngineEvent::Market(MarketEvent::OrderAdded(OrderAddedEvent {
            market_seq: MarketSeq(market_seq),
            command_id: CommandId(command_id),
            journal_seq: JournalSeq(journal_seq),
            order_id: OrderId(order_id),
            side,
            price: Price(price),
            quantity: Quantity(quantity),
        }))
    }

    fn cancelled_market_event(
        market_seq: u64,
        command_id: u64,
        journal_seq: u64,
        order_id: u64,
        side: Side,
        price: u64,
        quantity: u64,
    ) -> EngineEvent {
        EngineEvent::Market(MarketEvent::OrderCancelled(OrderCancelledEvent {
            market_seq: MarketSeq(market_seq),
            command_id: CommandId(command_id),
            journal_seq: JournalSeq(journal_seq),
            order_id: OrderId(order_id),
            side,
            price: Price(price),
            quantity: Quantity(quantity),
        }))
    }

    fn trade_event(
        trade_id: u64,
        market_seq: u64,
        command_id: u64,
        journal_seq: u64,
        maker_order_id: u64,
        taker_order_id: u64,
        price: u64,
        quantity: u64,
    ) -> EngineEvent {
        EngineEvent::Trade(TradeEvent {
            trade_id: TradeId(trade_id),
            market_seq: MarketSeq(market_seq),
            command_id: CommandId(command_id),
            journal_seq: JournalSeq(journal_seq),
            maker_order_id: OrderId(maker_order_id),
            taker_order_id: OrderId(taker_order_id),
            price: Price(price),
            quantity: Quantity(quantity),
        })
    }

    fn process_entries_through_async_output_commit_on_fresh_runtime(
        entries: Vec<JournalInputEntry>,
    ) -> (Vec<JournalOutputEntry>, Option<JournalSeq>) {
        let mut runtime = SymbolRuntime::new(symbol());
        let mut pending_output_buffer = PendingOutputBuffer::new(entries.len());
        let expected_processed = entries.len();

        for entry in entries {
            assert_eq!(
                runtime.process_entry_into_pending_output_buffer(entry, &mut pending_output_buffer),
                Ok(())
            );
        }
        assert_eq!(runtime.last_input_seq(), None);

        let mut journal_client = OutputJournalClient::new();
        let mut output = InMemoryJournalOutputAppender::new();
        let report = run_output_batch_commit_step_report(
            &mut journal_client,
            &mut pending_output_buffer,
            &mut output,
            expected_processed,
        );

        assert_eq!(report.blocking_seq, None);
        assert_eq!(report.blocking_outcome, None);
        assert_eq!(report.commit_result.committed_count, expected_processed);
        assert_eq!(
            runtime.mark_output_committed(report.commit_result.committed_seqs[0]),
            Ok(())
        );
        for journal_seq in report.commit_result.committed_seqs.iter().skip(1) {
            assert_eq!(runtime.mark_output_committed(*journal_seq), Ok(()));
        }

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
            vec![
                accepted_event(10, 100, 1),
                added_event(1, 10, 1, 100, Side::Buy, 100, 5),
            ]
        );
    }

    #[test]
    fn same_input_sequence_on_two_fresh_runtimes_produces_identical_output_entries_and_safe_point_through_async_output_commit(
    ) {
        let entries = vec![
            limit_entry(1, 10, 100, Side::Sell, 100, 3),
            limit_entry(2, 11, 101, Side::Buy, 100, 3),
            limit_entry(3, 12, 102, Side::Buy, 99, 5),
            cancel_entry(4, 13, 102),
        ];

        let (first_output, first_safe_point) =
            process_entries_through_async_output_commit_on_fresh_runtime(entries.clone());
        let (second_output, second_safe_point) =
            process_entries_through_async_output_commit_on_fresh_runtime(entries);

        assert_eq!(first_output, second_output);
        assert_eq!(first_safe_point, second_safe_point);
        assert_eq!(first_safe_point, Some(JournalSeq(4)));
        assert_eq!(
            first_output,
            vec![
                JournalOutputEntry {
                    command_id: CommandId(10),
                    journal_seq: JournalSeq(1),
                    events: vec![
                        accepted_event(10, 100, 1),
                        added_event(1, 10, 1, 100, Side::Sell, 100, 3),
                    ],
                    output_commit_metadata: None,
                },
                JournalOutputEntry {
                    command_id: CommandId(11),
                    journal_seq: JournalSeq(2),
                    events: vec![
                        accepted_event(11, 101, 2),
                        trade_event(1, 2, 11, 2, 100, 101, 100, 3),
                    ],
                    output_commit_metadata: None,
                },
                JournalOutputEntry {
                    command_id: CommandId(12),
                    journal_seq: JournalSeq(3),
                    events: vec![
                        accepted_event(12, 102, 3),
                        added_event(3, 12, 3, 102, Side::Buy, 99, 5),
                    ],
                    output_commit_metadata: None,
                },
                JournalOutputEntry {
                    command_id: CommandId(13),
                    journal_seq: JournalSeq(4),
                    events: vec![
                        cancelled_ack_event(13, 102, 4),
                        cancelled_market_event(4, 13, 4, 102, Side::Buy, 99, 5),
                    ],
                    output_commit_metadata: None,
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
            vec![
                accepted_event(10, 100, 1),
                added_event(1, 10, 1, 100, Side::Buy, 100, 5),
            ]
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
    fn mark_output_committed_allows_global_sequence_gaps_for_one_symbol() {
        let mut runtime = SymbolRuntime::new(symbol());

        assert_eq!(runtime.mark_output_committed(JournalSeq(2)), Ok(()));
        assert_eq!(runtime.last_input_seq(), Some(JournalSeq(2)));
        assert_eq!(runtime.mark_output_committed(JournalSeq(5)), Ok(()));
        assert_eq!(runtime.last_input_seq(), Some(JournalSeq(5)));
    }

    #[test]
    fn mark_output_committed_rejects_non_monotonic_safe_point() {
        let mut runtime = SymbolRuntime::new(symbol());

        assert_eq!(runtime.mark_output_committed(JournalSeq(3)), Ok(()));
        assert_eq!(
            runtime.mark_output_committed(JournalSeq(2)),
            Err(SafePointError::NonMonotonicCommit {
                last_committed: Some(JournalSeq(3)),
                actual: JournalSeq(2),
            })
        );
        assert_eq!(
            runtime.mark_output_committed(JournalSeq(3)),
            Err(SafePointError::NonMonotonicCommit {
                last_committed: Some(JournalSeq(3)),
                actual: JournalSeq(3),
            })
        );
        assert_eq!(runtime.last_input_seq(), Some(JournalSeq(3)));
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
            vec![
                accepted_event(10, 100, 1),
                added_event(1, 10, 1, 100, Side::Buy, 100, 5),
            ]
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
                accepted_event(11, 101, 2),
                trade_event(1, 2, 11, 2, 100, 101, 100, 3),
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
                output_commit_metadata: None,
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
            vec![
                cancelled_ack_event(11, 100, 2),
                cancelled_market_event(2, 11, 2, 100, Side::Buy, 100, 5),
            ]
        );
    }

    #[test]
    fn accepted_resting_order_emits_market_order_added_event() {
        let mut runtime = SymbolRuntime::new(symbol());
        let mut output = InMemoryJournalOutputAppender::new();

        assert_eq!(
            runtime.process_entry(limit_entry(1, 10, 100, Side::Buy, 100, 5), &mut output),
            Ok(())
        );

        let entries = output.read_all();
        assert_eq!(
            entries[0].events,
            vec![
                EngineEvent::OrderAck(OrderAck::Accepted {
                    command_id: CommandId(10),
                    order_id: OrderId(100),
                    journal_seq: JournalSeq(1),
                }),
                EngineEvent::Market(MarketEvent::OrderAdded(OrderAddedEvent {
                    market_seq: MarketSeq(1),
                    command_id: CommandId(10),
                    journal_seq: JournalSeq(1),
                    order_id: OrderId(100),
                    side: Side::Buy,
                    price: Price(100),
                    quantity: Quantity(5),
                })),
            ]
        );
    }

    #[test]
    fn cancelled_order_emits_market_order_cancelled_event() {
        let mut runtime = SymbolRuntime::new(symbol());
        let mut output = InMemoryJournalOutputAppender::new();

        assert_eq!(
            runtime.process_entry(limit_entry(1, 10, 100, Side::Buy, 100, 5), &mut output),
            Ok(())
        );
        assert_eq!(
            runtime.process_entry(cancel_entry(2, 11, 100), &mut output),
            Ok(())
        );

        let entries = output.read_all();
        assert_eq!(
            entries[1].events,
            vec![
                EngineEvent::OrderAck(OrderAck::Cancelled {
                    command_id: CommandId(11),
                    order_id: OrderId(100),
                    journal_seq: JournalSeq(2),
                }),
                EngineEvent::Market(MarketEvent::OrderCancelled(OrderCancelledEvent {
                    market_seq: MarketSeq(2),
                    command_id: CommandId(11),
                    journal_seq: JournalSeq(2),
                    order_id: OrderId(100),
                    side: Side::Buy,
                    price: Price(100),
                    quantity: Quantity(5),
                })),
            ]
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
                accepted_event(11, 101, 2),
                trade_event(1, 2, 11, 2, 100, 101, 100, 3),
            ]
        );
    }

    #[test]
    fn duplicate_order_id_is_rejected_before_matching_or_mutating_order_book() {
        let mut runtime = SymbolRuntime::new(symbol());
        let mut output = InMemoryJournalOutputAppender::new();

        let resting_sell = limit_entry(1, 10, 100, Side::Sell, 100, 3);
        let duplicate_crossing_buy = limit_entry(2, 11, 100, Side::Buy, 100, 3);
        let cancel_original = cancel_entry(3, 12, 100);

        assert_eq!(runtime.process_entry(resting_sell, &mut output), Ok(()));
        assert_eq!(
            runtime.process_entry(duplicate_crossing_buy, &mut output),
            Ok(())
        );
        assert_eq!(runtime.process_entry(cancel_original, &mut output), Ok(()));
        assert_eq!(runtime.last_input_seq(), Some(JournalSeq(3)));

        let entries = output.read_all();
        assert_eq!(entries.len(), 3);
        assert_eq!(
            entries[1].events,
            vec![EngineEvent::OrderAck(OrderAck::Rejected {
                command_id: CommandId(11),
                order_id: Some(OrderId(100)),
                journal_seq: JournalSeq(2),
                reason: RejectReason::DuplicateOrderId,
            })]
        );
        assert_eq!(
            entries[2].events,
            vec![
                cancelled_ack_event(12, 100, 3),
                cancelled_market_event(2, 12, 3, 100, Side::Sell, 100, 3),
            ]
        );
    }

    #[test]
    fn duplicate_order_id_is_rejected_after_original_order_was_filled() {
        let mut runtime = SymbolRuntime::new(symbol());
        let mut output = InMemoryJournalOutputAppender::new();

        let resting_sell = limit_entry(1, 10, 100, Side::Sell, 100, 3);
        let crossing_buy = limit_entry(2, 11, 101, Side::Buy, 100, 3);
        let duplicate_after_fill = limit_entry(3, 12, 100, Side::Buy, 99, 1);

        assert_eq!(runtime.process_entry(resting_sell, &mut output), Ok(()));
        assert_eq!(runtime.process_entry(crossing_buy, &mut output), Ok(()));
        assert_eq!(
            runtime.process_entry(duplicate_after_fill, &mut output),
            Ok(())
        );

        let entries = output.read_all();
        assert_eq!(entries.len(), 3);
        assert_eq!(
            entries[2].events,
            vec![EngineEvent::OrderAck(OrderAck::Rejected {
                command_id: CommandId(12),
                order_id: Some(OrderId(100)),
                journal_seq: JournalSeq(3),
                reason: RejectReason::DuplicateOrderId,
            })]
        );
    }

    #[test]
    fn duplicate_command_id_is_rejected_before_matching_or_mutating_order_book() {
        let mut runtime = SymbolRuntime::new(symbol());
        let mut output = InMemoryJournalOutputAppender::new();

        let resting_sell = limit_entry(1, 10, 100, Side::Sell, 100, 3);
        let duplicate_command_crossing_buy = limit_entry(2, 10, 101, Side::Buy, 100, 3);
        let cancel_original = cancel_entry(3, 12, 100);

        assert_eq!(runtime.process_entry(resting_sell, &mut output), Ok(()));
        assert_eq!(
            runtime.process_entry(duplicate_command_crossing_buy, &mut output),
            Ok(())
        );
        assert_eq!(runtime.process_entry(cancel_original, &mut output), Ok(()));

        let entries = output.read_all();
        assert_eq!(entries.len(), 3);
        assert_eq!(
            entries[1].events,
            vec![EngineEvent::OrderAck(OrderAck::Rejected {
                command_id: CommandId(10),
                order_id: Some(OrderId(101)),
                journal_seq: JournalSeq(2),
                reason: RejectReason::DuplicateCommandId,
            })]
        );
        assert_eq!(
            entries[2].events,
            vec![
                cancelled_ack_event(12, 100, 3),
                cancelled_market_event(2, 12, 3, 100, Side::Sell, 100, 3),
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
                accepted_event(11, 101, 2),
                trade_event(1, 2, 11, 2, 100, 101, 100, 3),
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
            vec![
                cancelled_ack_event(11, 100, 2),
                cancelled_market_event(2, 11, 2, 100, Side::Buy, 100, 5),
            ]
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
            vec![
                accepted_event(11, 101, 2),
                added_event(2, 11, 2, 101, Side::Buy, 100, 5),
            ]
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
                accepted_event(11, 101, 2),
                trade_event(1, 2, 11, 2, 100, 101, 100, 3),
            ]
        );
    }

    #[test]
    fn restored_runtime_continues_trade_and_market_sequence_from_snapshot() {
        let mut runtime = SymbolRuntime::new(symbol());
        let mut output = InMemoryJournalOutputAppender::new();

        assert_eq!(
            runtime.process_entry(limit_entry(1, 10, 100, Side::Sell, 100, 1), &mut output),
            Ok(())
        );
        assert_eq!(
            runtime.process_entry(limit_entry(2, 11, 101, Side::Buy, 100, 1), &mut output),
            Ok(())
        );

        let snapshot = runtime.snapshot().expect("snapshot requires a safe point");
        let mut restored = SymbolRuntime::restore_from_snapshot(snapshot);
        let mut restored_output = InMemoryJournalOutputAppender::new();

        assert_eq!(
            restored.process_entry(
                limit_entry(3, 12, 102, Side::Sell, 100, 1),
                &mut restored_output
            ),
            Ok(())
        );
        assert_eq!(
            restored.process_entry(
                limit_entry(4, 13, 103, Side::Buy, 100, 1),
                &mut restored_output
            ),
            Ok(())
        );

        let entries = restored_output.read_all();
        assert_eq!(entries.len(), 2);
        assert_eq!(
            entries[1].events,
            vec![
                accepted_event(13, 103, 4),
                trade_event(2, 4, 13, 4, 102, 103, 100, 1),
            ]
        );
    }

    #[test]
    fn restored_runtime_rejects_order_id_seen_before_snapshot() {
        let mut runtime = SymbolRuntime::new(symbol());
        let mut output = InMemoryJournalOutputAppender::new();

        assert_eq!(
            runtime.process_entry(limit_entry(1, 10, 100, Side::Sell, 100, 1), &mut output),
            Ok(())
        );

        let snapshot = runtime.snapshot().expect("snapshot requires a safe point");
        let mut restored = SymbolRuntime::restore_from_snapshot(snapshot);
        let mut restored_output = InMemoryJournalOutputAppender::new();

        assert_eq!(
            restored.process_entry(
                limit_entry(2, 11, 100, Side::Buy, 100, 1),
                &mut restored_output
            ),
            Ok(())
        );

        let entries = restored_output.read_all();
        assert_eq!(entries.len(), 1);
        assert_eq!(
            entries[0].events,
            vec![EngineEvent::OrderAck(OrderAck::Rejected {
                command_id: CommandId(11),
                order_id: Some(OrderId(100)),
                journal_seq: JournalSeq(2),
                reason: RejectReason::DuplicateOrderId,
            })]
        );
    }

    #[test]
    fn restored_runtime_rejects_command_id_seen_before_snapshot() {
        let mut runtime = SymbolRuntime::new(symbol());
        let mut output = InMemoryJournalOutputAppender::new();

        assert_eq!(
            runtime.process_entry(limit_entry(1, 10, 100, Side::Sell, 100, 1), &mut output),
            Ok(())
        );

        let snapshot = runtime.snapshot().expect("snapshot requires a safe point");
        let mut restored = SymbolRuntime::restore_from_snapshot(snapshot);
        let mut restored_output = InMemoryJournalOutputAppender::new();

        assert_eq!(
            restored.process_entry(
                limit_entry(2, 10, 101, Side::Buy, 100, 1),
                &mut restored_output
            ),
            Ok(())
        );

        let entries = restored_output.read_all();
        assert_eq!(entries.len(), 1);
        assert_eq!(
            entries[0].events,
            vec![EngineEvent::OrderAck(OrderAck::Rejected {
                command_id: CommandId(10),
                order_id: Some(OrderId(101)),
                journal_seq: JournalSeq(2),
                reason: RejectReason::DuplicateCommandId,
            })]
        );
    }
}
