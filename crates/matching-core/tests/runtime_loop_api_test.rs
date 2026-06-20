use matching_core::bounded_handoff::BoundedHandoff;
use matching_core::journal_adapter::{
    JournalAdapterError, JournalInputEntry, JournalInputReader, JournalOutputAppender,
    JournalOutputCommitMetadata, JournalOutputEntry,
};
use matching_core::matching_engine::EngineEvent;
use matching_core::order::{Command, Order};
use matching_core::output_commit_boundary::{
    OutputBatchQueryStatus, OutputCommitBlockAction, OutputCommitOutcome, OutputJournalClient,
};
use matching_core::replay_runner::{ReplayResult, ReplayRunner};
use matching_core::runtime_loop::{RuntimeLoop, RuntimeLoopError, RuntimeLoopTickLimits};
use matching_core::runtime_manager::RuntimeManager;
use matching_core::types::{CommandId, JournalSeq, OrderId, Price, Quantity, Side, Symbol};
use std::collections::HashMap;

struct RejectOneSymbolJournalOutputAppender {
    rejected_symbol: Symbol,
    entries: Vec<JournalOutputEntry>,
}

struct AcceptingJournalOutputAppender {
    entries: Vec<JournalOutputEntry>,
}

struct DurableUnknownJournalOutputAppender {
    entries: Vec<JournalOutputEntry>,
}

impl AcceptingJournalOutputAppender {
    fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }
}

impl DurableUnknownJournalOutputAppender {
    fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }
}

impl JournalOutputAppender for DurableUnknownJournalOutputAppender {
    fn append(
        &mut self,
        _command_id: CommandId,
        _journal_seq: JournalSeq,
        _events: Vec<EngineEvent>,
    ) -> Result<(), JournalAdapterError> {
        Err(JournalAdapterError::CommitOutcomeUnknown)
    }

    fn append_with_output_commit_metadata(
        &mut self,
        command_id: CommandId,
        journal_seq: JournalSeq,
        events: Vec<EngineEvent>,
        metadata: JournalOutputCommitMetadata,
    ) -> Result<(), JournalAdapterError> {
        self.entries.push(JournalOutputEntry {
            command_id,
            journal_seq,
            events,
            output_commit_metadata: Some(metadata),
        });

        Err(JournalAdapterError::CommitOutcomeUnknown)
    }

    fn read_all(&self) -> Vec<JournalOutputEntry> {
        self.entries.clone()
    }
}

impl JournalOutputAppender for AcceptingJournalOutputAppender {
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

    fn append_with_output_commit_metadata(
        &mut self,
        command_id: CommandId,
        journal_seq: JournalSeq,
        events: Vec<EngineEvent>,
        metadata: JournalOutputCommitMetadata,
    ) -> Result<(), JournalAdapterError> {
        self.entries.push(JournalOutputEntry {
            command_id,
            journal_seq,
            events,
            output_commit_metadata: Some(metadata),
        });

        Ok(())
    }

    fn read_all(&self) -> Vec<JournalOutputEntry> {
        self.entries.clone()
    }
}

impl RejectOneSymbolJournalOutputAppender {
    fn new(rejected_symbol: Symbol) -> Self {
        Self {
            rejected_symbol,
            entries: Vec::new(),
        }
    }
}

impl JournalOutputAppender for RejectOneSymbolJournalOutputAppender {
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

    fn append_with_output_commit_metadata(
        &mut self,
        command_id: CommandId,
        journal_seq: JournalSeq,
        events: Vec<EngineEvent>,
        metadata: JournalOutputCommitMetadata,
    ) -> Result<(), JournalAdapterError> {
        if metadata.symbol == self.rejected_symbol {
            return Err(JournalAdapterError::AppendRejected);
        }

        self.entries.push(JournalOutputEntry {
            command_id,
            journal_seq,
            events,
            output_commit_metadata: Some(metadata),
        });

        Ok(())
    }

    fn read_all(&self) -> Vec<JournalOutputEntry> {
        self.entries.clone()
    }
}

fn command_entry(seq: u64, symbol: Symbol) -> JournalInputEntry {
    JournalInputEntry {
        seq: JournalSeq(seq),
        command_id: CommandId(seq),
        command: Command::PlaceLimit(Order {
            order_id: OrderId(seq),
            symbol,
            side: Side::Buy,
            price: Price(100),
            quantity: Quantity(1),
        }),
    }
}

fn limit_entry(
    seq: u64,
    command_id: u64,
    order_id: u64,
    symbol: Symbol,
    side: Side,
    price: u64,
    quantity: u64,
) -> JournalInputEntry {
    JournalInputEntry {
        seq: JournalSeq(seq),
        command_id: CommandId(command_id),
        command: Command::PlaceLimit(Order {
            order_id: OrderId(order_id),
            symbol,
            side,
            price: Price(price),
            quantity: Quantity(quantity),
        }),
    }
}

fn cancel_entry(seq: u64, command_id: u64, order_id: u64, symbol: Symbol) -> JournalInputEntry {
    JournalInputEntry {
        seq: JournalSeq(seq),
        command_id: CommandId(command_id),
        command: Command::Cancel {
            order_id: OrderId(order_id),
            symbol,
        },
    }
}

#[derive(Clone)]
struct RuntimeLoopReplayJournal {
    entries: Vec<JournalInputEntry>,
}

impl RuntimeLoopReplayJournal {
    fn for_symbol(entries: &[JournalInputEntry], symbol: &Symbol) -> Self {
        Self {
            entries: entries
                .iter()
                .filter(|entry| entry.command.symbol() == symbol)
                .cloned()
                .collect(),
        }
    }
}

impl JournalInputReader for RuntimeLoopReplayJournal {
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

fn normalized_output_for_symbol(
    output: &dyn JournalOutputAppender,
    symbol: &Symbol,
) -> Vec<JournalOutputEntry> {
    output
        .read_all()
        .into_iter()
        .filter(|entry| {
            entry
                .output_commit_metadata
                .as_ref()
                .map(|metadata| &metadata.symbol == symbol)
                .unwrap_or(false)
        })
        .map(|mut entry| {
            entry.output_commit_metadata = None;
            entry
        })
        .collect()
}

#[test]
fn runtime_loop_enqueue_input_routes_entry_to_symbol_handoff() {
    let btc = Symbol("BTC-USDT".to_string());
    let eth = Symbol("ETH-USDT".to_string());
    let mut manager = RuntimeManager::new();
    let mut handoffs = HashMap::new();

    manager.add_symbol(btc.clone());
    manager.add_symbol(eth.clone());
    handoffs.insert(btc.clone(), BoundedHandoff::new(4));
    handoffs.insert(eth.clone(), BoundedHandoff::new(4));

    let mut runtime_loop = RuntimeLoop::new(manager, handoffs);

    assert_eq!(
        runtime_loop.enqueue_input(command_entry(1, eth.clone())),
        Ok(())
    );
    assert_eq!(runtime_loop.pending_input_len(&btc), Some(0));
    assert_eq!(runtime_loop.pending_input_len(&eth), Some(1));
    assert_eq!(
        runtime_loop.pending_input_status(&eth),
        Some(matching_core::runtime_loop::RuntimeLoopInputStatus {
            len: 1,
            capacity: 4,
            full: false,
        })
    );
}

#[test]
fn runtime_loop_enqueue_input_rejects_entry_without_symbol_handoff() {
    let btc = Symbol("BTC-USDT".to_string());
    let eth = Symbol("ETH-USDT".to_string());
    let mut manager = RuntimeManager::new();
    let mut handoffs = HashMap::new();

    manager.add_symbol(btc.clone());
    manager.add_symbol(eth.clone());
    handoffs.insert(btc.clone(), BoundedHandoff::new(4));

    let mut runtime_loop = RuntimeLoop::new(manager, handoffs);

    assert_eq!(
        runtime_loop.enqueue_input(command_entry(1, eth.clone())),
        Err(RuntimeLoopError::MissingHandoff(eth.clone()))
    );
    assert_eq!(runtime_loop.pending_input_len(&btc), Some(0));
}

#[test]
fn runtime_loop_enqueue_input_rejects_entry_for_unregistered_handoff_symbol() {
    let btc = Symbol("BTC-USDT".to_string());
    let eth = Symbol("ETH-USDT".to_string());
    let mut manager = RuntimeManager::new();
    let mut handoffs = HashMap::new();

    manager.add_symbol(btc.clone());
    handoffs.insert(btc.clone(), BoundedHandoff::new(4));
    handoffs.insert(eth.clone(), BoundedHandoff::new(4));

    let mut runtime_loop = RuntimeLoop::new(manager, handoffs);

    assert_eq!(
        runtime_loop.enqueue_input(command_entry(1, eth.clone())),
        Err(RuntimeLoopError::UnregisteredHandoff(eth.clone()))
    );
    assert_eq!(runtime_loop.pending_input_len(&btc), Some(0));
    assert_eq!(runtime_loop.pending_input_len(&eth), Some(0));
}

#[test]
fn runtime_loop_enqueue_input_reports_full_symbol_handoff() {
    let btc = Symbol("BTC-USDT".to_string());
    let mut manager = RuntimeManager::new();
    let mut handoffs = HashMap::new();

    manager.add_symbol(btc.clone());
    handoffs.insert(btc.clone(), BoundedHandoff::new(1));

    let mut runtime_loop = RuntimeLoop::new(manager, handoffs);

    assert_eq!(
        runtime_loop.enqueue_input(command_entry(1, btc.clone())),
        Ok(())
    );
    assert_eq!(
        runtime_loop.enqueue_input(command_entry(2, btc.clone())),
        Err(RuntimeLoopError::InputHandoffFull(btc.clone()))
    );
    assert_eq!(runtime_loop.pending_input_len(&btc), Some(1));
}

#[test]
fn runtime_loop_enqueue_inputs_routes_batch_to_symbol_handoffs() {
    let btc = Symbol("BTC-USDT".to_string());
    let eth = Symbol("ETH-USDT".to_string());
    let mut runtime_loop = RuntimeLoop::new_for_symbols(vec![btc.clone(), eth.clone()], 4, 8);

    assert_eq!(
        runtime_loop.enqueue_inputs(vec![
            command_entry(1, btc.clone()),
            command_entry(2, eth.clone()),
            command_entry(3, btc.clone()),
        ]),
        Ok(3)
    );
    assert_eq!(runtime_loop.pending_input_len(&btc), Some(2));
    assert_eq!(runtime_loop.pending_input_len(&eth), Some(1));
}

#[test]
fn runtime_loop_enqueue_inputs_rejects_batch_without_partial_enqueue_when_symbol_handoff_would_fill(
) {
    let btc = Symbol("BTC-USDT".to_string());
    let mut runtime_loop = RuntimeLoop::new_for_symbols(vec![btc.clone()], 1, 8);

    assert_eq!(
        runtime_loop.enqueue_inputs(vec![
            command_entry(1, btc.clone()),
            command_entry(2, btc.clone()),
        ]),
        Err(RuntimeLoopError::InputHandoffFull(btc.clone()))
    );
    assert_eq!(runtime_loop.pending_input_len(&btc), Some(0));
}

#[test]
fn runtime_loop_enqueue_inputs_rejects_batch_without_partial_enqueue_when_symbol_is_unregistered() {
    let btc = Symbol("BTC-USDT".to_string());
    let eth = Symbol("ETH-USDT".to_string());
    let mut manager = RuntimeManager::new();
    let mut handoffs = HashMap::new();

    manager.add_symbol(btc.clone());
    handoffs.insert(btc.clone(), BoundedHandoff::new(4));
    handoffs.insert(eth.clone(), BoundedHandoff::new(4));

    let mut runtime_loop = RuntimeLoop::new(manager, handoffs);

    assert_eq!(
        runtime_loop.enqueue_inputs(vec![
            command_entry(1, btc.clone()),
            command_entry(2, eth.clone()),
        ]),
        Err(RuntimeLoopError::UnregisteredHandoff(eth.clone()))
    );
    assert_eq!(runtime_loop.pending_input_len(&btc), Some(0));
    assert_eq!(runtime_loop.pending_input_len(&eth), Some(0));
}

#[test]
fn runtime_loop_enqueue_inputs_rejects_batch_without_partial_enqueue_when_symbol_handoff_is_missing(
) {
    let btc = Symbol("BTC-USDT".to_string());
    let eth = Symbol("ETH-USDT".to_string());
    let mut manager = RuntimeManager::new();
    let mut handoffs = HashMap::new();

    manager.add_symbol(btc.clone());
    manager.add_symbol(eth.clone());
    handoffs.insert(btc.clone(), BoundedHandoff::new(4));

    let mut runtime_loop = RuntimeLoop::new(manager, handoffs);

    assert_eq!(
        runtime_loop.enqueue_inputs(vec![
            command_entry(1, btc.clone()),
            command_entry(2, eth.clone()),
        ]),
        Err(RuntimeLoopError::MissingHandoff(eth.clone()))
    );
    assert_eq!(runtime_loop.pending_input_len(&btc), Some(0));
}

#[test]
fn runtime_loop_enqueue_inputs_reports_full_handoff_in_deterministic_order() {
    let ada = Symbol("ADA-USDT".to_string());
    let btc = Symbol("BTC-USDT".to_string());
    let mut runtime_loop = RuntimeLoop::new_for_symbols(vec![btc.clone(), ada.clone()], 1, 8);

    assert_eq!(
        runtime_loop.enqueue_inputs(vec![
            command_entry(1, btc.clone()),
            command_entry(2, btc.clone()),
            command_entry(3, ada.clone()),
            command_entry(4, ada.clone()),
        ]),
        Err(RuntimeLoopError::InputHandoffFull(ada.clone()))
    );
    assert_eq!(runtime_loop.pending_input_len(&ada), Some(0));
    assert_eq!(runtime_loop.pending_input_len(&btc), Some(0));
}

#[test]
fn runtime_loop_can_be_created_with_registered_symbols_and_handoffs_together() {
    let btc = Symbol("BTC-USDT".to_string());
    let eth = Symbol("ETH-USDT".to_string());
    let mut journal_client = OutputJournalClient::new();
    let mut output = AcceptingJournalOutputAppender::new();
    let mut runtime_loop = RuntimeLoop::new_for_symbols(vec![eth.clone(), btc.clone()], 4, 8);

    assert_eq!(runtime_loop.pending_input_len(&btc), Some(0));
    assert_eq!(runtime_loop.pending_input_len(&eth), Some(0));
    assert!(runtime_loop.symbol_status(&btc).is_some());
    assert!(runtime_loop.symbol_status(&eth).is_some());
    assert_eq!(
        runtime_loop.enqueue_inputs(vec![
            command_entry(1, btc.clone()),
            command_entry(1, eth.clone()),
        ]),
        Ok(2)
    );

    let report = runtime_loop
        .run_tick(
            &mut journal_client,
            &mut output,
            RuntimeLoopTickLimits {
                max_input_entries_per_symbol: 1,
                max_output_requests_per_symbol: 10,
            },
        )
        .expect("assembled runtime loop should process registered symbols");
    let report_symbols: Vec<Symbol> = report
        .symbol_reports
        .iter()
        .map(|symbol_report| symbol_report.symbol.clone())
        .collect();

    assert_eq!(report_symbols, vec![btc.clone(), eth.clone()]);
    assert_eq!(runtime_loop.last_input_seq(&btc), Some(Some(JournalSeq(1))));
    assert_eq!(runtime_loop.last_input_seq(&eth), Some(Some(JournalSeq(1))));
}

#[test]
fn runtime_loop_live_path_matches_replay_output_checksum_and_safe_point_per_symbol() {
    let btc = Symbol("BTC-USDT".to_string());
    let eth = Symbol("ETH-USDT".to_string());
    let entries = vec![
        limit_entry(1, 10, 100, btc.clone(), Side::Sell, 100, 1),
        limit_entry(2, 20, 200, eth.clone(), Side::Buy, 50, 1),
        limit_entry(3, 11, 101, btc.clone(), Side::Buy, 100, 1),
        cancel_entry(4, 21, 200, eth.clone()),
    ];
    let mut runtime_loop = RuntimeLoop::new_for_symbols(vec![eth.clone(), btc.clone()], 8, 8);
    let mut journal_client = OutputJournalClient::new();
    let mut output = AcceptingJournalOutputAppender::new();

    assert_eq!(
        runtime_loop.enqueue_inputs(entries.clone()),
        Ok(entries.len())
    );

    let report = runtime_loop
        .run_tick(
            &mut journal_client,
            &mut output,
            RuntimeLoopTickLimits {
                max_input_entries_per_symbol: 8,
                max_output_requests_per_symbol: 8,
            },
        )
        .expect("runtime loop should process all queued inputs");

    assert!(!report.has_work_remaining());

    for symbol in [btc, eth] {
        let replay_journal = RuntimeLoopReplayJournal::for_symbol(&entries, &symbol);
        let replay_result = ReplayRunner::new(symbol.clone()).replay_result(&replay_journal);
        let live_result = ReplayResult {
            checksum: runtime_loop
                .checksum(&symbol)
                .expect("runtime loop should expose registered symbol checksum"),
            last_replayed_seq: runtime_loop
                .last_input_seq(&symbol)
                .expect("runtime loop should expose registered symbol safe point"),
            output_entries: normalized_output_for_symbol(&output, &symbol),
        };

        assert!(live_result.compare_with(&replay_result).is_match());
    }
}

#[test]
fn runtime_loop_can_validate_configuration_before_running_tick() {
    let btc = Symbol("BTC-USDT".to_string());
    let eth = Symbol("ETH-USDT".to_string());
    let runtime_loop = RuntimeLoop::new_for_symbols(vec![btc.clone(), eth.clone()], 4, 8);

    assert_eq!(runtime_loop.validate_configuration(), Ok(()));
}

#[test]
fn runtime_loop_tick_report_identifies_idle_tick() {
    let btc = Symbol("BTC-USDT".to_string());
    let mut journal_client = OutputJournalClient::new();
    let mut output = AcceptingJournalOutputAppender::new();
    let mut runtime_loop = RuntimeLoop::new_for_symbols(vec![btc.clone()], 4, 8);

    let report = runtime_loop
        .run_tick(
            &mut journal_client,
            &mut output,
            RuntimeLoopTickLimits {
                max_input_entries_per_symbol: 1,
                max_output_requests_per_symbol: 10,
            },
        )
        .expect("empty runtime loop tick should produce an idle report");
    let btc_report = report
        .symbol_report(&btc)
        .expect("btc should have an idle tick report");

    assert!(!report.made_progress());
    assert!(!report.has_work_remaining());
    assert!(!report.has_blocked_symbols());
    assert!(report.is_idle());
    assert_eq!(btc_report.input_processed_count, 0);
    assert_eq!(btc_report.safe_point_advanced_count, 0);
    assert_eq!(btc_report.pending_input_len_after_tick, 0);
    assert_eq!(btc_report.runtime_status_after_tick.pending_output_len, 0);
    assert_eq!(runtime_loop.last_input_seq(&btc), Some(None));
    assert_eq!(output.read_all().len(), 0);
}

#[test]
fn runtime_loop_validate_configuration_reports_missing_handoff_before_tick() {
    let btc = Symbol("BTC-USDT".to_string());
    let eth = Symbol("ETH-USDT".to_string());
    let mut manager = RuntimeManager::new();
    let mut handoffs = HashMap::new();

    manager.add_symbol(btc.clone());
    manager.add_symbol(eth.clone());
    handoffs.insert(btc.clone(), BoundedHandoff::new(4));

    let runtime_loop = RuntimeLoop::new(manager, handoffs);

    assert_eq!(
        runtime_loop.validate_configuration(),
        Err(RuntimeLoopError::MissingHandoff(eth.clone()))
    );
}

#[test]
fn runtime_loop_validate_configuration_reports_unregistered_handoff_in_deterministic_order() {
    let ada = Symbol("ADA-USDT".to_string());
    let btc = Symbol("BTC-USDT".to_string());
    let sol = Symbol("SOL-USDT".to_string());
    let xrp = Symbol("XRP-USDT".to_string());
    let mut manager = RuntimeManager::new();
    let mut handoffs = HashMap::new();

    manager.add_symbol(btc.clone());
    handoffs.insert(btc.clone(), BoundedHandoff::new(4));
    handoffs.insert(xrp.clone(), BoundedHandoff::new(4));
    handoffs.insert(sol.clone(), BoundedHandoff::new(4));
    handoffs.insert(ada.clone(), BoundedHandoff::new(4));

    let runtime_loop = RuntimeLoop::new(manager, handoffs);

    assert_eq!(
        runtime_loop.validate_configuration(),
        Err(RuntimeLoopError::UnregisteredHandoff(ada.clone()))
    );
}

#[test]
fn runtime_loop_tick_keeps_unblocked_symbol_running_when_one_symbol_output_blocks() {
    let btc = Symbol("BTC-USDT".to_string());
    let eth = Symbol("ETH-USDT".to_string());
    let mut manager = RuntimeManager::new();
    let mut handoffs = HashMap::new();
    let mut journal_client = OutputJournalClient::new();
    let mut output = RejectOneSymbolJournalOutputAppender::new(btc.clone());

    manager.add_symbol(btc.clone());
    manager.add_symbol(eth.clone());
    handoffs.insert(btc.clone(), BoundedHandoff::new(4));
    handoffs.insert(eth.clone(), BoundedHandoff::new(4));
    handoffs
        .get_mut(&btc)
        .expect("btc handoff should exist")
        .enqueue(command_entry(1, btc.clone()))
        .expect("btc command should enqueue");
    handoffs
        .get_mut(&eth)
        .expect("eth handoff should exist")
        .enqueue(command_entry(1, eth.clone()))
        .expect("eth command should enqueue");

    let mut runtime_loop = RuntimeLoop::new(manager, handoffs);
    let report = runtime_loop
        .run_tick(
            &mut journal_client,
            &mut output,
            RuntimeLoopTickLimits {
                max_input_entries_per_symbol: 1,
                max_output_requests_per_symbol: 10,
            },
        )
        .expect(
            "runtime tick should report symbol-local output blockage without stopping all symbols",
        );

    let btc_report = report
        .symbol_report(&btc)
        .expect("btc should have a tick report");
    let eth_report = report
        .symbol_report(&eth)
        .expect("eth should have a tick report");

    assert!(report.made_progress());
    assert!(report.has_work_remaining());
    assert!(report.has_blocked_symbols());
    assert_eq!(btc_report.input_processed_count, 1);
    assert_eq!(btc_report.safe_point_advanced_count, 0);
    assert_eq!(btc_report.blocking_seq, Some(JournalSeq(1)));
    assert_eq!(
        btc_report.blocking_outcome,
        Some(OutputCommitOutcome::Rejected)
    );
    assert_eq!(
        btc_report
            .block_decision
            .expect("btc should be blocked")
            .action,
        OutputCommitBlockAction::StopAndEscalate
    );
    assert_eq!(eth_report.input_processed_count, 1);
    assert_eq!(eth_report.safe_point_advanced_count, 1);

    assert_eq!(runtime_loop.last_input_seq(&btc), Some(None));
    assert_eq!(runtime_loop.last_input_seq(&eth), Some(Some(JournalSeq(1))));
    let btc_status = runtime_loop
        .symbol_status(&btc)
        .expect("btc status should exist");
    assert_eq!(btc_status.pending_output_len, 1);
    assert_eq!(
        btc_status.output_commit_escalation,
        btc_report.block_decision
    );
    assert!(btc_status.output_commit_blockage.is_some());
    assert_eq!(runtime_loop.pending_input_len(&eth), Some(0));
    assert_eq!(output.read_all().len(), 1);
}

#[test]
fn runtime_loop_tick_prioritizes_output_commit_when_pending_output_is_full() {
    let btc = Symbol("BTC-USDT".to_string());
    let mut manager = RuntimeManager::new_with_pending_output_capacity(1);
    let mut handoffs = HashMap::new();
    let mut journal_client = OutputJournalClient::new();
    let mut output = AcceptingJournalOutputAppender::new();

    manager.add_symbol(btc.clone());
    handoffs.insert(btc.clone(), BoundedHandoff::new(4));
    handoffs
        .get_mut(&btc)
        .expect("btc handoff should exist")
        .enqueue(command_entry(1, btc.clone()))
        .expect("first btc command should enqueue");

    let mut runtime_loop = RuntimeLoop::new(manager, handoffs);
    let first_report = runtime_loop
        .run_tick(
            &mut journal_client,
            &mut output,
            RuntimeLoopTickLimits {
                max_input_entries_per_symbol: 1,
                max_output_requests_per_symbol: 0,
            },
        )
        .expect("first tick should fill pending output without committing it");
    let first_btc_report = first_report
        .symbol_report(&btc)
        .expect("btc should have a first tick report");

    assert!(first_report.made_progress());
    assert!(first_report.has_work_remaining());
    assert!(!first_report.has_blocked_symbols());
    assert_eq!(first_btc_report.input_processed_count, 1);
    assert_eq!(first_btc_report.safe_point_advanced_count, 0);
    assert_eq!(runtime_loop.last_input_seq(&btc), Some(None));
    assert_eq!(
        runtime_loop
            .symbol_status(&btc)
            .expect("btc status should exist")
            .pending_output_len,
        1
    );
    assert_eq!(output.read_all().len(), 0);

    runtime_loop
        .enqueue_input(command_entry(2, btc.clone()))
        .expect("second btc command should enqueue");

    let second_report = runtime_loop
        .run_tick(
            &mut journal_client,
            &mut output,
            RuntimeLoopTickLimits {
                max_input_entries_per_symbol: 1,
                max_output_requests_per_symbol: 10,
            },
        )
        .expect("second tick should commit full pending output before draining new input");
    let second_btc_report = second_report
        .symbol_report(&btc)
        .expect("btc should have a second tick report");

    assert!(second_report.made_progress());
    assert!(second_report.has_work_remaining());
    assert!(!second_report.has_blocked_symbols());
    assert_eq!(second_btc_report.input_processed_count, 0);
    assert_eq!(second_btc_report.safe_point_advanced_count, 1);
    assert_eq!(
        second_btc_report.runtime_status_after_tick.last_input_seq,
        Some(JournalSeq(1))
    );
    assert_eq!(
        second_btc_report
            .runtime_status_after_tick
            .pending_output_len,
        0
    );
    assert!(
        !second_btc_report
            .runtime_status_after_tick
            .pending_output_full
    );
    assert_eq!(second_btc_report.pending_input_len_after_tick, 1);
    assert_eq!(second_btc_report.pending_input_capacity, 4);
    assert!(!second_btc_report.pending_input_full);
    assert_eq!(runtime_loop.last_input_seq(&btc), Some(Some(JournalSeq(1))));
    assert_eq!(runtime_loop.pending_input_len(&btc), Some(1));
    assert_eq!(
        runtime_loop
            .symbol_status(&btc)
            .expect("btc status should exist")
            .pending_output_len,
        0
    );
    assert_eq!(output.read_all().len(), 1);
}

#[test]
fn runtime_loop_tick_reports_output_batch_identity_and_query_status() {
    let btc = Symbol("BTC-USDT".to_string());
    let mut manager = RuntimeManager::new();
    let mut handoffs = HashMap::new();
    let mut journal_client = OutputJournalClient::new();
    let mut output = DurableUnknownJournalOutputAppender::new();

    manager.add_symbol(btc.clone());
    handoffs.insert(btc.clone(), BoundedHandoff::new(4));
    handoffs
        .get_mut(&btc)
        .expect("btc handoff should exist")
        .enqueue(command_entry(1, btc.clone()))
        .expect("btc command should enqueue");

    let mut runtime_loop = RuntimeLoop::new(manager, handoffs);
    let report = runtime_loop
        .run_tick(
            &mut journal_client,
            &mut output,
            RuntimeLoopTickLimits {
                max_input_entries_per_symbol: 1,
                max_output_requests_per_symbol: 10,
            },
        )
        .expect("runtime tick should report durable output batch status");
    let btc_report = report
        .symbol_report(&btc)
        .expect("btc should have a tick report");
    let output_batch_identity = btc_report
        .output_batch_identity
        .as_ref()
        .expect("runtime tick should expose attempted output batch identity");

    assert_eq!(output_batch_identity.symbol, btc.clone());
    assert_eq!(output_batch_identity.input_seq_start, JournalSeq(1));
    assert_eq!(output_batch_identity.input_seq_end, JournalSeq(1));
    assert_eq!(output_batch_identity.entry_count, 1);
    assert_eq!(
        btc_report.output_batch_query_status,
        Some(OutputBatchQueryStatus::Durable)
    );
    assert_eq!(btc_report.input_processed_count, 1);
    assert_eq!(btc_report.safe_point_advanced_count, 1);
    assert_eq!(btc_report.block_decision, None);
    assert_eq!(runtime_loop.last_input_seq(&btc), Some(Some(JournalSeq(1))));
}

#[test]
fn runtime_loop_tick_fails_before_processing_when_a_registered_symbol_has_no_handoff() {
    let btc = Symbol("BTC-USDT".to_string());
    let eth = Symbol("ETH-USDT".to_string());
    let mut manager = RuntimeManager::new();
    let mut handoffs = HashMap::new();
    let mut journal_client = OutputJournalClient::new();
    let mut output = AcceptingJournalOutputAppender::new();

    manager.add_symbol(btc.clone());
    manager.add_symbol(eth.clone());
    handoffs.insert(btc.clone(), BoundedHandoff::new(4));
    handoffs
        .get_mut(&btc)
        .expect("btc handoff should exist")
        .enqueue(command_entry(1, btc.clone()))
        .expect("btc command should enqueue");

    let mut runtime_loop = RuntimeLoop::new(manager, handoffs);
    let result = runtime_loop.run_tick(
        &mut journal_client,
        &mut output,
        RuntimeLoopTickLimits {
            max_input_entries_per_symbol: 1,
            max_output_requests_per_symbol: 10,
        },
    );

    assert_eq!(result, Err(RuntimeLoopError::MissingHandoff(eth.clone())));
    assert_eq!(output.read_all().len(), 0);
    assert_eq!(runtime_loop.last_input_seq(&btc), Some(None));
    assert_eq!(
        runtime_loop
            .symbol_status(&btc)
            .expect("btc status should exist")
            .pending_output_len,
        0
    );
}

#[test]
fn runtime_loop_tick_fails_before_processing_when_handoff_has_unregistered_symbol() {
    let btc = Symbol("BTC-USDT".to_string());
    let eth = Symbol("ETH-USDT".to_string());
    let mut manager = RuntimeManager::new();
    let mut handoffs = HashMap::new();
    let mut journal_client = OutputJournalClient::new();
    let mut output = AcceptingJournalOutputAppender::new();

    manager.add_symbol(btc.clone());
    handoffs.insert(btc.clone(), BoundedHandoff::new(4));
    handoffs.insert(eth.clone(), BoundedHandoff::new(4));
    handoffs
        .get_mut(&btc)
        .expect("btc handoff should exist")
        .enqueue(command_entry(1, btc.clone()))
        .expect("btc command should enqueue");
    handoffs
        .get_mut(&eth)
        .expect("eth handoff should exist")
        .enqueue(command_entry(1, eth.clone()))
        .expect("eth command should enqueue");

    let mut runtime_loop = RuntimeLoop::new(manager, handoffs);
    let result = runtime_loop.run_tick(
        &mut journal_client,
        &mut output,
        RuntimeLoopTickLimits {
            max_input_entries_per_symbol: 1,
            max_output_requests_per_symbol: 10,
        },
    );

    assert_eq!(
        result,
        Err(RuntimeLoopError::UnregisteredHandoff(eth.clone()))
    );
    assert_eq!(output.read_all().len(), 0);
    assert_eq!(runtime_loop.last_input_seq(&btc), Some(None));
    assert_eq!(
        runtime_loop
            .symbol_status(&btc)
            .expect("btc status should exist")
            .pending_output_len,
        0
    );
}

#[test]
fn runtime_loop_tick_keeps_unblocked_symbol_running_when_one_symbol_is_quarantined() {
    let btc = Symbol("BTC-USDT".to_string());
    let eth = Symbol("ETH-USDT".to_string());
    let mut manager = RuntimeManager::new();
    let mut handoffs = HashMap::new();
    let mut journal_client = OutputJournalClient::new();
    let mut output = RejectOneSymbolJournalOutputAppender::new(btc.clone());

    manager.add_symbol(btc.clone());
    manager.add_symbol(eth.clone());
    handoffs.insert(btc.clone(), BoundedHandoff::new(4));
    handoffs.insert(eth.clone(), BoundedHandoff::new(4));
    handoffs
        .get_mut(&btc)
        .expect("btc handoff should exist")
        .enqueue(command_entry(1, btc.clone()))
        .expect("btc command should enqueue");

    let mut runtime_loop = RuntimeLoop::new(manager, handoffs);
    let first_report = runtime_loop
        .run_tick(
            &mut journal_client,
            &mut output,
            RuntimeLoopTickLimits {
                max_input_entries_per_symbol: 1,
                max_output_requests_per_symbol: 10,
            },
        )
        .expect("first tick should create a btc escalation");
    let btc_decision = first_report
        .symbol_report(&btc)
        .expect("btc should have a first tick report")
        .block_decision
        .expect("btc should be escalated");

    assert_eq!(
        runtime_loop.quarantine_symbol_output_commit_escalation(&btc),
        Ok(Some(btc_decision))
    );
    runtime_loop
        .enqueue_input(command_entry(2, btc.clone()))
        .expect("second btc command should enqueue");
    runtime_loop
        .enqueue_input(command_entry(1, eth.clone()))
        .expect("eth command should enqueue");

    let second_report = runtime_loop
        .run_tick(
            &mut journal_client,
            &mut output,
            RuntimeLoopTickLimits {
                max_input_entries_per_symbol: 1,
                max_output_requests_per_symbol: 10,
            },
        )
        .expect("second tick should keep quarantined btc paused and continue eth");
    let paused_btc_report = second_report
        .symbol_report(&btc)
        .expect("btc should have a paused tick report");
    let eth_report = second_report
        .symbol_report(&eth)
        .expect("eth should have a tick report");

    assert_eq!(paused_btc_report.input_processed_count, 0);
    assert_eq!(paused_btc_report.safe_point_advanced_count, 0);
    assert_eq!(paused_btc_report.block_decision, Some(btc_decision));
    assert_eq!(eth_report.input_processed_count, 1);
    assert_eq!(eth_report.safe_point_advanced_count, 1);
    assert_eq!(runtime_loop.pending_input_len(&btc), Some(1));
    assert_eq!(runtime_loop.pending_input_len(&eth), Some(0));
    assert_eq!(runtime_loop.last_input_seq(&btc), Some(None));
    assert_eq!(runtime_loop.last_input_seq(&eth), Some(Some(JournalSeq(1))));
    assert!(runtime_loop
        .symbol_status(&btc)
        .expect("btc status should exist")
        .output_commit_quarantine
        .is_some());
}

#[test]
fn runtime_loop_can_clear_quarantine_and_retry_pending_output() {
    let btc = Symbol("BTC-USDT".to_string());
    let mut manager = RuntimeManager::new();
    let mut handoffs = HashMap::new();
    let mut journal_client = OutputJournalClient::new();
    let mut rejecting_output = RejectOneSymbolJournalOutputAppender::new(btc.clone());

    manager.add_symbol(btc.clone());
    handoffs.insert(btc.clone(), BoundedHandoff::new(4));
    handoffs
        .get_mut(&btc)
        .expect("btc handoff should exist")
        .enqueue(command_entry(1, btc.clone()))
        .expect("btc command should enqueue");

    let mut runtime_loop = RuntimeLoop::new(manager, handoffs);
    let blocked_report = runtime_loop
        .run_tick(
            &mut journal_client,
            &mut rejecting_output,
            RuntimeLoopTickLimits {
                max_input_entries_per_symbol: 1,
                max_output_requests_per_symbol: 10,
            },
        )
        .expect("first tick should create a btc escalation");
    let decision = blocked_report
        .symbol_report(&btc)
        .expect("btc should have a blocked report")
        .block_decision
        .expect("btc should be escalated");

    assert_eq!(
        runtime_loop.quarantine_symbol_output_commit_escalation(&btc),
        Ok(Some(decision))
    );
    assert_eq!(
        runtime_loop.clear_symbol_output_commit_quarantine(&btc),
        Ok(Some(decision))
    );

    let mut accepting_output = AcceptingJournalOutputAppender::new();
    let retry_report = runtime_loop
        .run_tick(
            &mut journal_client,
            &mut accepting_output,
            RuntimeLoopTickLimits {
                max_input_entries_per_symbol: 1,
                max_output_requests_per_symbol: 10,
            },
        )
        .expect("cleared quarantine should allow pending output retry");
    let btc_report = retry_report
        .symbol_report(&btc)
        .expect("btc should have a retry report");

    assert_eq!(btc_report.input_processed_count, 0);
    assert_eq!(btc_report.safe_point_advanced_count, 1);
    assert_eq!(btc_report.block_decision, None);
    assert_eq!(runtime_loop.last_input_seq(&btc), Some(Some(JournalSeq(1))));
    assert_eq!(
        runtime_loop
            .symbol_status(&btc)
            .expect("btc status should exist")
            .pending_output_len,
        0
    );
    assert_eq!(accepting_output.read_all().len(), 1);
}

#[test]
fn runtime_loop_clear_quarantine_does_not_drop_pending_output_when_retry_fails() {
    let btc = Symbol("BTC-USDT".to_string());
    let mut manager = RuntimeManager::new();
    let mut handoffs = HashMap::new();
    let mut journal_client = OutputJournalClient::new();
    let mut rejecting_output = RejectOneSymbolJournalOutputAppender::new(btc.clone());

    manager.add_symbol(btc.clone());
    handoffs.insert(btc.clone(), BoundedHandoff::new(4));
    handoffs
        .get_mut(&btc)
        .expect("btc handoff should exist")
        .enqueue(command_entry(1, btc.clone()))
        .expect("btc command should enqueue");

    let mut runtime_loop = RuntimeLoop::new(manager, handoffs);
    let blocked_report = runtime_loop
        .run_tick(
            &mut journal_client,
            &mut rejecting_output,
            RuntimeLoopTickLimits {
                max_input_entries_per_symbol: 1,
                max_output_requests_per_symbol: 10,
            },
        )
        .expect("first tick should create a btc escalation");
    let first_decision = blocked_report
        .symbol_report(&btc)
        .expect("btc should have a blocked report")
        .block_decision
        .expect("btc should be escalated");

    assert_eq!(
        runtime_loop.quarantine_symbol_output_commit_escalation(&btc),
        Ok(Some(first_decision))
    );
    assert_eq!(
        runtime_loop.clear_symbol_output_commit_quarantine(&btc),
        Ok(Some(first_decision))
    );

    let retry_report = runtime_loop
        .run_tick(
            &mut journal_client,
            &mut rejecting_output,
            RuntimeLoopTickLimits {
                max_input_entries_per_symbol: 1,
                max_output_requests_per_symbol: 10,
            },
        )
        .expect("cleared quarantine should allow retry even if retry fails again");
    let retry_btc_report = retry_report
        .symbol_report(&btc)
        .expect("btc should have a retry report");

    assert_eq!(retry_btc_report.input_processed_count, 0);
    assert_eq!(retry_btc_report.safe_point_advanced_count, 0);
    assert_eq!(
        retry_btc_report
            .block_decision
            .expect("retry should block again")
            .action,
        OutputCommitBlockAction::StopAndEscalate
    );
    assert_eq!(runtime_loop.last_input_seq(&btc), Some(None));
    assert_eq!(
        runtime_loop
            .symbol_status(&btc)
            .expect("btc status should exist")
            .pending_output_len,
        1
    );
    assert!(runtime_loop
        .symbol_status(&btc)
        .expect("btc status should exist")
        .output_commit_escalation
        .is_some());
}

#[test]
fn runtime_loop_tick_reports_symbols_in_deterministic_order() {
    let btc = Symbol("BTC-USDT".to_string());
    let eth = Symbol("ETH-USDT".to_string());
    let mut manager = RuntimeManager::new();
    let mut handoffs = HashMap::new();
    let mut journal_client = OutputJournalClient::new();
    let mut output = AcceptingJournalOutputAppender::new();

    manager.add_symbol(eth.clone());
    manager.add_symbol(btc.clone());
    handoffs.insert(eth.clone(), BoundedHandoff::new(4));
    handoffs.insert(btc.clone(), BoundedHandoff::new(4));
    handoffs
        .get_mut(&eth)
        .expect("eth handoff should exist")
        .enqueue(command_entry(1, eth.clone()))
        .expect("eth command should enqueue");
    handoffs
        .get_mut(&btc)
        .expect("btc handoff should exist")
        .enqueue(command_entry(1, btc.clone()))
        .expect("btc command should enqueue");

    let mut runtime_loop = RuntimeLoop::new(manager, handoffs);
    let report = runtime_loop
        .run_tick(
            &mut journal_client,
            &mut output,
            RuntimeLoopTickLimits {
                max_input_entries_per_symbol: 1,
                max_output_requests_per_symbol: 10,
            },
        )
        .expect("runtime tick should process registered symbols");

    let report_symbols: Vec<Symbol> = report
        .symbol_reports
        .iter()
        .map(|symbol_report| symbol_report.symbol.clone())
        .collect();

    assert!(report.made_progress());
    assert!(!report.has_work_remaining());
    assert!(!report.has_blocked_symbols());
    assert_eq!(report_symbols, vec![btc.clone(), eth.clone()]);
    assert_eq!(runtime_loop.last_input_seq(&btc), Some(Some(JournalSeq(1))));
    assert_eq!(runtime_loop.last_input_seq(&eth), Some(Some(JournalSeq(1))));
}
