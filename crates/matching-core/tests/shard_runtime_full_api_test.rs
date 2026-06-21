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
use matching_core::runtime_config::MatchingRuntimeConfig;
use matching_core::shard_execution_core::ShardExecutionCore;
use matching_core::shard_runtime::{
    ShardRuntime, ShardRuntimeError, ShardRuntimeRunLimit, ShardRuntimeRunOnceLimits,
    ShardRuntimeRunStopReason,
};
use matching_core::snapshot_restore::{OrderBookSnapshot, SymbolRuntimeSnapshot};
use matching_core::snapshot_store::{FileSnapshotStore, InMemorySnapshotStore, SnapshotStore};
use matching_core::types::{CommandId, JournalSeq, OrderId, Price, Quantity, Side, Symbol};
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

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

fn temporary_snapshot_dir(test_name: &str) -> PathBuf {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after unix epoch")
        .as_nanos();
    let path = std::env::temp_dir().join(format!("matching-core-{test_name}-{unique}"));

    fs::create_dir_all(&path).expect("temporary snapshot dir should be created");
    path
}

#[derive(Clone)]
struct ShardRuntimeReplayJournal {
    entries: Vec<JournalInputEntry>,
}

impl ShardRuntimeReplayJournal {
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

impl JournalInputReader for ShardRuntimeReplayJournal {
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
fn shard_runtime_enqueue_input_routes_entry_to_symbol_handoff() {
    let btc = Symbol("BTC-USDT".to_string());
    let eth = Symbol("ETH-USDT".to_string());
    let mut execution_core = ShardExecutionCore::new();
    let mut handoffs = HashMap::new();

    execution_core.add_symbol(btc.clone());
    execution_core.add_symbol(eth.clone());
    handoffs.insert(btc.clone(), BoundedHandoff::new(4));
    handoffs.insert(eth.clone(), BoundedHandoff::new(4));

    let mut shard_runtime = ShardRuntime::new(execution_core, handoffs);

    assert_eq!(
        shard_runtime.enqueue_input(command_entry(1, eth.clone())),
        Ok(())
    );
    assert_eq!(shard_runtime.pending_input_len(&btc), Some(0));
    assert_eq!(shard_runtime.pending_input_len(&eth), Some(1));
    assert_eq!(
        shard_runtime.pending_input_status(&eth),
        Some(matching_core::shard_runtime::ShardRuntimeInputStatus {
            len: 1,
            capacity: 4,
            full: false,
        })
    );
}

#[test]
fn shard_runtime_enqueue_input_rejects_entry_without_symbol_handoff() {
    let btc = Symbol("BTC-USDT".to_string());
    let eth = Symbol("ETH-USDT".to_string());
    let mut execution_core = ShardExecutionCore::new();
    let mut handoffs = HashMap::new();

    execution_core.add_symbol(btc.clone());
    execution_core.add_symbol(eth.clone());
    handoffs.insert(btc.clone(), BoundedHandoff::new(4));

    let mut shard_runtime = ShardRuntime::new(execution_core, handoffs);

    assert_eq!(
        shard_runtime.enqueue_input(command_entry(1, eth.clone())),
        Err(ShardRuntimeError::MissingHandoff(eth.clone()))
    );
    assert_eq!(shard_runtime.pending_input_len(&btc), Some(0));
}

#[test]
fn shard_runtime_enqueue_input_rejects_entry_for_unregistered_handoff_symbol() {
    let btc = Symbol("BTC-USDT".to_string());
    let eth = Symbol("ETH-USDT".to_string());
    let mut execution_core = ShardExecutionCore::new();
    let mut handoffs = HashMap::new();

    execution_core.add_symbol(btc.clone());
    handoffs.insert(btc.clone(), BoundedHandoff::new(4));
    handoffs.insert(eth.clone(), BoundedHandoff::new(4));

    let mut shard_runtime = ShardRuntime::new(execution_core, handoffs);

    assert_eq!(
        shard_runtime.enqueue_input(command_entry(1, eth.clone())),
        Err(ShardRuntimeError::UnregisteredHandoff(eth.clone()))
    );
    assert_eq!(shard_runtime.pending_input_len(&btc), Some(0));
    assert_eq!(shard_runtime.pending_input_len(&eth), Some(0));
}

#[test]
fn shard_runtime_enqueue_input_reports_full_symbol_handoff() {
    let btc = Symbol("BTC-USDT".to_string());
    let mut execution_core = ShardExecutionCore::new();
    let mut handoffs = HashMap::new();

    execution_core.add_symbol(btc.clone());
    handoffs.insert(btc.clone(), BoundedHandoff::new(1));

    let mut shard_runtime = ShardRuntime::new(execution_core, handoffs);

    assert_eq!(
        shard_runtime.enqueue_input(command_entry(1, btc.clone())),
        Ok(())
    );
    assert_eq!(
        shard_runtime.enqueue_input(command_entry(2, btc.clone())),
        Err(ShardRuntimeError::InputHandoffFull(btc.clone()))
    );
    assert_eq!(shard_runtime.pending_input_len(&btc), Some(1));
}

#[test]
fn shard_runtime_enqueue_inputs_routes_batch_to_symbol_handoffs() {
    let btc = Symbol("BTC-USDT".to_string());
    let eth = Symbol("ETH-USDT".to_string());
    let mut shard_runtime = ShardRuntime::new_for_symbols(vec![btc.clone(), eth.clone()], 4, 8);

    assert_eq!(
        shard_runtime.enqueue_inputs(vec![
            command_entry(1, btc.clone()),
            command_entry(2, eth.clone()),
            command_entry(3, btc.clone()),
        ]),
        Ok(3)
    );
    assert_eq!(shard_runtime.pending_input_len(&btc), Some(2));
    assert_eq!(shard_runtime.pending_input_len(&eth), Some(1));
}

#[test]
fn shard_runtime_can_be_created_for_symbols_with_runtime_config() {
    let btc = Symbol("BTC-USDT".to_string());
    let mut config = MatchingRuntimeConfig::default();
    config.handoff.capacity = 3;
    config.output_commit.pending_output_capacity = 5;

    let shard_runtime = ShardRuntime::new_for_symbols_with_config(vec![btc.clone()], config);

    let input_status = shard_runtime
        .pending_input_status(&btc)
        .expect("configured symbol should have pending input status");
    let runtime_status = shard_runtime
        .symbol_status(&btc)
        .expect("configured symbol should have runtime status");

    assert_eq!(input_status.capacity, 3);
    assert_eq!(runtime_status.pending_output_capacity, 5);
}

#[test]
fn shard_runtime_run_once_limits_can_be_derived_from_runtime_config() {
    let mut config = MatchingRuntimeConfig::default();
    config.symbol_runtime.max_input_entries_per_step = 7;
    config.output_commit.max_output_requests_per_step = 9;

    let limits = ShardRuntimeRunOnceLimits::from_config(&config);

    assert_eq!(limits.max_input_entries_per_symbol, 7);
    assert_eq!(limits.max_output_requests_per_symbol, 9);
}

#[test]
fn shard_runtime_enqueue_inputs_rejects_batch_without_partial_enqueue_when_symbol_handoff_would_fill(
) {
    let btc = Symbol("BTC-USDT".to_string());
    let mut shard_runtime = ShardRuntime::new_for_symbols(vec![btc.clone()], 1, 8);

    assert_eq!(
        shard_runtime.enqueue_inputs(vec![
            command_entry(1, btc.clone()),
            command_entry(2, btc.clone()),
        ]),
        Err(ShardRuntimeError::InputHandoffFull(btc.clone()))
    );
    assert_eq!(shard_runtime.pending_input_len(&btc), Some(0));
}

#[test]
fn shard_runtime_enqueue_inputs_rejects_batch_without_partial_enqueue_when_symbol_is_unregistered()
{
    let btc = Symbol("BTC-USDT".to_string());
    let eth = Symbol("ETH-USDT".to_string());
    let mut execution_core = ShardExecutionCore::new();
    let mut handoffs = HashMap::new();

    execution_core.add_symbol(btc.clone());
    handoffs.insert(btc.clone(), BoundedHandoff::new(4));
    handoffs.insert(eth.clone(), BoundedHandoff::new(4));

    let mut shard_runtime = ShardRuntime::new(execution_core, handoffs);

    assert_eq!(
        shard_runtime.enqueue_inputs(vec![
            command_entry(1, btc.clone()),
            command_entry(2, eth.clone()),
        ]),
        Err(ShardRuntimeError::UnregisteredHandoff(eth.clone()))
    );
    assert_eq!(shard_runtime.pending_input_len(&btc), Some(0));
    assert_eq!(shard_runtime.pending_input_len(&eth), Some(0));
}

#[test]
fn shard_runtime_enqueue_inputs_rejects_batch_without_partial_enqueue_when_symbol_handoff_is_missing(
) {
    let btc = Symbol("BTC-USDT".to_string());
    let eth = Symbol("ETH-USDT".to_string());
    let mut execution_core = ShardExecutionCore::new();
    let mut handoffs = HashMap::new();

    execution_core.add_symbol(btc.clone());
    execution_core.add_symbol(eth.clone());
    handoffs.insert(btc.clone(), BoundedHandoff::new(4));

    let mut shard_runtime = ShardRuntime::new(execution_core, handoffs);

    assert_eq!(
        shard_runtime.enqueue_inputs(vec![
            command_entry(1, btc.clone()),
            command_entry(2, eth.clone()),
        ]),
        Err(ShardRuntimeError::MissingHandoff(eth.clone()))
    );
    assert_eq!(shard_runtime.pending_input_len(&btc), Some(0));
}

#[test]
fn shard_runtime_enqueue_inputs_reports_full_handoff_in_deterministic_order() {
    let ada = Symbol("ADA-USDT".to_string());
    let btc = Symbol("BTC-USDT".to_string());
    let mut shard_runtime = ShardRuntime::new_for_symbols(vec![btc.clone(), ada.clone()], 1, 8);

    assert_eq!(
        shard_runtime.enqueue_inputs(vec![
            command_entry(1, btc.clone()),
            command_entry(2, btc.clone()),
            command_entry(3, ada.clone()),
            command_entry(4, ada.clone()),
        ]),
        Err(ShardRuntimeError::InputHandoffFull(ada.clone()))
    );
    assert_eq!(shard_runtime.pending_input_len(&ada), Some(0));
    assert_eq!(shard_runtime.pending_input_len(&btc), Some(0));
}

#[test]
fn shard_runtime_can_be_created_with_registered_symbols_and_handoffs_together() {
    let btc = Symbol("BTC-USDT".to_string());
    let eth = Symbol("ETH-USDT".to_string());
    let mut journal_client = OutputJournalClient::new();
    let mut output = AcceptingJournalOutputAppender::new();
    let mut shard_runtime = ShardRuntime::new_for_symbols(vec![eth.clone(), btc.clone()], 4, 8);

    assert_eq!(shard_runtime.pending_input_len(&btc), Some(0));
    assert_eq!(shard_runtime.pending_input_len(&eth), Some(0));
    assert!(shard_runtime.symbol_status(&btc).is_some());
    assert!(shard_runtime.symbol_status(&eth).is_some());
    assert_eq!(
        shard_runtime.enqueue_inputs(vec![
            command_entry(1, btc.clone()),
            command_entry(1, eth.clone()),
        ]),
        Ok(2)
    );

    let report = shard_runtime
        .run_once(
            &mut journal_client,
            &mut output,
            ShardRuntimeRunOnceLimits {
                max_input_entries_per_symbol: 1,
                max_output_requests_per_symbol: 10,
            },
        )
        .expect("assembled shard runtime should process registered symbols");
    let report_symbols: Vec<Symbol> = report
        .symbol_reports
        .iter()
        .map(|symbol_report| symbol_report.symbol.clone())
        .collect();

    assert_eq!(report_symbols, vec![btc.clone(), eth.clone()]);
    assert_eq!(
        shard_runtime.last_input_seq(&btc),
        Some(Some(JournalSeq(1)))
    );
    assert_eq!(
        shard_runtime.last_input_seq(&eth),
        Some(Some(JournalSeq(1)))
    );
}

#[test]
fn shard_runtime_live_path_matches_replay_output_checksum_and_safe_point_per_symbol() {
    let btc = Symbol("BTC-USDT".to_string());
    let eth = Symbol("ETH-USDT".to_string());
    let entries = vec![
        limit_entry(1, 10, 100, btc.clone(), Side::Sell, 100, 1),
        limit_entry(2, 20, 200, eth.clone(), Side::Buy, 50, 1),
        limit_entry(3, 11, 101, btc.clone(), Side::Buy, 100, 1),
        cancel_entry(4, 21, 200, eth.clone()),
    ];
    let mut shard_runtime = ShardRuntime::new_for_symbols(vec![eth.clone(), btc.clone()], 8, 8);
    let mut journal_client = OutputJournalClient::new();
    let mut output = AcceptingJournalOutputAppender::new();

    assert_eq!(
        shard_runtime.enqueue_inputs(entries.clone()),
        Ok(entries.len())
    );

    let report = shard_runtime
        .run_once(
            &mut journal_client,
            &mut output,
            ShardRuntimeRunOnceLimits {
                max_input_entries_per_symbol: 8,
                max_output_requests_per_symbol: 8,
            },
        )
        .expect("shard runtime should process all queued inputs");

    assert!(!report.has_work_remaining());

    for symbol in [btc, eth] {
        let replay_journal = ShardRuntimeReplayJournal::for_symbol(&entries, &symbol);
        let replay_result = ReplayRunner::new(symbol.clone()).replay_result(&replay_journal);
        let live_result = ReplayResult {
            checksum: shard_runtime
                .checksum(&symbol)
                .expect("shard runtime should expose registered symbol checksum"),
            last_replayed_seq: shard_runtime
                .last_input_seq(&symbol)
                .expect("shard runtime should expose registered symbol safe point"),
            output_entries: normalized_output_for_symbol(&output, &symbol),
        };

        assert!(live_result.compare_with(&replay_result).is_match());
    }
}

#[test]
fn shard_runtime_can_save_symbol_snapshot_to_store_after_safe_point_advances() {
    let btc = Symbol("BTC-USDT".to_string());
    let mut shard_runtime = ShardRuntime::new_for_symbols(vec![btc.clone()], 4, 8);
    let mut journal_client = OutputJournalClient::new();
    let mut output = AcceptingJournalOutputAppender::new();
    let mut snapshot_store = InMemorySnapshotStore::new();

    assert_eq!(
        shard_runtime.enqueue_input(command_entry(1, btc.clone())),
        Ok(())
    );
    shard_runtime
        .run_once(
            &mut journal_client,
            &mut output,
            ShardRuntimeRunOnceLimits {
                max_input_entries_per_symbol: 1,
                max_output_requests_per_symbol: 10,
            },
        )
        .expect("shard runtime should advance a safe point");

    let saved = shard_runtime
        .save_symbol_snapshot(&btc, &mut snapshot_store)
        .expect("snapshot store should accept a runtime snapshot")
        .expect("safe point should produce a snapshot");
    let loaded = snapshot_store
        .load_latest_symbol_snapshot(&btc)
        .expect("saved snapshot should decode")
        .expect("saved snapshot should exist");

    assert_eq!(saved.symbol, btc);
    assert_eq!(saved.safe_point, JournalSeq(1));
    assert_eq!(loaded.order_book_snapshot.last_input_seq, JournalSeq(1));
    assert_eq!(
        loaded.order_book_snapshot.checksum,
        shard_runtime
            .checksum(&loaded.order_book_snapshot.symbol)
            .expect("shard runtime should expose checksum")
    );
}

#[test]
fn shard_runtime_returns_no_snapshot_before_symbol_safe_point_exists() {
    let btc = Symbol("BTC-USDT".to_string());
    let shard_runtime = ShardRuntime::new_for_symbols(vec![btc.clone()], 4, 8);
    let mut snapshot_store = InMemorySnapshotStore::new();

    assert_eq!(
        shard_runtime
            .save_symbol_snapshot(&btc, &mut snapshot_store)
            .expect("snapshot store should not be called without a snapshot"),
        None
    );
    assert_eq!(snapshot_store.load_latest_symbol_snapshot(&btc), Ok(None));
}

#[test]
fn shard_runtime_can_be_restored_from_symbol_snapshot_store() {
    let btc = Symbol("BTC-USDT".to_string());
    let snapshot = SymbolRuntimeSnapshot {
        order_book_snapshot: OrderBookSnapshot {
            symbol: btc.clone(),
            last_input_seq: JournalSeq(10),
            checksum: matching_core::types::Checksum(0),
            resting_orders: vec![Order {
                order_id: OrderId(100),
                symbol: btc.clone(),
                side: Side::Sell,
                price: Price(100),
                quantity: Quantity(1),
            }],
        },
        next_trade_seq: 1,
        next_market_seq: 2,
        seen_command_ids: vec![CommandId(10)],
        seen_order_ids: vec![OrderId(100)],
    };
    let mut snapshot_store = InMemorySnapshotStore::new();

    snapshot_store
        .save_symbol_snapshot(&snapshot)
        .expect("snapshot should be saved");

    let mut shard_runtime = ShardRuntime::new_from_symbol_snapshots(
        vec![snapshot_store
            .load_latest_symbol_snapshot(&btc)
            .expect("stored snapshot should decode")
            .expect("stored snapshot should exist")],
        4,
        8,
    );
    let mut journal_client = OutputJournalClient::new();
    let mut output = AcceptingJournalOutputAppender::new();

    assert_eq!(
        shard_runtime.enqueue_input(limit_entry(11, 11, 101, btc.clone(), Side::Buy, 100, 1,)),
        Ok(())
    );
    shard_runtime
        .run_once(
            &mut journal_client,
            &mut output,
            ShardRuntimeRunOnceLimits {
                max_input_entries_per_symbol: 1,
                max_output_requests_per_symbol: 10,
            },
        )
        .expect("restored shard runtime should process the replay tail");

    assert_eq!(
        shard_runtime.last_input_seq(&btc),
        Some(Some(JournalSeq(11)))
    );
}

#[test]
fn shard_runtime_can_restore_from_file_snapshot_store_after_process_restart() {
    let btc = Symbol("BTC-USDT".to_string());
    let dir = temporary_snapshot_dir("runtime-loop-file-snapshot");
    let mut shard_runtime = ShardRuntime::new_for_symbols(vec![btc.clone()], 4, 8);
    let mut journal_client = OutputJournalClient::new();
    let mut output = AcceptingJournalOutputAppender::new();
    let mut writer_store = FileSnapshotStore::new(dir.clone());

    assert_eq!(
        shard_runtime.enqueue_input(limit_entry(1, 10, 100, btc.clone(), Side::Sell, 100, 1,)),
        Ok(())
    );
    shard_runtime
        .run_once(
            &mut journal_client,
            &mut output,
            ShardRuntimeRunOnceLimits {
                max_input_entries_per_symbol: 1,
                max_output_requests_per_symbol: 10,
            },
        )
        .expect("shard runtime should advance a safe point");
    shard_runtime
        .save_symbol_snapshot(&btc, &mut writer_store)
        .expect("file snapshot store should accept runtime snapshot")
        .expect("safe point should produce a snapshot");

    let reader_store = FileSnapshotStore::new(dir.clone());
    let loaded_snapshot = reader_store
        .load_latest_symbol_snapshot(&btc)
        .expect("file snapshot should decode after restart")
        .expect("file snapshot should exist after restart");
    let mut restored_loop = ShardRuntime::new_from_symbol_snapshots(vec![loaded_snapshot], 4, 8);
    let mut restored_journal_client = OutputJournalClient::new();
    let mut restored_output = AcceptingJournalOutputAppender::new();

    assert_eq!(
        restored_loop.enqueue_input(limit_entry(2, 11, 101, btc.clone(), Side::Buy, 100, 1,)),
        Ok(())
    );
    restored_loop
        .run_once(
            &mut restored_journal_client,
            &mut restored_output,
            ShardRuntimeRunOnceLimits {
                max_input_entries_per_symbol: 1,
                max_output_requests_per_symbol: 10,
            },
        )
        .expect("restored shard runtime should process the replay tail");

    assert_eq!(
        restored_loop.last_input_seq(&btc),
        Some(Some(JournalSeq(2)))
    );

    fs::remove_dir_all(dir).expect("temporary snapshot dir should be removed");
}

#[test]
fn shard_runtime_can_restore_from_older_file_snapshot_when_latest_is_corrupt() {
    let btc = Symbol("BTC-USDT".to_string());
    let dir = temporary_snapshot_dir("runtime-loop-file-snapshot-fallback");
    let mut shard_runtime = ShardRuntime::new_for_symbols(vec![btc.clone()], 4, 8);
    let mut journal_client = OutputJournalClient::new();
    let mut output = AcceptingJournalOutputAppender::new();
    let mut writer_store = FileSnapshotStore::new_with_retention_limit(dir.clone(), 2);

    assert_eq!(
        shard_runtime.enqueue_input(limit_entry(1, 10, 100, btc.clone(), Side::Sell, 100, 1,)),
        Ok(())
    );
    shard_runtime
        .run_once(
            &mut journal_client,
            &mut output,
            ShardRuntimeRunOnceLimits {
                max_input_entries_per_symbol: 1,
                max_output_requests_per_symbol: 10,
            },
        )
        .expect("shard runtime should advance a safe point");
    let saved = shard_runtime
        .save_symbol_snapshot(&btc, &mut writer_store)
        .expect("file snapshot store should accept runtime snapshot")
        .expect("safe point should produce a snapshot");

    let mut corrupt_latest = saved.bytes.clone();
    corrupt_latest[0] = b'X';
    writer_store
        .write_raw_symbol_snapshot_bytes(btc.clone(), JournalSeq(2), corrupt_latest)
        .expect("corrupt latest snapshot should be written");

    let reader_store = FileSnapshotStore::new_with_retention_limit(dir.clone(), 2);
    let selection = reader_store
        .select_latest_valid_symbol_snapshot(&btc)
        .expect("file snapshot selection should read retained snapshots");

    assert_eq!(
        selection
            .selected_record
            .as_ref()
            .expect("older snapshot record should be selected")
            .safe_point,
        JournalSeq(1)
    );
    assert_eq!(selection.rejected[0].record.safe_point, JournalSeq(2));

    let mut restored_loop = ShardRuntime::new_from_symbol_snapshots(
        vec![selection
            .selected
            .expect("older valid snapshot should be selected")],
        4,
        8,
    );
    let mut restored_journal_client = OutputJournalClient::new();
    let mut restored_output = AcceptingJournalOutputAppender::new();

    assert_eq!(
        restored_loop.enqueue_input(limit_entry(2, 11, 101, btc.clone(), Side::Buy, 100, 1,)),
        Ok(())
    );
    restored_loop
        .run_once(
            &mut restored_journal_client,
            &mut restored_output,
            ShardRuntimeRunOnceLimits {
                max_input_entries_per_symbol: 1,
                max_output_requests_per_symbol: 10,
            },
        )
        .expect("restored shard runtime should process tail from older snapshot");

    assert_eq!(
        restored_loop.last_input_seq(&btc),
        Some(Some(JournalSeq(2)))
    );

    fs::remove_dir_all(dir).expect("temporary snapshot dir should be removed");
}

#[test]
fn shard_runtime_can_restore_from_latest_verified_file_snapshot_when_newer_is_unverified() {
    let btc = Symbol("BTC-USDT".to_string());
    let dir = temporary_snapshot_dir("runtime-loop-file-snapshot-verified");
    let mut shard_runtime = ShardRuntime::new_for_symbols(vec![btc.clone()], 4, 8);
    let mut journal_client = OutputJournalClient::new();
    let mut output = AcceptingJournalOutputAppender::new();
    let mut writer_store = FileSnapshotStore::new_with_retention_limit(dir.clone(), 2);

    assert_eq!(
        shard_runtime.enqueue_input(limit_entry(1, 10, 100, btc.clone(), Side::Sell, 100, 1,)),
        Ok(())
    );
    shard_runtime
        .run_once(
            &mut journal_client,
            &mut output,
            ShardRuntimeRunOnceLimits {
                max_input_entries_per_symbol: 1,
                max_output_requests_per_symbol: 10,
            },
        )
        .expect("shard runtime should advance the first safe point");
    shard_runtime
        .save_symbol_snapshot(&btc, &mut writer_store)
        .expect("file snapshot store should accept first runtime snapshot")
        .expect("first safe point should produce a snapshot");
    writer_store
        .mark_symbol_snapshot_verified(&btc, JournalSeq(1))
        .expect("first snapshot should be marked verified")
        .expect("first snapshot should exist");

    assert_eq!(
        shard_runtime.enqueue_input(limit_entry(2, 11, 101, btc.clone(), Side::Buy, 100, 1,)),
        Ok(())
    );
    shard_runtime
        .run_once(
            &mut journal_client,
            &mut output,
            ShardRuntimeRunOnceLimits {
                max_input_entries_per_symbol: 1,
                max_output_requests_per_symbol: 10,
            },
        )
        .expect("shard runtime should advance the second safe point");
    shard_runtime
        .save_symbol_snapshot(&btc, &mut writer_store)
        .expect("file snapshot store should accept second runtime snapshot")
        .expect("second safe point should produce an unverified snapshot");

    let reader_store = FileSnapshotStore::new_with_retention_limit(dir.clone(), 2);
    let selection = reader_store
        .select_latest_verified_symbol_snapshot(&btc)
        .expect("file snapshot selection should read verified markers");

    assert_eq!(
        selection
            .selected_record
            .as_ref()
            .expect("verified snapshot record should be selected")
            .safe_point,
        JournalSeq(1)
    );
    assert_eq!(
        selection
            .skipped_unverified
            .iter()
            .map(|record| record.safe_point)
            .collect::<Vec<_>>(),
        vec![JournalSeq(2)]
    );

    let mut restored_loop = ShardRuntime::new_from_symbol_snapshots(
        vec![selection
            .selected
            .expect("older verified snapshot should be selected")],
        4,
        8,
    );
    let mut restored_journal_client = OutputJournalClient::new();
    let mut restored_output = AcceptingJournalOutputAppender::new();

    assert_eq!(
        restored_loop.enqueue_input(limit_entry(2, 11, 101, btc.clone(), Side::Buy, 100, 1,)),
        Ok(())
    );
    restored_loop
        .run_once(
            &mut restored_journal_client,
            &mut restored_output,
            ShardRuntimeRunOnceLimits {
                max_input_entries_per_symbol: 1,
                max_output_requests_per_symbol: 10,
            },
        )
        .expect("restored shard runtime should process tail from verified snapshot");

    assert_eq!(
        restored_loop.last_input_seq(&btc),
        Some(Some(JournalSeq(2)))
    );

    fs::remove_dir_all(dir).expect("temporary snapshot dir should be removed");
}

#[test]
fn shard_runtime_can_validate_configuration_before_running_once() {
    let btc = Symbol("BTC-USDT".to_string());
    let eth = Symbol("ETH-USDT".to_string());
    let shard_runtime = ShardRuntime::new_for_symbols(vec![btc.clone(), eth.clone()], 4, 8);

    assert_eq!(shard_runtime.validate_configuration(), Ok(()));
}

#[test]
fn shard_runtime_run_once_report_identifies_idle_run() {
    let btc = Symbol("BTC-USDT".to_string());
    let mut journal_client = OutputJournalClient::new();
    let mut output = AcceptingJournalOutputAppender::new();
    let mut shard_runtime = ShardRuntime::new_for_symbols(vec![btc.clone()], 4, 8);

    let report = shard_runtime
        .run_once(
            &mut journal_client,
            &mut output,
            ShardRuntimeRunOnceLimits {
                max_input_entries_per_symbol: 1,
                max_output_requests_per_symbol: 10,
            },
        )
        .expect("empty shard runtime run_once should produce an idle report");
    let btc_report = report
        .symbol_report(&btc)
        .expect("btc should have an idle run_once report");

    assert!(!report.made_progress());
    assert!(!report.has_work_remaining());
    assert!(!report.has_blocked_symbols());
    assert!(report.is_idle());
    assert_eq!(btc_report.input_processed_count, 0);
    assert_eq!(btc_report.safe_point_advanced_count, 0);
    assert_eq!(btc_report.pending_input_len_after_run, 0);
    assert_eq!(btc_report.runtime_status_after_run.pending_output_len, 0);
    assert_eq!(shard_runtime.last_input_seq(&btc), Some(None));
    assert_eq!(output.read_all().len(), 0);
}

#[test]
fn shard_runtime_validate_configuration_reports_missing_handoff_before_run_once() {
    let btc = Symbol("BTC-USDT".to_string());
    let eth = Symbol("ETH-USDT".to_string());
    let mut execution_core = ShardExecutionCore::new();
    let mut handoffs = HashMap::new();

    execution_core.add_symbol(btc.clone());
    execution_core.add_symbol(eth.clone());
    handoffs.insert(btc.clone(), BoundedHandoff::new(4));

    let shard_runtime = ShardRuntime::new(execution_core, handoffs);

    assert_eq!(
        shard_runtime.validate_configuration(),
        Err(ShardRuntimeError::MissingHandoff(eth.clone()))
    );
}

#[test]
fn shard_runtime_validate_configuration_reports_unregistered_handoff_in_deterministic_order() {
    let ada = Symbol("ADA-USDT".to_string());
    let btc = Symbol("BTC-USDT".to_string());
    let sol = Symbol("SOL-USDT".to_string());
    let xrp = Symbol("XRP-USDT".to_string());
    let mut execution_core = ShardExecutionCore::new();
    let mut handoffs = HashMap::new();

    execution_core.add_symbol(btc.clone());
    handoffs.insert(btc.clone(), BoundedHandoff::new(4));
    handoffs.insert(xrp.clone(), BoundedHandoff::new(4));
    handoffs.insert(sol.clone(), BoundedHandoff::new(4));
    handoffs.insert(ada.clone(), BoundedHandoff::new(4));

    let shard_runtime = ShardRuntime::new(execution_core, handoffs);

    assert_eq!(
        shard_runtime.validate_configuration(),
        Err(ShardRuntimeError::UnregisteredHandoff(ada.clone()))
    );
}

#[test]
fn shard_runtime_run_once_keeps_unblocked_symbol_running_when_one_symbol_output_blocks() {
    let btc = Symbol("BTC-USDT".to_string());
    let eth = Symbol("ETH-USDT".to_string());
    let mut execution_core = ShardExecutionCore::new();
    let mut handoffs = HashMap::new();
    let mut journal_client = OutputJournalClient::new();
    let mut output = RejectOneSymbolJournalOutputAppender::new(btc.clone());

    execution_core.add_symbol(btc.clone());
    execution_core.add_symbol(eth.clone());
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

    let mut shard_runtime = ShardRuntime::new(execution_core, handoffs);
    let report = shard_runtime
        .run_once(
            &mut journal_client,
            &mut output,
            ShardRuntimeRunOnceLimits {
                max_input_entries_per_symbol: 1,
                max_output_requests_per_symbol: 10,
            },
        )
        .expect(
            "runtime run_once should report symbol-local output blockage without stopping all symbols",
        );

    let btc_report = report
        .symbol_report(&btc)
        .expect("btc should have a run_once report");
    let eth_report = report
        .symbol_report(&eth)
        .expect("eth should have a run_once report");

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

    assert_eq!(shard_runtime.last_input_seq(&btc), Some(None));
    assert_eq!(
        shard_runtime.last_input_seq(&eth),
        Some(Some(JournalSeq(1)))
    );
    let btc_status = shard_runtime
        .symbol_status(&btc)
        .expect("btc status should exist");
    assert_eq!(btc_status.pending_output_len, 1);
    assert_eq!(
        btc_status.output_commit_escalation,
        btc_report.block_decision
    );
    assert!(btc_status.output_commit_blockage.is_some());
    assert_eq!(shard_runtime.pending_input_len(&eth), Some(0));
    assert_eq!(output.read_all().len(), 1);
}

#[test]
fn shard_runtime_run_once_prioritizes_output_commit_when_pending_output_is_full() {
    let btc = Symbol("BTC-USDT".to_string());
    let mut execution_core = ShardExecutionCore::new_with_pending_output_capacity(1);
    let mut handoffs = HashMap::new();
    let mut journal_client = OutputJournalClient::new();
    let mut output = AcceptingJournalOutputAppender::new();

    execution_core.add_symbol(btc.clone());
    handoffs.insert(btc.clone(), BoundedHandoff::new(4));
    handoffs
        .get_mut(&btc)
        .expect("btc handoff should exist")
        .enqueue(command_entry(1, btc.clone()))
        .expect("first btc command should enqueue");

    let mut shard_runtime = ShardRuntime::new(execution_core, handoffs);
    let first_report = shard_runtime
        .run_once(
            &mut journal_client,
            &mut output,
            ShardRuntimeRunOnceLimits {
                max_input_entries_per_symbol: 1,
                max_output_requests_per_symbol: 0,
            },
        )
        .expect("first run_once should fill pending output without committing it");
    let first_btc_report = first_report
        .symbol_report(&btc)
        .expect("btc should have a first run_once report");

    assert!(first_report.made_progress());
    assert!(first_report.has_work_remaining());
    assert!(!first_report.has_blocked_symbols());
    assert_eq!(first_btc_report.input_processed_count, 1);
    assert_eq!(first_btc_report.safe_point_advanced_count, 0);
    assert_eq!(shard_runtime.last_input_seq(&btc), Some(None));
    assert_eq!(
        shard_runtime
            .symbol_status(&btc)
            .expect("btc status should exist")
            .pending_output_len,
        1
    );
    assert_eq!(output.read_all().len(), 0);

    shard_runtime
        .enqueue_input(command_entry(2, btc.clone()))
        .expect("second btc command should enqueue");

    let second_report = shard_runtime
        .run_once(
            &mut journal_client,
            &mut output,
            ShardRuntimeRunOnceLimits {
                max_input_entries_per_symbol: 1,
                max_output_requests_per_symbol: 10,
            },
        )
        .expect("second run_once should commit full pending output before draining new input");
    let second_btc_report = second_report
        .symbol_report(&btc)
        .expect("btc should have a second run_once report");

    assert!(second_report.made_progress());
    assert!(second_report.has_work_remaining());
    assert!(!second_report.has_blocked_symbols());
    assert_eq!(second_btc_report.input_processed_count, 0);
    assert_eq!(second_btc_report.safe_point_advanced_count, 1);
    assert_eq!(
        second_btc_report.runtime_status_after_run.last_input_seq,
        Some(JournalSeq(1))
    );
    assert_eq!(
        second_btc_report
            .runtime_status_after_run
            .pending_output_len,
        0
    );
    assert!(
        !second_btc_report
            .runtime_status_after_run
            .pending_output_full
    );
    assert_eq!(second_btc_report.pending_input_len_after_run, 1);
    assert_eq!(second_btc_report.pending_input_capacity, 4);
    assert!(!second_btc_report.pending_input_full);
    assert_eq!(
        shard_runtime.last_input_seq(&btc),
        Some(Some(JournalSeq(1)))
    );
    assert_eq!(shard_runtime.pending_input_len(&btc), Some(1));
    assert_eq!(
        shard_runtime
            .symbol_status(&btc)
            .expect("btc status should exist")
            .pending_output_len,
        0
    );
    assert_eq!(output.read_all().len(), 1);
}

#[test]
fn shard_runtime_cycle_reports_output_batch_identity_and_query_status() {
    let btc = Symbol("BTC-USDT".to_string());
    let mut execution_core = ShardExecutionCore::new();
    let mut handoffs = HashMap::new();
    let mut journal_client = OutputJournalClient::new();
    let mut output = DurableUnknownJournalOutputAppender::new();

    execution_core.add_symbol(btc.clone());
    handoffs.insert(btc.clone(), BoundedHandoff::new(4));
    handoffs
        .get_mut(&btc)
        .expect("btc handoff should exist")
        .enqueue(command_entry(1, btc.clone()))
        .expect("btc command should enqueue");

    let mut shard_runtime = ShardRuntime::new(execution_core, handoffs);
    let report = shard_runtime
        .run_once(
            &mut journal_client,
            &mut output,
            ShardRuntimeRunOnceLimits {
                max_input_entries_per_symbol: 1,
                max_output_requests_per_symbol: 10,
            },
        )
        .expect("runtime run_once should report durable output batch status");
    let btc_report = report
        .symbol_report(&btc)
        .expect("btc should have a run_once report");
    let output_batch_identity = btc_report
        .output_batch_identity
        .as_ref()
        .expect("runtime run_once should expose attempted output batch identity");

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
    assert_eq!(
        shard_runtime.last_input_seq(&btc),
        Some(Some(JournalSeq(1)))
    );
}

#[test]
fn shard_runtime_run_once_fails_before_processing_when_a_registered_symbol_has_no_handoff() {
    let btc = Symbol("BTC-USDT".to_string());
    let eth = Symbol("ETH-USDT".to_string());
    let mut execution_core = ShardExecutionCore::new();
    let mut handoffs = HashMap::new();
    let mut journal_client = OutputJournalClient::new();
    let mut output = AcceptingJournalOutputAppender::new();

    execution_core.add_symbol(btc.clone());
    execution_core.add_symbol(eth.clone());
    handoffs.insert(btc.clone(), BoundedHandoff::new(4));
    handoffs
        .get_mut(&btc)
        .expect("btc handoff should exist")
        .enqueue(command_entry(1, btc.clone()))
        .expect("btc command should enqueue");

    let mut shard_runtime = ShardRuntime::new(execution_core, handoffs);
    let result = shard_runtime.run_once(
        &mut journal_client,
        &mut output,
        ShardRuntimeRunOnceLimits {
            max_input_entries_per_symbol: 1,
            max_output_requests_per_symbol: 10,
        },
    );

    assert_eq!(result, Err(ShardRuntimeError::MissingHandoff(eth.clone())));
    assert_eq!(output.read_all().len(), 0);
    assert_eq!(shard_runtime.last_input_seq(&btc), Some(None));
    assert_eq!(
        shard_runtime
            .symbol_status(&btc)
            .expect("btc status should exist")
            .pending_output_len,
        0
    );
}

#[test]
fn shard_runtime_run_once_fails_before_processing_when_handoff_has_unregistered_symbol() {
    let btc = Symbol("BTC-USDT".to_string());
    let eth = Symbol("ETH-USDT".to_string());
    let mut execution_core = ShardExecutionCore::new();
    let mut handoffs = HashMap::new();
    let mut journal_client = OutputJournalClient::new();
    let mut output = AcceptingJournalOutputAppender::new();

    execution_core.add_symbol(btc.clone());
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

    let mut shard_runtime = ShardRuntime::new(execution_core, handoffs);
    let result = shard_runtime.run_once(
        &mut journal_client,
        &mut output,
        ShardRuntimeRunOnceLimits {
            max_input_entries_per_symbol: 1,
            max_output_requests_per_symbol: 10,
        },
    );

    assert_eq!(
        result,
        Err(ShardRuntimeError::UnregisteredHandoff(eth.clone()))
    );
    assert_eq!(output.read_all().len(), 0);
    assert_eq!(shard_runtime.last_input_seq(&btc), Some(None));
    assert_eq!(
        shard_runtime
            .symbol_status(&btc)
            .expect("btc status should exist")
            .pending_output_len,
        0
    );
}

#[test]
fn shard_runtime_run_once_keeps_unblocked_symbol_running_when_one_symbol_is_quarantined() {
    let btc = Symbol("BTC-USDT".to_string());
    let eth = Symbol("ETH-USDT".to_string());
    let mut execution_core = ShardExecutionCore::new();
    let mut handoffs = HashMap::new();
    let mut journal_client = OutputJournalClient::new();
    let mut output = RejectOneSymbolJournalOutputAppender::new(btc.clone());

    execution_core.add_symbol(btc.clone());
    execution_core.add_symbol(eth.clone());
    handoffs.insert(btc.clone(), BoundedHandoff::new(4));
    handoffs.insert(eth.clone(), BoundedHandoff::new(4));
    handoffs
        .get_mut(&btc)
        .expect("btc handoff should exist")
        .enqueue(command_entry(1, btc.clone()))
        .expect("btc command should enqueue");

    let mut shard_runtime = ShardRuntime::new(execution_core, handoffs);
    let first_report = shard_runtime
        .run_once(
            &mut journal_client,
            &mut output,
            ShardRuntimeRunOnceLimits {
                max_input_entries_per_symbol: 1,
                max_output_requests_per_symbol: 10,
            },
        )
        .expect("first run_once should create a btc escalation");
    let btc_decision = first_report
        .symbol_report(&btc)
        .expect("btc should have a first run_once report")
        .block_decision
        .expect("btc should be escalated");

    assert_eq!(
        shard_runtime.quarantine_symbol_output_commit_escalation(&btc),
        Ok(Some(btc_decision))
    );
    shard_runtime
        .enqueue_input(command_entry(2, btc.clone()))
        .expect("second btc command should enqueue");
    shard_runtime
        .enqueue_input(command_entry(1, eth.clone()))
        .expect("eth command should enqueue");

    let second_report = shard_runtime
        .run_once(
            &mut journal_client,
            &mut output,
            ShardRuntimeRunOnceLimits {
                max_input_entries_per_symbol: 1,
                max_output_requests_per_symbol: 10,
            },
        )
        .expect("second run_once should keep quarantined btc paused and continue eth");
    let paused_btc_report = second_report
        .symbol_report(&btc)
        .expect("btc should have a paused run_once report");
    let eth_report = second_report
        .symbol_report(&eth)
        .expect("eth should have a run_once report");

    assert_eq!(paused_btc_report.input_processed_count, 0);
    assert_eq!(paused_btc_report.safe_point_advanced_count, 0);
    assert_eq!(paused_btc_report.block_decision, Some(btc_decision));
    assert_eq!(eth_report.input_processed_count, 1);
    assert_eq!(eth_report.safe_point_advanced_count, 1);
    assert_eq!(shard_runtime.pending_input_len(&btc), Some(1));
    assert_eq!(shard_runtime.pending_input_len(&eth), Some(0));
    assert_eq!(shard_runtime.last_input_seq(&btc), Some(None));
    assert_eq!(
        shard_runtime.last_input_seq(&eth),
        Some(Some(JournalSeq(1)))
    );
    assert!(shard_runtime
        .symbol_status(&btc)
        .expect("btc status should exist")
        .output_commit_quarantine
        .is_some());
}

#[test]
fn shard_runtime_can_clear_quarantine_and_retry_pending_output() {
    let btc = Symbol("BTC-USDT".to_string());
    let mut execution_core = ShardExecutionCore::new();
    let mut handoffs = HashMap::new();
    let mut journal_client = OutputJournalClient::new();
    let mut rejecting_output = RejectOneSymbolJournalOutputAppender::new(btc.clone());

    execution_core.add_symbol(btc.clone());
    handoffs.insert(btc.clone(), BoundedHandoff::new(4));
    handoffs
        .get_mut(&btc)
        .expect("btc handoff should exist")
        .enqueue(command_entry(1, btc.clone()))
        .expect("btc command should enqueue");

    let mut shard_runtime = ShardRuntime::new(execution_core, handoffs);
    let blocked_report = shard_runtime
        .run_once(
            &mut journal_client,
            &mut rejecting_output,
            ShardRuntimeRunOnceLimits {
                max_input_entries_per_symbol: 1,
                max_output_requests_per_symbol: 10,
            },
        )
        .expect("first run_once should create a btc escalation");
    let decision = blocked_report
        .symbol_report(&btc)
        .expect("btc should have a blocked report")
        .block_decision
        .expect("btc should be escalated");

    assert_eq!(
        shard_runtime.quarantine_symbol_output_commit_escalation(&btc),
        Ok(Some(decision))
    );
    assert_eq!(
        shard_runtime.clear_symbol_output_commit_quarantine(&btc),
        Ok(Some(decision))
    );

    let mut accepting_output = AcceptingJournalOutputAppender::new();
    let retry_report = shard_runtime
        .run_once(
            &mut journal_client,
            &mut accepting_output,
            ShardRuntimeRunOnceLimits {
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
    assert_eq!(
        shard_runtime.last_input_seq(&btc),
        Some(Some(JournalSeq(1)))
    );
    assert_eq!(
        shard_runtime
            .symbol_status(&btc)
            .expect("btc status should exist")
            .pending_output_len,
        0
    );
    assert_eq!(accepting_output.read_all().len(), 1);
}

#[test]
fn shard_runtime_clear_quarantine_does_not_drop_pending_output_when_retry_fails() {
    let btc = Symbol("BTC-USDT".to_string());
    let mut execution_core = ShardExecutionCore::new();
    let mut handoffs = HashMap::new();
    let mut journal_client = OutputJournalClient::new();
    let mut rejecting_output = RejectOneSymbolJournalOutputAppender::new(btc.clone());

    execution_core.add_symbol(btc.clone());
    handoffs.insert(btc.clone(), BoundedHandoff::new(4));
    handoffs
        .get_mut(&btc)
        .expect("btc handoff should exist")
        .enqueue(command_entry(1, btc.clone()))
        .expect("btc command should enqueue");

    let mut shard_runtime = ShardRuntime::new(execution_core, handoffs);
    let blocked_report = shard_runtime
        .run_once(
            &mut journal_client,
            &mut rejecting_output,
            ShardRuntimeRunOnceLimits {
                max_input_entries_per_symbol: 1,
                max_output_requests_per_symbol: 10,
            },
        )
        .expect("first run_once should create a btc escalation");
    let first_decision = blocked_report
        .symbol_report(&btc)
        .expect("btc should have a blocked report")
        .block_decision
        .expect("btc should be escalated");

    assert_eq!(
        shard_runtime.quarantine_symbol_output_commit_escalation(&btc),
        Ok(Some(first_decision))
    );
    assert_eq!(
        shard_runtime.clear_symbol_output_commit_quarantine(&btc),
        Ok(Some(first_decision))
    );

    let retry_report = shard_runtime
        .run_once(
            &mut journal_client,
            &mut rejecting_output,
            ShardRuntimeRunOnceLimits {
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
    assert_eq!(shard_runtime.last_input_seq(&btc), Some(None));
    assert_eq!(
        shard_runtime
            .symbol_status(&btc)
            .expect("btc status should exist")
            .pending_output_len,
        1
    );
    assert!(shard_runtime
        .symbol_status(&btc)
        .expect("btc status should exist")
        .output_commit_escalation
        .is_some());
}

#[test]
fn shard_runtime_cycle_reports_symbols_in_deterministic_order() {
    let btc = Symbol("BTC-USDT".to_string());
    let eth = Symbol("ETH-USDT".to_string());
    let mut execution_core = ShardExecutionCore::new();
    let mut handoffs = HashMap::new();
    let mut journal_client = OutputJournalClient::new();
    let mut output = AcceptingJournalOutputAppender::new();

    execution_core.add_symbol(eth.clone());
    execution_core.add_symbol(btc.clone());
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

    let mut shard_runtime = ShardRuntime::new(execution_core, handoffs);
    let report = shard_runtime
        .run_once(
            &mut journal_client,
            &mut output,
            ShardRuntimeRunOnceLimits {
                max_input_entries_per_symbol: 1,
                max_output_requests_per_symbol: 10,
            },
        )
        .expect("runtime run_once should process registered symbols");

    let report_symbols: Vec<Symbol> = report
        .symbol_reports
        .iter()
        .map(|symbol_report| symbol_report.symbol.clone())
        .collect();

    assert!(report.made_progress());
    assert!(!report.has_work_remaining());
    assert!(!report.has_blocked_symbols());
    assert_eq!(report_symbols, vec![btc.clone(), eth.clone()]);
    assert_eq!(
        shard_runtime.last_input_seq(&btc),
        Some(Some(JournalSeq(1)))
    );
    assert_eq!(
        shard_runtime.last_input_seq(&eth),
        Some(Some(JournalSeq(1)))
    );
}

#[test]
fn shard_runtime_run_limited_drains_work_until_idle() {
    let btc = Symbol("BTC-USDT".to_string());
    let mut journal_client = OutputJournalClient::new();
    let mut output = AcceptingJournalOutputAppender::new();
    let mut shard_runtime = ShardRuntime::new_for_symbols(vec![btc.clone()], 4, 8);

    assert_eq!(
        shard_runtime.enqueue_inputs(vec![
            command_entry(1, btc.clone()),
            command_entry(2, btc.clone()),
        ]),
        Ok(2)
    );

    let report = shard_runtime
        .run_limited(
            &mut journal_client,
            &mut output,
            ShardRuntimeRunOnceLimits {
                max_input_entries_per_symbol: 1,
                max_output_requests_per_symbol: 10,
            },
            ShardRuntimeRunLimit { max_cycles: 4 },
        )
        .expect("limited run should drain queued work");

    assert_eq!(report.stop_reason, ShardRuntimeRunStopReason::Idle);
    assert_eq!(report.cycle_count(), 2);
    assert!(report.made_progress);
    assert!(!report.has_work_remaining);
    assert!(!report.has_blocked_symbols);
    assert!(report.is_idle());
    assert_eq!(
        shard_runtime.last_input_seq(&btc),
        Some(Some(JournalSeq(2)))
    );
    assert_eq!(shard_runtime.pending_input_len(&btc), Some(0));
    assert_eq!(output.read_all().len(), 2);
}

#[test]
fn shard_runtime_run_limited_reports_run_limit_exhaustion_with_remaining_work() {
    let btc = Symbol("BTC-USDT".to_string());
    let mut journal_client = OutputJournalClient::new();
    let mut output = AcceptingJournalOutputAppender::new();
    let mut shard_runtime = ShardRuntime::new_for_symbols(vec![btc.clone()], 4, 8);

    assert_eq!(
        shard_runtime.enqueue_inputs(vec![
            command_entry(1, btc.clone()),
            command_entry(2, btc.clone()),
        ]),
        Ok(2)
    );

    let report = shard_runtime
        .run_limited(
            &mut journal_client,
            &mut output,
            ShardRuntimeRunOnceLimits {
                max_input_entries_per_symbol: 1,
                max_output_requests_per_symbol: 10,
            },
            ShardRuntimeRunLimit { max_cycles: 1 },
        )
        .expect("limited run should stop when run limit is consumed");

    assert_eq!(
        report.stop_reason,
        ShardRuntimeRunStopReason::RunLimitReached
    );
    assert_eq!(report.cycle_count(), 1);
    assert!(report.made_progress);
    assert!(report.has_work_remaining);
    assert!(!report.has_blocked_symbols);
    assert!(!report.is_idle());
    assert_eq!(report.symbols_with_remaining_work(), vec![btc.clone()]);
    assert_eq!(report.blocked_symbols(), Vec::<Symbol>::new());
    assert_eq!(
        shard_runtime.last_input_seq(&btc),
        Some(Some(JournalSeq(1)))
    );
    assert_eq!(shard_runtime.pending_input_len(&btc), Some(1));
    assert_eq!(output.read_all().len(), 1);
}

#[test]
fn shard_runtime_run_limited_stops_after_unblocked_work_drains_when_symbol_is_blocked() {
    let btc = Symbol("BTC-USDT".to_string());
    let eth = Symbol("ETH-USDT".to_string());
    let mut journal_client = OutputJournalClient::new();
    let mut output = RejectOneSymbolJournalOutputAppender::new(btc.clone());
    let mut shard_runtime = ShardRuntime::new_for_symbols(vec![btc.clone(), eth.clone()], 4, 8);

    assert_eq!(
        shard_runtime.enqueue_inputs(vec![
            command_entry(1, btc.clone()),
            command_entry(1, eth.clone()),
            command_entry(2, eth.clone()),
        ]),
        Ok(3)
    );

    let report = shard_runtime
        .run_limited(
            &mut journal_client,
            &mut output,
            ShardRuntimeRunOnceLimits {
                max_input_entries_per_symbol: 1,
                max_output_requests_per_symbol: 10,
            },
            ShardRuntimeRunLimit { max_cycles: 4 },
        )
        .expect("limited run should stop once only blocked work remains");

    assert_eq!(report.stop_reason, ShardRuntimeRunStopReason::Blocked);
    assert_eq!(report.cycle_count(), 3);
    assert!(report.made_progress);
    assert!(report.has_work_remaining);
    assert!(report.has_blocked_symbols);
    assert!(!report.is_idle());
    assert_eq!(report.symbols_with_remaining_work(), vec![btc.clone()]);
    assert_eq!(report.blocked_symbols(), vec![btc.clone()]);
    assert_eq!(report.work_status_after_run.len(), 2);
    let btc_work_status = report
        .work_status_after_run
        .iter()
        .find(|status| status.symbol == btc)
        .expect("btc should have work status");
    assert_eq!(btc_work_status.pending_input_len, 0);
    assert_eq!(btc_work_status.pending_output_len, 1);
    assert!(btc_work_status.output_commit_blocked);
    assert_eq!(shard_runtime.last_input_seq(&btc), Some(None));
    assert_eq!(
        shard_runtime.last_input_seq(&eth),
        Some(Some(JournalSeq(2)))
    );
    assert_eq!(shard_runtime.pending_input_len(&eth), Some(0));
    assert_eq!(
        shard_runtime
            .symbol_status(&btc)
            .expect("btc status should exist")
            .pending_output_len,
        1
    );
    assert_eq!(output.read_all().len(), 2);
}

#[test]
fn shard_runtime_run_limited_reports_initial_work_when_run_limit_is_zero() {
    let btc = Symbol("BTC-USDT".to_string());
    let eth = Symbol("ETH-USDT".to_string());
    let mut journal_client = OutputJournalClient::new();
    let mut output = AcceptingJournalOutputAppender::new();
    let mut shard_runtime = ShardRuntime::new_for_symbols(vec![eth.clone(), btc.clone()], 4, 8);

    assert_eq!(
        shard_runtime.enqueue_inputs(vec![
            command_entry(1, eth.clone()),
            command_entry(1, btc.clone()),
        ]),
        Ok(2)
    );

    let report = shard_runtime
        .run_limited(
            &mut journal_client,
            &mut output,
            ShardRuntimeRunOnceLimits {
                max_input_entries_per_symbol: 1,
                max_output_requests_per_symbol: 10,
            },
            ShardRuntimeRunLimit { max_cycles: 0 },
        )
        .expect("zero limit should report initial work without running a run_once cycle");

    assert_eq!(
        report.stop_reason,
        ShardRuntimeRunStopReason::RunLimitReached
    );
    assert_eq!(report.cycle_count(), 0);
    assert!(!report.made_progress);
    assert!(report.has_work_remaining);
    assert_eq!(
        report.symbols_with_remaining_work(),
        vec![btc.clone(), eth.clone()]
    );
    assert_eq!(report.blocked_symbols(), Vec::<Symbol>::new());
    assert_eq!(
        report
            .work_status_after_run
            .iter()
            .map(|status| status.symbol.clone())
            .collect::<Vec<_>>(),
        vec![btc.clone(), eth.clone()]
    );
    assert_eq!(shard_runtime.last_input_seq(&btc), Some(None));
    assert_eq!(shard_runtime.last_input_seq(&eth), Some(None));
    assert_eq!(output.read_all().len(), 0);
}
