use matching_core::matching_engine::EngineEvent;
use matching_core::output_commit_boundary::OutputJournalClient;
use matching_core::runtime_config::{
    MatchingRuntimeConfig, RuntimeShardId, RuntimeTopologyConfig, SymbolAssignmentPolicy,
};
use matching_core::shard_runtime::ShardRuntimeError;
use matching_core::shard_runtime::ShardRuntimeRunOnceLimits;
use matching_core::shard_runtime_set::{
    InlineShardRuntimeSet, InputHandoffWritePlan, InputHandoffWriter, ShardRuntimeSet,
    ShardRuntimeSetError, ShardWorkerRuntimeSet,
};
use matching_core::types::{CommandId, JournalSeq, OrderId, Price, Quantity, Side, Symbol};
use matching_core::{
    journal_adapter::{
        JournalAdapterError, JournalInputEntry, JournalOutputAppender, JournalOutputCommitMetadata,
        JournalOutputEntry,
    },
    order::{Command, Order},
};
use std::sync::{Arc, Mutex};

#[derive(Clone)]
struct SharedJournalOutputAppender {
    entries: Arc<Mutex<Vec<JournalOutputEntry>>>,
}

impl SharedJournalOutputAppender {
    fn new(entries: Arc<Mutex<Vec<JournalOutputEntry>>>) -> Self {
        Self { entries }
    }
}

impl JournalOutputAppender for SharedJournalOutputAppender {
    fn append(
        &mut self,
        command_id: CommandId,
        journal_seq: JournalSeq,
        events: Vec<EngineEvent>,
    ) -> Result<(), JournalAdapterError> {
        self.entries
            .lock()
            .expect("shared output entries lock should be available")
            .push(JournalOutputEntry {
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
        self.entries
            .lock()
            .expect("shared output entries lock should be available")
            .push(JournalOutputEntry {
                command_id,
                journal_seq,
                events,
                output_commit_metadata: Some(metadata),
            });

        Ok(())
    }

    fn read_all(&self) -> Vec<JournalOutputEntry> {
        self.entries
            .lock()
            .expect("shared output entries lock should be available")
            .clone()
    }
}

fn symbol(value: &str) -> Symbol {
    Symbol(value.to_string())
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

#[test]
fn inline_shard_runtime_set_builds_shards_from_runtime_topology_from_public_api() {
    let btc = symbol("BTC-USDT");
    let eth = symbol("ETH-USDT");
    let sol = symbol("SOL-USDT");
    let mut config = MatchingRuntimeConfig::default();
    config.topology = RuntimeTopologyConfig {
        shard_count: 2,
        assignment_policy: SymbolAssignmentPolicy::DeclarationOrder,
    };

    let runtime_set = InlineShardRuntimeSet::from_symbols_with_config(
        vec![btc.clone(), eth.clone(), sol.clone()],
        config,
    )
    .expect("inline matching runtime runtime_set topology should resolve");

    assert_eq!(runtime_set.shard_count(), 2);
    assert_eq!(
        runtime_set.shard_ids(),
        vec![RuntimeShardId(0), RuntimeShardId(1)]
    );
    assert_eq!(
        runtime_set.symbols_for_shard(RuntimeShardId(0)),
        Some(&[btc, sol][..])
    );
    assert_eq!(
        runtime_set.symbols_for_shard(RuntimeShardId(1)),
        Some(&[eth][..])
    );
}

#[test]
fn shard_worker_runtime_set_builds_shards_from_runtime_topology_from_public_api() {
    let btc = symbol("BTC-USDT");
    let eth = symbol("ETH-USDT");
    let sol = symbol("SOL-USDT");
    let mut config = MatchingRuntimeConfig::default();
    config.execution.mode = matching_core::runtime_config::RuntimeExecutionMode::ShardWorker;
    config.topology = RuntimeTopologyConfig {
        shard_count: 2,
        assignment_policy: SymbolAssignmentPolicy::DeclarationOrder,
    };

    let runtime_set = ShardWorkerRuntimeSet::from_symbols_with_config(
        vec![btc.clone(), eth.clone(), sol.clone()],
        config,
    )
    .expect("shard-worker matching runtime runtime_set topology should resolve");

    assert_eq!(runtime_set.shard_count(), 2);
    assert_eq!(
        runtime_set.shard_ids(),
        vec![RuntimeShardId(0), RuntimeShardId(1)]
    );
    assert_eq!(
        runtime_set.symbols_for_shard(RuntimeShardId(0)),
        Some(&[btc.clone(), sol.clone()][..])
    );
    assert_eq!(
        runtime_set.symbols_for_shard(RuntimeShardId(1)),
        Some(&[eth.clone()][..])
    );

    assert_eq!(runtime_set.worker_count(), 2);
    assert_eq!(
        runtime_set.worker_symbols_for_shard(RuntimeShardId(0)),
        Some(&[btc.clone(), sol.clone()][..])
    );
    assert_eq!(
        runtime_set.worker_symbols_for_shard(RuntimeShardId(1)),
        Some(&[eth.clone()][..])
    );
}

#[test]
fn inline_shard_runtime_set_wraps_shard_runtime_errors_from_public_api() {
    let btc = symbol("BTC-USDT");
    let eth = symbol("ETH-USDT");
    let mut runtime_set = InlineShardRuntimeSet::from_symbols_with_config(
        vec![btc],
        MatchingRuntimeConfig::default(),
    )
    .expect("inline matching runtime runtime_set topology should resolve");

    assert_eq!(
        runtime_set.write_input(command_entry(1, eth.clone())),
        Err(ShardRuntimeSetError::ShardRuntime(
            ShardRuntimeError::UnregisteredHandoff(eth)
        ))
    );
}

#[test]
fn shard_worker_runtime_set_wraps_shard_runtime_errors_from_public_api() {
    let btc = symbol("BTC-USDT");
    let eth = symbol("ETH-USDT");
    let mut config = MatchingRuntimeConfig::default();
    config.execution.mode = matching_core::runtime_config::RuntimeExecutionMode::ShardWorker;
    let mut runtime_set = ShardWorkerRuntimeSet::from_symbols_with_config(vec![btc], config)
        .expect("shard-worker matching runtime runtime_set topology should resolve");

    assert_eq!(
        runtime_set.write_input(command_entry(1, eth.clone())),
        Err(ShardRuntimeSetError::ShardRuntime(
            ShardRuntimeError::UnregisteredHandoff(eth)
        ))
    );
}

#[test]
fn inline_shard_runtime_set_plans_input_handoff_writes_by_owning_shard_from_public_api() {
    let btc = symbol("BTC-USDT");
    let eth = symbol("ETH-USDT");
    let sol = symbol("SOL-USDT");
    let mut config = MatchingRuntimeConfig::default();
    config.topology = RuntimeTopologyConfig {
        shard_count: 2,
        assignment_policy: SymbolAssignmentPolicy::DeclarationOrder,
    };
    let runtime_set = InlineShardRuntimeSet::from_symbols_with_config(
        vec![btc.clone(), eth.clone(), sol.clone()],
        config,
    )
    .expect("inline matching runtime runtime_set topology should resolve");
    let btc_entry = command_entry(1, btc);
    let eth_entry = command_entry(2, eth);
    let sol_entry = command_entry(3, sol);

    let plans = runtime_set
        .plan_writes(&[btc_entry.clone(), eth_entry.clone(), sol_entry.clone()])
        .expect("inline matching runtime runtime_set should plan input handoff writes");

    assert_eq!(
        plans,
        vec![
            InputHandoffWritePlan::WriteInputs {
                shard_id: RuntimeShardId(0),
                entries: vec![btc_entry, sol_entry],
            },
            InputHandoffWritePlan::WriteInputs {
                shard_id: RuntimeShardId(1),
                entries: vec![eth_entry],
            },
        ]
    );
}

#[test]
fn shard_worker_runtime_set_plans_input_handoff_writes_by_owning_shard_from_public_api() {
    let btc = symbol("BTC-USDT");
    let eth = symbol("ETH-USDT");
    let sol = symbol("SOL-USDT");
    let mut config = MatchingRuntimeConfig::default();
    config.execution.mode = matching_core::runtime_config::RuntimeExecutionMode::ShardWorker;
    config.topology = RuntimeTopologyConfig {
        shard_count: 2,
        assignment_policy: SymbolAssignmentPolicy::DeclarationOrder,
    };
    let runtime_set = ShardWorkerRuntimeSet::from_symbols_with_config(
        vec![btc.clone(), eth.clone(), sol.clone()],
        config,
    )
    .expect("shard-worker matching runtime runtime_set should resolve");
    let btc_entry = command_entry(1, btc);
    let eth_entry = command_entry(2, eth);
    let sol_entry = command_entry(3, sol);

    let plans = runtime_set
        .plan_writes(&[btc_entry.clone(), eth_entry.clone(), sol_entry.clone()])
        .expect("shard-worker matching runtime runtime_set should plan input handoff writes");

    assert_eq!(
        plans,
        vec![
            InputHandoffWritePlan::WriteInputs {
                shard_id: RuntimeShardId(0),
                entries: vec![btc_entry, sol_entry],
            },
            InputHandoffWritePlan::WriteInputs {
                shard_id: RuntimeShardId(1),
                entries: vec![eth_entry],
            },
        ]
    );
}

#[test]
fn shard_worker_runtime_set_can_own_per_shard_output_writers_from_public_api() {
    let btc = symbol("BTC-USDT");
    let eth = symbol("ETH-USDT");
    let mut config = MatchingRuntimeConfig::default();
    config.execution.mode = matching_core::runtime_config::RuntimeExecutionMode::ShardWorker;
    config.topology = RuntimeTopologyConfig {
        shard_count: 2,
        assignment_policy: SymbolAssignmentPolicy::DeclarationOrder,
    };
    let shard_zero_entries = Arc::new(Mutex::new(Vec::new()));
    let shard_one_entries = Arc::new(Mutex::new(Vec::new()));
    let mut runtime_set = ShardWorkerRuntimeSet::from_symbols_with_config_and_output_factory(
        vec![btc.clone(), eth.clone()],
        config,
        {
            let shard_zero_entries = Arc::clone(&shard_zero_entries);
            let shard_one_entries = Arc::clone(&shard_one_entries);

            move |shard_id| match shard_id {
                RuntimeShardId(0) => Box::new(SharedJournalOutputAppender::new(Arc::clone(
                    &shard_zero_entries,
                ))) as Box<dyn JournalOutputAppender + Send>,
                RuntimeShardId(1) => Box::new(SharedJournalOutputAppender::new(Arc::clone(
                    &shard_one_entries,
                ))) as Box<dyn JournalOutputAppender + Send>,
                _ => panic!("unexpected shard id"),
            }
        },
    )
    .expect("shard-worker matching runtime runtime_set should resolve");
    let external_entries = Arc::new(Mutex::new(Vec::new()));
    let mut external_output = SharedJournalOutputAppender::new(Arc::clone(&external_entries));
    let mut external_journal_client = OutputJournalClient::new();

    runtime_set
        .write_inputs(vec![
            command_entry(1, btc.clone()),
            command_entry(2, eth.clone()),
        ])
        .expect("shard-worker runtime set should accept inputs");
    runtime_set
        .run_once_all(
            &mut external_journal_client,
            &mut external_output,
            ShardRuntimeRunOnceLimits {
                max_input_entries_per_symbol: 1,
                max_output_requests_per_symbol: 10,
            },
        )
        .expect("shard-worker runtime set should run with owned shard outputs");

    assert_eq!(external_output.read_all().len(), 0);
    assert_eq!(
        shard_zero_entries
            .lock()
            .expect("shard 0 output entries lock should be available")
            .len(),
        1
    );
    assert_eq!(
        shard_one_entries
            .lock()
            .expect("shard 1 output entries lock should be available")
            .len(),
        1
    );

    let shard_zero_entry = shard_zero_entries
        .lock()
        .expect("shard 0 output entries lock should be available")[0]
        .clone();
    let shard_one_entry = shard_one_entries
        .lock()
        .expect("shard 1 output entries lock should be available")[0]
        .clone();
    let shard_zero_metadata = shard_zero_entry
        .output_commit_metadata
        .expect("shard 0 output should carry commit metadata");
    let shard_one_metadata = shard_one_entry
        .output_commit_metadata
        .expect("shard 1 output should carry commit metadata");

    assert_eq!(shard_zero_metadata.shard_id, Some(RuntimeShardId(0)));
    assert_eq!(shard_zero_metadata.shard_sequence, Some(1));
    assert_eq!(shard_one_metadata.shard_id, Some(RuntimeShardId(1)));
    assert_eq!(shard_one_metadata.shard_sequence, Some(1));
}

#[test]
fn shard_worker_runtime_set_with_owned_outputs_shuts_down_worker_threads() {
    let btc = symbol("BTC-USDT");
    let eth = symbol("ETH-USDT");
    let mut config = MatchingRuntimeConfig::default();
    config.execution.mode = matching_core::runtime_config::RuntimeExecutionMode::ShardWorker;
    config.topology = RuntimeTopologyConfig {
        shard_count: 2,
        assignment_policy: SymbolAssignmentPolicy::DeclarationOrder,
    };
    let shard_zero_entries = Arc::new(Mutex::new(Vec::new()));
    let shard_one_entries = Arc::new(Mutex::new(Vec::new()));
    let mut runtime_set = ShardWorkerRuntimeSet::from_symbols_with_config_and_output_factory(
        vec![btc.clone(), eth.clone()],
        config,
        {
            let shard_zero_entries = Arc::clone(&shard_zero_entries);
            let shard_one_entries = Arc::clone(&shard_one_entries);

            move |shard_id| match shard_id {
                RuntimeShardId(0) => Box::new(SharedJournalOutputAppender::new(Arc::clone(
                    &shard_zero_entries,
                ))) as Box<dyn JournalOutputAppender + Send>,
                RuntimeShardId(1) => Box::new(SharedJournalOutputAppender::new(Arc::clone(
                    &shard_one_entries,
                ))) as Box<dyn JournalOutputAppender + Send>,
                _ => panic!("unexpected shard id"),
            }
        },
    )
    .expect("shard-worker matching runtime runtime_set should resolve");

    let shutdown_report = runtime_set
        .shutdown()
        .expect("shard-worker runtime set should shut down worker threads");

    assert_eq!(
        shutdown_report.shard_ids,
        vec![RuntimeShardId(0), RuntimeShardId(1)]
    );
}
