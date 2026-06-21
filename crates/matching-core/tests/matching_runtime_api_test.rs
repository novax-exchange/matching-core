use matching_core::journal_adapter::{
    JournalAdapterError, JournalInputEntry, JournalOutputAppender, JournalOutputCommitMetadata,
    JournalOutputEntry,
};
use matching_core::matching_engine::EngineEvent;
use matching_core::matching_runtime::{
    MatchingRuntime, MatchingRuntimeDrainStopReason, MatchingRuntimeError,
    MatchingRuntimeInputState, MatchingRuntimeLifecycleState, MatchingRuntimeRunStopReason,
    MatchingRuntimeRunUntilIdleLimit, MatchingRuntimeRunUntilIdleStopReason,
};
use matching_core::order::{Command, Order};
use matching_core::runtime_config::{
    MatchingRuntimeConfig, RuntimeExecutionConfig, RuntimeExecutionMode, RuntimeShardId,
    RuntimeTopologyConfig, SymbolAssignmentPolicy,
};
use matching_core::shard_runtime::{
    ShardRuntimeError, ShardRuntimeRunLimit, ShardRuntimeRunOnceLimits, ShardRuntimeRunStopReason,
};
use matching_core::types::{CommandId, JournalSeq, OrderId, Price, Quantity, Side, Symbol};

struct TestJournalOutputAppender {
    entries: Vec<JournalOutputEntry>,
}

struct RejectOneSymbolJournalOutputAppender {
    rejected_symbol: Symbol,
    entries: Vec<JournalOutputEntry>,
}

impl TestJournalOutputAppender {
    fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
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

impl JournalOutputAppender for TestJournalOutputAppender {
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
fn matching_runtime_builds_inline_runtime_from_public_api() {
    let btc = symbol("BTC-USDT");
    let eth = symbol("ETH-USDT");
    let mut config = MatchingRuntimeConfig::default();
    config.topology = RuntimeTopologyConfig {
        shard_count: 2,
        assignment_policy: SymbolAssignmentPolicy::DeclarationOrder,
    };
    config.execution = RuntimeExecutionConfig {
        mode: RuntimeExecutionMode::Inline,
        max_run_cycles_per_call: 1024,
        max_run_calls_per_until_idle: 1024,
    };

    let runtime =
        MatchingRuntime::new_for_symbols_with_config(vec![btc.clone(), eth.clone()], config)
            .expect("inline matching runtime should be supported");

    assert_eq!(runtime.mode(), RuntimeExecutionMode::Inline);
    assert_eq!(
        runtime.lifecycle_state(),
        MatchingRuntimeLifecycleState::Running
    );
    assert_eq!(runtime.shard_count(), 2);
    assert_eq!(
        runtime.shard_ids(),
        vec![RuntimeShardId(0), RuntimeShardId(1)]
    );
    assert_eq!(
        runtime.symbols_for_shard(RuntimeShardId(0)),
        Some(&[btc][..])
    );
    assert_eq!(
        runtime.symbols_for_shard(RuntimeShardId(1)),
        Some(&[eth][..])
    );
}

#[test]
fn matching_runtime_rejects_thread_mode_until_runtime_mode_exists_from_public_api() {
    let btc = symbol("BTC-USDT");
    let mut config = MatchingRuntimeConfig::default();
    config.execution = RuntimeExecutionConfig {
        mode: RuntimeExecutionMode::ThreadPerShard,
        max_run_cycles_per_call: 1024,
        max_run_calls_per_until_idle: 1024,
    };

    let result = MatchingRuntime::new_for_symbols_with_config(vec![btc], config);

    assert!(matches!(
        result,
        Err(MatchingRuntimeError::RuntimeModeUnavailable(
            RuntimeExecutionMode::ThreadPerShard
        ))
    ));
}

#[test]
fn matching_runtime_rejects_async_mode_until_runtime_mode_exists_from_public_api() {
    let btc = symbol("BTC-USDT");
    let mut config = MatchingRuntimeConfig::default();
    config.execution = RuntimeExecutionConfig {
        mode: RuntimeExecutionMode::AsyncTaskPerShard,
        max_run_cycles_per_call: 1024,
        max_run_calls_per_until_idle: 1024,
    };

    let result = MatchingRuntime::new_for_symbols_with_config(vec![btc], config);

    assert!(matches!(
        result,
        Err(MatchingRuntimeError::RuntimeModeUnavailable(
            RuntimeExecutionMode::AsyncTaskPerShard
        ))
    ));
}

#[test]
fn matching_runtime_routes_input_to_owning_shard_and_runs_once_all() {
    let btc = symbol("BTC-USDT");
    let eth = symbol("ETH-USDT");
    let mut config = MatchingRuntimeConfig::default();
    config.topology = RuntimeTopologyConfig {
        shard_count: 2,
        assignment_policy: SymbolAssignmentPolicy::DeclarationOrder,
    };
    let mut runtime =
        MatchingRuntime::new_for_symbols_with_config(vec![btc.clone(), eth.clone()], config)
            .expect("inline matching runtime should be supported");
    let mut journal_client = matching_core::output_commit_boundary::OutputJournalClient::new();
    let mut output = TestJournalOutputAppender::new();

    assert_eq!(runtime.enqueue_input(command_entry(1, eth.clone())), Ok(()));

    let report = runtime
        .run_once_all(
            &mut journal_client,
            &mut output,
            ShardRuntimeRunOnceLimits {
                max_input_entries_per_symbol: 1,
                max_output_requests_per_symbol: 1,
            },
        )
        .expect("inline matching runtime should run all shard run_once cycles");

    assert_eq!(report.shard_reports.len(), 2);
    assert_eq!(
        report
            .shard_report(RuntimeShardId(1))
            .and_then(|item| item.run_once_report.symbol_report(&eth))
            .map(|item| item.input_processed_count),
        Some(1)
    );
}

#[test]
fn matching_runtime_enqueue_inputs_routes_batch_across_shards() {
    let btc = symbol("BTC-USDT");
    let eth = symbol("ETH-USDT");
    let mut config = MatchingRuntimeConfig::default();
    config.topology = RuntimeTopologyConfig {
        shard_count: 2,
        assignment_policy: SymbolAssignmentPolicy::DeclarationOrder,
    };
    let mut runtime =
        MatchingRuntime::new_for_symbols_with_config(vec![btc.clone(), eth.clone()], config)
            .expect("inline matching runtime should be supported");

    assert_eq!(
        runtime.enqueue_inputs(vec![
            command_entry(1, btc.clone()),
            command_entry(2, eth.clone()),
            command_entry(3, btc.clone()),
        ]),
        Ok(3)
    );

    let status = runtime
        .status()
        .expect("inline matching runtime should report status");
    assert_eq!(
        status
            .shard_status(RuntimeShardId(0))
            .and_then(|item| item.symbol_status(&btc))
            .map(|item| item.pending_input_len),
        Some(2)
    );
    assert_eq!(
        status
            .shard_status(RuntimeShardId(1))
            .and_then(|item| item.symbol_status(&eth))
            .map(|item| item.pending_input_len),
        Some(1)
    );
}

#[test]
fn matching_runtime_can_enqueue_inputs_preflights_batch_without_mutating_state() {
    let btc = symbol("BTC-USDT");
    let eth = symbol("ETH-USDT");
    let mut config = MatchingRuntimeConfig::default();
    config.topology = RuntimeTopologyConfig {
        shard_count: 2,
        assignment_policy: SymbolAssignmentPolicy::DeclarationOrder,
    };
    let runtime =
        MatchingRuntime::new_for_symbols_with_config(vec![btc.clone(), eth.clone()], config)
            .expect("inline matching runtime should be supported");

    assert_eq!(
        runtime.can_enqueue_inputs(&[
            command_entry(1, btc.clone()),
            command_entry(2, eth.clone()),
            command_entry(3, btc.clone()),
        ]),
        Ok(())
    );

    let status = runtime
        .status()
        .expect("inline matching runtime should report status");
    assert!(status.is_idle());
}

#[test]
fn matching_runtime_can_enqueue_inputs_reports_capacity_without_partial_enqueue() {
    let btc = symbol("BTC-USDT");
    let eth = symbol("ETH-USDT");
    let mut config = MatchingRuntimeConfig::default();
    config.topology = RuntimeTopologyConfig {
        shard_count: 2,
        assignment_policy: SymbolAssignmentPolicy::DeclarationOrder,
    };
    config.handoff.capacity = 1;
    let runtime =
        MatchingRuntime::new_for_symbols_with_config(vec![btc.clone(), eth.clone()], config)
            .expect("inline matching runtime should be supported");

    assert_eq!(
        runtime.can_enqueue_inputs(&[
            command_entry(1, btc.clone()),
            command_entry(2, eth.clone()),
            command_entry(3, eth.clone()),
        ]),
        Err(MatchingRuntimeError::ShardRuntime(
            ShardRuntimeError::InputHandoffFull(eth.clone())
        ))
    );

    let status = runtime
        .status()
        .expect("inline matching runtime should report status");
    assert!(status.is_idle());
}

#[test]
fn matching_runtime_enqueue_inputs_rejects_batch_without_partial_enqueue_when_handoff_would_fill() {
    let btc = symbol("BTC-USDT");
    let eth = symbol("ETH-USDT");
    let mut config = MatchingRuntimeConfig::default();
    config.topology = RuntimeTopologyConfig {
        shard_count: 2,
        assignment_policy: SymbolAssignmentPolicy::DeclarationOrder,
    };
    config.handoff.capacity = 1;
    let mut runtime =
        MatchingRuntime::new_for_symbols_with_config(vec![btc.clone(), eth.clone()], config)
            .expect("inline matching runtime should be supported");

    assert_eq!(
        runtime.enqueue_inputs(vec![
            command_entry(1, btc.clone()),
            command_entry(2, eth.clone()),
            command_entry(3, eth.clone()),
        ]),
        Err(MatchingRuntimeError::ShardRuntime(
            ShardRuntimeError::InputHandoffFull(eth.clone())
        ))
    );

    let status = runtime
        .status()
        .expect("inline matching runtime should report status");
    assert!(status.is_idle());
    assert_eq!(
        status.shards_with_remaining_work(),
        Vec::<RuntimeShardId>::new()
    );
}

#[test]
fn matching_runtime_enqueue_inputs_rejects_batch_without_partial_enqueue_when_symbol_is_unknown() {
    let btc = symbol("BTC-USDT");
    let sol = symbol("SOL-USDT");
    let mut runtime = MatchingRuntime::new_for_symbols_with_config(
        vec![btc.clone()],
        MatchingRuntimeConfig::default(),
    )
    .expect("inline matching runtime should be supported");

    assert_eq!(
        runtime.enqueue_inputs(vec![
            command_entry(1, btc.clone()),
            command_entry(2, sol.clone())
        ]),
        Err(MatchingRuntimeError::ShardRuntime(
            ShardRuntimeError::UnregisteredHandoff(sol)
        ))
    );

    let status = runtime
        .status()
        .expect("inline matching runtime should report status");
    assert!(status.is_idle());
}

#[test]
fn matching_runtime_close_input_rejects_new_single_and_batch_inputs() {
    let btc = symbol("BTC-USDT");
    let mut runtime = MatchingRuntime::new_for_symbols_with_config(
        vec![btc.clone()],
        MatchingRuntimeConfig::default(),
    )
    .expect("inline matching runtime should be supported");

    assert_eq!(runtime.input_state(), MatchingRuntimeInputState::Open);
    runtime.close_input();
    assert_eq!(runtime.input_state(), MatchingRuntimeInputState::Closed);

    assert_eq!(
        runtime.enqueue_input(command_entry(1, btc.clone())),
        Err(MatchingRuntimeError::InputClosed)
    );
    assert_eq!(
        runtime.can_enqueue_inputs(&[command_entry(1, btc.clone())]),
        Err(MatchingRuntimeError::InputClosed)
    );
    assert_eq!(
        runtime.enqueue_inputs(vec![command_entry(1, btc.clone())]),
        Err(MatchingRuntimeError::InputClosed)
    );
    assert!(runtime
        .status()
        .expect("runtime status should be available")
        .is_idle());
}

#[test]
fn matching_runtime_shutdown_closes_input_without_draining_pending_work() {
    let btc = symbol("BTC-USDT");
    let mut runtime = MatchingRuntime::new_for_symbols_with_config(
        vec![btc.clone()],
        MatchingRuntimeConfig::default(),
    )
    .expect("inline matching runtime should be supported");

    assert_eq!(runtime.enqueue_input(command_entry(1, btc.clone())), Ok(()));

    let report = runtime
        .shutdown()
        .expect("inline matching runtime should shut down");

    assert_eq!(report.input_state, MatchingRuntimeInputState::Closed);
    assert_eq!(
        report.lifecycle_state,
        MatchingRuntimeLifecycleState::Shutdown
    );
    assert_eq!(report.runtime_set_report.shard_ids, vec![RuntimeShardId(0)]);
    assert_eq!(
        report.final_status.lifecycle_state,
        MatchingRuntimeLifecycleState::Shutdown
    );
    assert_eq!(
        report.final_status.input_state,
        MatchingRuntimeInputState::Closed
    );
    assert!(report.has_work_remaining());
    assert_eq!(report.shards_with_remaining_work(), vec![RuntimeShardId(0)]);
    assert_eq!(report.symbols_with_remaining_work(), vec![btc.clone()]);
    assert!(!report.has_blocked_symbols());
    assert_eq!(report.blocked_shards(), Vec::<RuntimeShardId>::new());
    assert_eq!(report.blocked_symbols(), Vec::<Symbol>::new());
    assert_eq!(runtime.input_state(), MatchingRuntimeInputState::Closed);
    assert_eq!(
        runtime.enqueue_input(command_entry(2, btc.clone())),
        Err(MatchingRuntimeError::RuntimeShutdown)
    );
    assert_eq!(
        runtime.can_enqueue_inputs(&[command_entry(2, btc.clone())]),
        Err(MatchingRuntimeError::RuntimeShutdown)
    );
    assert_eq!(
        runtime.enqueue_inputs(vec![command_entry(2, btc.clone())]),
        Err(MatchingRuntimeError::RuntimeShutdown)
    );

    let status = runtime
        .status()
        .expect("inline matching runtime status should remain available");
    assert_eq!(
        status.lifecycle_state,
        MatchingRuntimeLifecycleState::Shutdown
    );
    let symbol_status = status
        .shard_status(RuntimeShardId(0))
        .and_then(|shard_status| shard_status.symbol_status(&btc))
        .expect("BTC-USDT status should be available");
    assert_eq!(symbol_status.pending_input_len, 1);
}

#[test]
fn matching_runtime_shutdown_rejects_later_execution_without_losing_status() {
    let btc = symbol("BTC-USDT");
    let mut runtime = MatchingRuntime::new_for_symbols_with_config(
        vec![btc.clone()],
        MatchingRuntimeConfig::default(),
    )
    .expect("inline matching runtime should be supported");
    let mut journal_client = matching_core::output_commit_boundary::OutputJournalClient::new();
    let mut output = TestJournalOutputAppender::new();

    assert_eq!(runtime.enqueue_input(command_entry(1, btc.clone())), Ok(()));
    runtime
        .shutdown()
        .expect("inline matching runtime should shut down");

    assert_eq!(
        runtime.run_once_all(
            &mut journal_client,
            &mut output,
            ShardRuntimeRunOnceLimits {
                max_input_entries_per_symbol: 1,
                max_output_requests_per_symbol: 1,
            },
        ),
        Err(MatchingRuntimeError::RuntimeShutdown)
    );
    assert_eq!(
        runtime.run_configured_all(&mut journal_client, &mut output),
        Err(MatchingRuntimeError::RuntimeShutdown)
    );
    assert_eq!(
        runtime.run_until_idle_configured(&mut journal_client, &mut output),
        Err(MatchingRuntimeError::RuntimeShutdown)
    );
    assert_eq!(
        runtime.drain_configured(&mut journal_client, &mut output),
        Err(MatchingRuntimeError::RuntimeShutdown)
    );

    let status = runtime
        .status()
        .expect("inline matching runtime status should remain queryable after shutdown");
    assert_eq!(
        status.lifecycle_state,
        MatchingRuntimeLifecycleState::Shutdown
    );
    assert_eq!(status.shards_with_remaining_work(), vec![RuntimeShardId(0)]);
    assert_eq!(output.read_all().len(), 0);
}

#[test]
fn matching_runtime_shutdown_rejects_repeated_shutdown_without_losing_status() {
    let btc = symbol("BTC-USDT");
    let mut runtime = MatchingRuntime::new_for_symbols_with_config(
        vec![btc.clone()],
        MatchingRuntimeConfig::default(),
    )
    .expect("inline matching runtime should be supported");

    assert_eq!(runtime.enqueue_input(command_entry(1, btc.clone())), Ok(()));
    runtime
        .shutdown()
        .expect("inline matching runtime should shut down once");

    assert_eq!(
        runtime.shutdown(),
        Err(MatchingRuntimeError::RuntimeShutdown)
    );

    let status = runtime
        .status()
        .expect("inline matching runtime status should remain queryable after repeated shutdown");
    assert_eq!(
        status.lifecycle_state,
        MatchingRuntimeLifecycleState::Shutdown
    );
    assert_eq!(status.shards_with_remaining_work(), vec![RuntimeShardId(0)]);
}

#[test]
fn matching_runtime_drain_configured_closes_input_and_drains_existing_work() {
    let btc = symbol("BTC-USDT");
    let mut config = MatchingRuntimeConfig::default();
    config.execution.max_run_cycles_per_call = 1;
    config.execution.max_run_calls_per_until_idle = 4;
    config.symbol_runtime.max_input_entries_per_step = 1;
    config.output_commit.max_output_requests_per_step = 1;
    let mut runtime = MatchingRuntime::new_for_symbols_with_config(vec![btc.clone()], config)
        .expect("inline matching runtime should be supported");
    let mut journal_client = matching_core::output_commit_boundary::OutputJournalClient::new();
    let mut output = TestJournalOutputAppender::new();

    assert_eq!(runtime.enqueue_input(command_entry(1, btc.clone())), Ok(()));
    assert_eq!(runtime.enqueue_input(command_entry(2, btc.clone())), Ok(()));

    let report = runtime
        .drain_configured(&mut journal_client, &mut output)
        .expect("inline matching runtime should drain with configured limits");

    assert_eq!(runtime.input_state(), MatchingRuntimeInputState::Closed);
    assert_eq!(report.stop_reason, MatchingRuntimeDrainStopReason::Drained);
    assert!(report.is_drained());
    assert!(!report.has_work_remaining());
    assert_eq!(
        report.shards_with_remaining_work(),
        Vec::<RuntimeShardId>::new()
    );
    assert_eq!(report.configured_run_count(), 2);
    assert_eq!(output.read_all().len(), 2);
    assert_eq!(
        runtime.enqueue_input(command_entry(3, btc.clone())),
        Err(MatchingRuntimeError::InputClosed)
    );
}

#[test]
fn matching_runtime_drain_configured_reports_blocked_output() {
    let btc = symbol("BTC-USDT");
    let mut runtime = MatchingRuntime::new_for_symbols_with_config(
        vec![btc.clone()],
        MatchingRuntimeConfig::default(),
    )
    .expect("inline matching runtime should be supported");
    let mut journal_client = matching_core::output_commit_boundary::OutputJournalClient::new();
    let mut output = RejectOneSymbolJournalOutputAppender::new(btc.clone());

    assert_eq!(runtime.enqueue_input(command_entry(1, btc.clone())), Ok(()));

    let report = runtime
        .drain_configured(&mut journal_client, &mut output)
        .expect("inline matching runtime should report blocked drain");

    assert_eq!(runtime.input_state(), MatchingRuntimeInputState::Closed);
    assert_eq!(report.stop_reason, MatchingRuntimeDrainStopReason::Blocked);
    assert!(report.has_work_remaining());
    assert!(report.has_blocked_symbols());
    assert_eq!(report.shards_with_remaining_work(), vec![RuntimeShardId(0)]);
    assert_eq!(report.blocked_shards(), vec![RuntimeShardId(0)]);
    assert_eq!(report.symbols_with_remaining_work(), vec![btc.clone()]);
    assert_eq!(report.blocked_symbols(), vec![btc.clone()]);
}

#[test]
fn matching_runtime_run_limited_all_drives_all_shards_until_idle() {
    let btc = symbol("BTC-USDT");
    let eth = symbol("ETH-USDT");
    let mut config = MatchingRuntimeConfig::default();
    config.topology = RuntimeTopologyConfig {
        shard_count: 2,
        assignment_policy: SymbolAssignmentPolicy::DeclarationOrder,
    };
    let mut runtime =
        MatchingRuntime::new_for_symbols_with_config(vec![btc.clone(), eth.clone()], config)
            .expect("inline matching runtime should be supported");
    let mut journal_client = matching_core::output_commit_boundary::OutputJournalClient::new();
    let mut output = TestJournalOutputAppender::new();

    assert_eq!(runtime.enqueue_input(command_entry(1, btc.clone())), Ok(()));
    assert_eq!(runtime.enqueue_input(command_entry(2, eth.clone())), Ok(()));

    let report = runtime
        .run_limited_all(
            &mut journal_client,
            &mut output,
            ShardRuntimeRunOnceLimits {
                max_input_entries_per_symbol: 1,
                max_output_requests_per_symbol: 1,
            },
            ShardRuntimeRunLimit { max_cycles: 2 },
        )
        .expect("inline matching runtime should run all shard run limits");

    assert_eq!(report.shard_reports.len(), 2);
    assert!(report.is_idle());
    assert_eq!(report.stop_reason(), MatchingRuntimeRunStopReason::Idle);
    assert_eq!(
        report.idle_shards(),
        vec![RuntimeShardId(0), RuntimeShardId(1)]
    );
    assert_eq!(
        report.shards_with_remaining_work(),
        Vec::<RuntimeShardId>::new()
    );
    assert_eq!(report.blocked_shards(), Vec::<RuntimeShardId>::new());
    assert_eq!(
        report.shards_reaching_run_limit(),
        Vec::<RuntimeShardId>::new()
    );
    assert!(!report.needs_another_run());
    assert_eq!(
        report
            .shard_report(RuntimeShardId(0))
            .map(|item| item.run_report.stop_reason),
        Some(ShardRuntimeRunStopReason::Idle)
    );
    assert_eq!(
        report
            .shard_report(RuntimeShardId(1))
            .map(|item| item.run_report.stop_reason),
        Some(ShardRuntimeRunStopReason::Idle)
    );
}

#[test]
fn matching_runtime_run_report_summarizes_mixed_shard_states() {
    let btc = symbol("BTC-USDT");
    let eth = symbol("ETH-USDT");
    let mut config = MatchingRuntimeConfig::default();
    config.topology = RuntimeTopologyConfig {
        shard_count: 2,
        assignment_policy: SymbolAssignmentPolicy::DeclarationOrder,
    };
    let mut runtime =
        MatchingRuntime::new_for_symbols_with_config(vec![btc.clone(), eth.clone()], config)
            .expect("inline matching runtime should be supported");
    let mut journal_client = matching_core::output_commit_boundary::OutputJournalClient::new();
    let mut output = RejectOneSymbolJournalOutputAppender::new(btc.clone());

    assert_eq!(runtime.enqueue_input(command_entry(1, btc.clone())), Ok(()));
    assert_eq!(runtime.enqueue_input(command_entry(1, eth.clone())), Ok(()));
    assert_eq!(runtime.enqueue_input(command_entry(2, eth.clone())), Ok(()));
    assert_eq!(runtime.enqueue_input(command_entry(3, eth.clone())), Ok(()));

    let report = runtime
        .run_limited_all(
            &mut journal_client,
            &mut output,
            ShardRuntimeRunOnceLimits {
                max_input_entries_per_symbol: 1,
                max_output_requests_per_symbol: 1,
            },
            ShardRuntimeRunLimit { max_cycles: 2 },
        )
        .expect("inline matching runtime should summarize shard run states");

    assert!(!report.is_idle());
    assert_eq!(
        report.stop_reason(),
        MatchingRuntimeRunStopReason::RunLimitReached
    );
    assert!(report.has_work_remaining());
    assert!(report.has_blocked_symbols());
    assert!(report.needs_another_run());
    assert_eq!(report.idle_shards(), Vec::<RuntimeShardId>::new());
    assert_eq!(
        report.shards_with_remaining_work(),
        vec![RuntimeShardId(0), RuntimeShardId(1)]
    );
    assert_eq!(report.blocked_shards(), vec![RuntimeShardId(0)]);
    assert_eq!(
        report.symbols_with_remaining_work(),
        vec![btc.clone(), eth.clone()]
    );
    assert_eq!(report.blocked_symbols(), vec![btc.clone()]);
    assert_eq!(report.shards_reaching_run_limit(), vec![RuntimeShardId(1)]);
}

#[test]
fn matching_runtime_run_report_reports_blocked_when_no_shard_can_progress() {
    let btc = symbol("BTC-USDT");
    let mut runtime = MatchingRuntime::new_for_symbols_with_config(
        vec![btc.clone()],
        MatchingRuntimeConfig::default(),
    )
    .expect("inline matching runtime should be supported");
    let mut journal_client = matching_core::output_commit_boundary::OutputJournalClient::new();
    let mut output = RejectOneSymbolJournalOutputAppender::new(btc.clone());

    assert_eq!(runtime.enqueue_input(command_entry(1, btc.clone())), Ok(()));

    let report = runtime
        .run_limited_all(
            &mut journal_client,
            &mut output,
            ShardRuntimeRunOnceLimits {
                max_input_entries_per_symbol: 1,
                max_output_requests_per_symbol: 1,
            },
            ShardRuntimeRunLimit { max_cycles: 2 },
        )
        .expect("inline matching runtime should report blocked shard execution");

    assert_eq!(report.stop_reason(), MatchingRuntimeRunStopReason::Blocked);
    assert!(report.has_work_remaining());
    assert!(report.has_blocked_symbols());
    assert!(!report.needs_another_run());
    assert_eq!(report.blocked_shards(), vec![RuntimeShardId(0)]);
    assert_eq!(report.symbols_with_remaining_work(), vec![btc.clone()]);
    assert_eq!(report.blocked_symbols(), vec![btc.clone()]);
}

#[test]
fn matching_runtime_run_configured_all_uses_runtime_config_limits() {
    let btc = symbol("BTC-USDT");
    let mut config = MatchingRuntimeConfig::default();
    config.execution.max_run_cycles_per_call = 1;
    config.symbol_runtime.max_input_entries_per_step = 1;
    config.output_commit.max_output_requests_per_step = 1;
    let mut runtime = MatchingRuntime::new_for_symbols_with_config(vec![btc.clone()], config)
        .expect("inline matching runtime should be supported");
    let mut journal_client = matching_core::output_commit_boundary::OutputJournalClient::new();
    let mut output = TestJournalOutputAppender::new();

    assert_eq!(runtime.enqueue_input(command_entry(1, btc.clone())), Ok(()));
    assert_eq!(runtime.enqueue_input(command_entry(2, btc.clone())), Ok(()));

    let report = runtime
        .run_configured_all(&mut journal_client, &mut output)
        .expect("inline matching runtime should run with configured limits");

    assert!(report.needs_another_run());
    assert_eq!(report.shards_reaching_run_limit(), vec![RuntimeShardId(0)]);
    assert_eq!(output.read_all().len(), 1);
}

#[test]
fn matching_runtime_run_until_idle_repeats_configured_runs_until_idle() {
    let btc = symbol("BTC-USDT");
    let mut config = MatchingRuntimeConfig::default();
    config.execution.max_run_cycles_per_call = 1;
    config.symbol_runtime.max_input_entries_per_step = 1;
    config.output_commit.max_output_requests_per_step = 1;
    let mut runtime = MatchingRuntime::new_for_symbols_with_config(vec![btc.clone()], config)
        .expect("inline matching runtime should be supported");
    let mut journal_client = matching_core::output_commit_boundary::OutputJournalClient::new();
    let mut output = TestJournalOutputAppender::new();

    assert_eq!(runtime.enqueue_input(command_entry(1, btc.clone())), Ok(()));
    assert_eq!(runtime.enqueue_input(command_entry(2, btc.clone())), Ok(()));
    assert_eq!(runtime.enqueue_input(command_entry(3, btc.clone())), Ok(()));

    let report = runtime
        .run_until_idle(
            &mut journal_client,
            &mut output,
            MatchingRuntimeRunUntilIdleLimit { max_run_calls: 4 },
        )
        .expect("inline matching runtime should run configured calls until idle");

    assert_eq!(
        report.stop_reason,
        MatchingRuntimeRunUntilIdleStopReason::Idle
    );
    assert_eq!(report.configured_run_count(), 3);
    assert!(report.is_idle());
    assert_eq!(output.read_all().len(), 3);
}

#[test]
fn matching_runtime_run_until_idle_reports_call_limit_before_idle() {
    let btc = symbol("BTC-USDT");
    let mut config = MatchingRuntimeConfig::default();
    config.execution.max_run_cycles_per_call = 1;
    config.symbol_runtime.max_input_entries_per_step = 1;
    config.output_commit.max_output_requests_per_step = 1;
    let mut runtime = MatchingRuntime::new_for_symbols_with_config(vec![btc.clone()], config)
        .expect("inline matching runtime should be supported");
    let mut journal_client = matching_core::output_commit_boundary::OutputJournalClient::new();
    let mut output = TestJournalOutputAppender::new();

    assert_eq!(runtime.enqueue_input(command_entry(1, btc.clone())), Ok(()));
    assert_eq!(runtime.enqueue_input(command_entry(2, btc.clone())), Ok(()));
    assert_eq!(runtime.enqueue_input(command_entry(3, btc.clone())), Ok(()));

    let report = runtime
        .run_until_idle(
            &mut journal_client,
            &mut output,
            MatchingRuntimeRunUntilIdleLimit { max_run_calls: 2 },
        )
        .expect("inline matching runtime should stop at outer call limit");

    assert_eq!(
        report.stop_reason,
        MatchingRuntimeRunUntilIdleStopReason::CallLimitReached
    );
    assert_eq!(report.configured_run_count(), 2);
    assert!(report.has_work_remaining());
    assert_eq!(output.read_all().len(), 2);
}

#[test]
fn matching_runtime_run_until_idle_reports_final_status_when_no_run_is_allowed() {
    let btc = symbol("BTC-USDT");
    let mut runtime = MatchingRuntime::new_for_symbols_with_config(
        vec![btc.clone()],
        MatchingRuntimeConfig::default(),
    )
    .expect("inline matching runtime should be supported");
    let mut journal_client = matching_core::output_commit_boundary::OutputJournalClient::new();
    let mut output = TestJournalOutputAppender::new();

    assert_eq!(runtime.enqueue_input(command_entry(1, btc.clone())), Ok(()));

    let report = runtime
        .run_until_idle(
            &mut journal_client,
            &mut output,
            MatchingRuntimeRunUntilIdleLimit { max_run_calls: 0 },
        )
        .expect("inline matching runtime should report final status without running");

    assert_eq!(
        report.stop_reason,
        MatchingRuntimeRunUntilIdleStopReason::CallLimitReached
    );
    assert_eq!(report.configured_run_count(), 0);
    assert!(report.has_work_remaining());
    assert_eq!(report.shards_with_remaining_work(), vec![RuntimeShardId(0)]);
    assert_eq!(
        report.final_status.input_state,
        MatchingRuntimeInputState::Open
    );
    assert_eq!(output.read_all().len(), 0);
}

#[test]
fn matching_runtime_run_until_idle_stops_when_only_blocked_work_remains() {
    let btc = symbol("BTC-USDT");
    let mut config = MatchingRuntimeConfig::default();
    config.execution.max_run_cycles_per_call = 2;
    config.symbol_runtime.max_input_entries_per_step = 1;
    config.output_commit.max_output_requests_per_step = 1;
    let mut runtime = MatchingRuntime::new_for_symbols_with_config(vec![btc.clone()], config)
        .expect("inline matching runtime should be supported");
    let mut journal_client = matching_core::output_commit_boundary::OutputJournalClient::new();
    let mut output = RejectOneSymbolJournalOutputAppender::new(btc.clone());

    assert_eq!(runtime.enqueue_input(command_entry(1, btc.clone())), Ok(()));

    let report = runtime
        .run_until_idle(
            &mut journal_client,
            &mut output,
            MatchingRuntimeRunUntilIdleLimit { max_run_calls: 3 },
        )
        .expect("inline matching runtime should stop when output remains blocked");

    assert_eq!(
        report.stop_reason,
        MatchingRuntimeRunUntilIdleStopReason::Blocked
    );
    assert_eq!(report.configured_run_count(), 1);
    assert!(report.has_blocked_symbols());
    assert_eq!(report.blocked_shards(), vec![RuntimeShardId(0)]);
    assert_eq!(report.symbols_with_remaining_work(), vec![btc.clone()]);
    assert_eq!(report.blocked_symbols(), vec![btc.clone()]);
}

#[test]
fn matching_runtime_run_until_idle_configured_uses_runtime_config_limit() {
    let btc = symbol("BTC-USDT");
    let mut config = MatchingRuntimeConfig::default();
    config.execution.max_run_cycles_per_call = 1;
    config.execution.max_run_calls_per_until_idle = 2;
    config.symbol_runtime.max_input_entries_per_step = 1;
    config.output_commit.max_output_requests_per_step = 1;
    let mut runtime = MatchingRuntime::new_for_symbols_with_config(vec![btc.clone()], config)
        .expect("inline matching runtime should be supported");
    let mut journal_client = matching_core::output_commit_boundary::OutputJournalClient::new();
    let mut output = TestJournalOutputAppender::new();

    assert_eq!(runtime.enqueue_input(command_entry(1, btc.clone())), Ok(()));
    assert_eq!(runtime.enqueue_input(command_entry(2, btc.clone())), Ok(()));
    assert_eq!(runtime.enqueue_input(command_entry(3, btc.clone())), Ok(()));

    let report = runtime
        .run_until_idle_configured(&mut journal_client, &mut output)
        .expect("inline matching runtime should run until idle with configured limit");

    assert_eq!(
        report.stop_reason,
        MatchingRuntimeRunUntilIdleStopReason::CallLimitReached
    );
    assert_eq!(report.configured_run_count(), 2);
    assert_eq!(output.read_all().len(), 2);
}

#[test]
fn matching_runtime_status_reports_pending_input_without_running() {
    let btc = symbol("BTC-USDT");
    let eth = symbol("ETH-USDT");
    let mut config = MatchingRuntimeConfig::default();
    config.topology = RuntimeTopologyConfig {
        shard_count: 2,
        assignment_policy: SymbolAssignmentPolicy::DeclarationOrder,
    };
    let mut runtime =
        MatchingRuntime::new_for_symbols_with_config(vec![btc.clone(), eth.clone()], config)
            .expect("inline matching runtime should be supported");

    assert_eq!(runtime.enqueue_input(command_entry(1, eth.clone())), Ok(()));

    let status = runtime
        .status()
        .expect("inline matching runtime should report status");

    assert_eq!(status.input_state, MatchingRuntimeInputState::Open);
    assert!(!status.is_idle());
    assert_eq!(status.shards_with_remaining_work(), vec![RuntimeShardId(1)]);
    assert_eq!(status.blocked_shards(), Vec::<RuntimeShardId>::new());
    assert_eq!(status.symbols_with_remaining_work(), vec![eth.clone()]);
    assert_eq!(status.blocked_symbols(), Vec::<Symbol>::new());

    let eth_status = status
        .shard_status(RuntimeShardId(1))
        .and_then(|item| item.symbol_status(&eth))
        .expect("eth status should be available from owning shard");
    assert_eq!(eth_status.pending_input_len, 1);
    assert_eq!(eth_status.pending_output_len, 0);
    assert!(!eth_status.output_commit_blocked);
}

#[test]
fn matching_runtime_status_reports_closed_input_state() {
    let btc = symbol("BTC-USDT");
    let mut runtime =
        MatchingRuntime::new_for_symbols_with_config(vec![btc], MatchingRuntimeConfig::default())
            .expect("inline matching runtime should be supported");

    runtime.close_input();

    let status = runtime
        .status()
        .expect("inline matching runtime should report status");

    assert_eq!(status.input_state, MatchingRuntimeInputState::Closed);
    assert_eq!(
        status.lifecycle_state,
        MatchingRuntimeLifecycleState::Running
    );
    assert!(status.is_idle());
}

#[test]
fn matching_runtime_status_reports_blocked_output_pressure() {
    let btc = symbol("BTC-USDT");
    let mut runtime = MatchingRuntime::new_for_symbols_with_config(
        vec![btc.clone()],
        MatchingRuntimeConfig::default(),
    )
    .expect("inline matching runtime should be supported");
    let mut journal_client = matching_core::output_commit_boundary::OutputJournalClient::new();
    let mut output = RejectOneSymbolJournalOutputAppender::new(btc.clone());

    assert_eq!(runtime.enqueue_input(command_entry(1, btc.clone())), Ok(()));

    let run_report = runtime
        .run_configured_all(&mut journal_client, &mut output)
        .expect("inline matching runtime should run with configured limits");
    assert!(run_report.has_blocked_symbols());

    let status = runtime
        .status()
        .expect("inline matching runtime should report status");

    assert!(!status.is_idle());
    assert!(status.has_blocked_symbols());
    assert_eq!(status.blocked_shards(), vec![RuntimeShardId(0)]);
    assert_eq!(status.symbols_with_remaining_work(), vec![btc.clone()]);
    assert_eq!(status.blocked_symbols(), vec![btc.clone()]);

    let btc_status = status
        .shard_status(RuntimeShardId(0))
        .and_then(|item| item.symbol_status(&btc))
        .expect("btc status should be available from owning shard");
    assert_eq!(btc_status.pending_input_len, 0);
    assert_eq!(btc_status.pending_output_len, 1);
    assert!(btc_status.output_commit_blocked);
}

#[test]
fn matching_runtime_status_summarizes_full_input_and_output_pressure() {
    let btc = symbol("BTC-USDT");
    let eth = symbol("ETH-USDT");
    let mut config = MatchingRuntimeConfig::default();
    config.topology = RuntimeTopologyConfig {
        shard_count: 2,
        assignment_policy: SymbolAssignmentPolicy::DeclarationOrder,
    };
    config.handoff.capacity = 1;
    config.output_commit.pending_output_capacity = 1;
    config.symbol_runtime.max_input_entries_per_step = 1;
    config.output_commit.max_output_requests_per_step = 1;
    let mut runtime =
        MatchingRuntime::new_for_symbols_with_config(vec![btc.clone(), eth.clone()], config)
            .expect("inline matching runtime should be supported");
    let mut journal_client = matching_core::output_commit_boundary::OutputJournalClient::new();
    let mut output = RejectOneSymbolJournalOutputAppender::new(btc.clone());

    assert_eq!(runtime.enqueue_input(command_entry(1, btc.clone())), Ok(()));
    let run_report = runtime
        .run_configured_all(&mut journal_client, &mut output)
        .expect("inline matching runtime should run with configured limits");
    assert!(run_report.has_blocked_symbols());
    assert_eq!(runtime.enqueue_input(command_entry(1, eth.clone())), Ok(()));

    let status = runtime
        .status()
        .expect("inline matching runtime should report status");

    assert_eq!(status.shards_with_full_input(), vec![RuntimeShardId(1)]);
    assert_eq!(status.shards_with_full_output(), vec![RuntimeShardId(0)]);
    assert_eq!(status.symbols_with_full_input(), vec![eth.clone()]);
    assert_eq!(status.symbols_with_full_output(), vec![btc.clone()]);
}

#[test]
fn matching_runtime_run_limited_all_reports_remaining_work_when_limit_is_reached() {
    let btc = symbol("BTC-USDT");
    let mut runtime = MatchingRuntime::new_for_symbols_with_config(
        vec![btc.clone()],
        MatchingRuntimeConfig::default(),
    )
    .expect("inline matching runtime should be supported");
    let mut journal_client = matching_core::output_commit_boundary::OutputJournalClient::new();
    let mut output = TestJournalOutputAppender::new();

    assert_eq!(runtime.enqueue_input(command_entry(1, btc.clone())), Ok(()));
    assert_eq!(runtime.enqueue_input(command_entry(2, btc.clone())), Ok(()));

    let report = runtime
        .run_limited_all(
            &mut journal_client,
            &mut output,
            ShardRuntimeRunOnceLimits {
                max_input_entries_per_symbol: 1,
                max_output_requests_per_symbol: 1,
            },
            ShardRuntimeRunLimit { max_cycles: 1 },
        )
        .expect("inline matching runtime should run all shard run limits");

    assert!(!report.is_idle());
    assert_eq!(
        report.stop_reason(),
        MatchingRuntimeRunStopReason::RunLimitReached
    );
    assert!(report.has_work_remaining());
    assert_eq!(
        report
            .shard_report(RuntimeShardId(0))
            .map(|item| item.run_report.stop_reason),
        Some(ShardRuntimeRunStopReason::RunLimitReached)
    );
}
