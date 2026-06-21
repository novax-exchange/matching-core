use matching_core::journal_adapter::{
    JournalAdapterError, JournalInputEntry, JournalOutputAppender, JournalOutputCommitMetadata,
    JournalOutputEntry,
};
use matching_core::matching_engine::EngineEvent;
use matching_core::order::{Command, Order};
use matching_core::runtime_config::{
    MatchingRuntimeConfig, RuntimeHostConfig, RuntimeHostMode, RuntimeShardId,
    RuntimeTopologyConfig, SymbolAssignmentPolicy,
};
use matching_core::runtime_host::{RuntimeHost, RuntimeHostError};
use matching_core::runtime_loop::{
    RuntimeLoopRunLimit, RuntimeLoopRunOnceLimits, RuntimeLoopRunStopReason,
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
fn runtime_host_builds_manual_host_from_public_api() {
    let btc = symbol("BTC-USDT");
    let eth = symbol("ETH-USDT");
    let mut config = MatchingRuntimeConfig::default();
    config.topology = RuntimeTopologyConfig {
        shard_count: 2,
        assignment_policy: SymbolAssignmentPolicy::DeclarationOrder,
    };
    config.host = RuntimeHostConfig {
        mode: RuntimeHostMode::Manual,
    };

    let host = RuntimeHost::new_for_symbols_with_config(vec![btc.clone(), eth.clone()], config)
        .expect("manual runtime host should be supported");

    assert_eq!(host.mode(), RuntimeHostMode::Manual);
    assert_eq!(host.shard_count(), 2);
    assert_eq!(host.shard_ids(), vec![RuntimeShardId(0), RuntimeShardId(1)]);
    assert_eq!(host.symbols_for_shard(RuntimeShardId(0)), Some(&[btc][..]));
    assert_eq!(host.symbols_for_shard(RuntimeShardId(1)), Some(&[eth][..]));
}

#[test]
fn runtime_host_rejects_inline_until_inline_scheduling_exists_from_public_api() {
    let btc = symbol("BTC-USDT");
    let mut config = MatchingRuntimeConfig::default();
    config.host = RuntimeHostConfig {
        mode: RuntimeHostMode::Inline,
    };

    let result = RuntimeHost::new_for_symbols_with_config(vec![btc], config);

    assert!(matches!(
        result,
        Err(RuntimeHostError::UnsupportedMode(RuntimeHostMode::Inline))
    ));
}

#[test]
fn runtime_host_rejects_unsupported_host_modes_from_public_api() {
    let btc = symbol("BTC-USDT");
    let mut config = MatchingRuntimeConfig::default();
    config.host = RuntimeHostConfig {
        mode: RuntimeHostMode::ThreadPerShard,
    };

    let result = RuntimeHost::new_for_symbols_with_config(vec![btc], config);

    assert!(matches!(
        result,
        Err(RuntimeHostError::UnsupportedMode(
            RuntimeHostMode::ThreadPerShard
        ))
    ));
}

#[test]
fn runtime_host_routes_input_to_owning_shard_and_runs_once_all() {
    let btc = symbol("BTC-USDT");
    let eth = symbol("ETH-USDT");
    let mut config = MatchingRuntimeConfig::default();
    config.topology = RuntimeTopologyConfig {
        shard_count: 2,
        assignment_policy: SymbolAssignmentPolicy::DeclarationOrder,
    };
    let mut host = RuntimeHost::new_for_symbols_with_config(vec![btc.clone(), eth.clone()], config)
        .expect("manual runtime host should be supported");
    let mut journal_client = matching_core::output_commit_boundary::OutputJournalClient::new();
    let mut output = TestJournalOutputAppender::new();

    assert_eq!(host.enqueue_input(command_entry(1, eth.clone())), Ok(()));

    let report = host
        .run_once_all(
            &mut journal_client,
            &mut output,
            RuntimeLoopRunOnceLimits {
                max_input_entries_per_symbol: 1,
                max_output_requests_per_symbol: 1,
            },
        )
        .expect("manual runtime host should run all shard run_once cycles");

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
fn runtime_host_run_limited_all_drives_all_shards_until_idle() {
    let btc = symbol("BTC-USDT");
    let eth = symbol("ETH-USDT");
    let mut config = MatchingRuntimeConfig::default();
    config.topology = RuntimeTopologyConfig {
        shard_count: 2,
        assignment_policy: SymbolAssignmentPolicy::DeclarationOrder,
    };
    let mut host = RuntimeHost::new_for_symbols_with_config(vec![btc.clone(), eth.clone()], config)
        .expect("manual runtime host should be supported");
    let mut journal_client = matching_core::output_commit_boundary::OutputJournalClient::new();
    let mut output = TestJournalOutputAppender::new();

    assert_eq!(host.enqueue_input(command_entry(1, btc.clone())), Ok(()));
    assert_eq!(host.enqueue_input(command_entry(2, eth.clone())), Ok(()));

    let report = host
        .run_limited_all(
            &mut journal_client,
            &mut output,
            RuntimeLoopRunOnceLimits {
                max_input_entries_per_symbol: 1,
                max_output_requests_per_symbol: 1,
            },
            RuntimeLoopRunLimit { max_cycles: 2 },
        )
        .expect("manual runtime host should run all shard run limits");

    assert_eq!(report.shard_reports.len(), 2);
    assert!(report.is_idle());
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
        Some(RuntimeLoopRunStopReason::Idle)
    );
    assert_eq!(
        report
            .shard_report(RuntimeShardId(1))
            .map(|item| item.run_report.stop_reason),
        Some(RuntimeLoopRunStopReason::Idle)
    );
}

#[test]
fn runtime_host_run_report_summarizes_mixed_shard_states() {
    let btc = symbol("BTC-USDT");
    let eth = symbol("ETH-USDT");
    let mut config = MatchingRuntimeConfig::default();
    config.topology = RuntimeTopologyConfig {
        shard_count: 2,
        assignment_policy: SymbolAssignmentPolicy::DeclarationOrder,
    };
    let mut host = RuntimeHost::new_for_symbols_with_config(vec![btc.clone(), eth.clone()], config)
        .expect("manual runtime host should be supported");
    let mut journal_client = matching_core::output_commit_boundary::OutputJournalClient::new();
    let mut output = RejectOneSymbolJournalOutputAppender::new(btc.clone());

    assert_eq!(host.enqueue_input(command_entry(1, btc.clone())), Ok(()));
    assert_eq!(host.enqueue_input(command_entry(1, eth.clone())), Ok(()));
    assert_eq!(host.enqueue_input(command_entry(2, eth.clone())), Ok(()));
    assert_eq!(host.enqueue_input(command_entry(3, eth.clone())), Ok(()));

    let report = host
        .run_limited_all(
            &mut journal_client,
            &mut output,
            RuntimeLoopRunOnceLimits {
                max_input_entries_per_symbol: 1,
                max_output_requests_per_symbol: 1,
            },
            RuntimeLoopRunLimit { max_cycles: 2 },
        )
        .expect("manual runtime host should summarize shard run states");

    assert!(!report.is_idle());
    assert!(report.has_work_remaining());
    assert!(report.has_blocked_symbols());
    assert!(report.needs_another_run());
    assert_eq!(report.idle_shards(), Vec::<RuntimeShardId>::new());
    assert_eq!(
        report.shards_with_remaining_work(),
        vec![RuntimeShardId(0), RuntimeShardId(1)]
    );
    assert_eq!(report.blocked_shards(), vec![RuntimeShardId(0)]);
    assert_eq!(report.shards_reaching_run_limit(), vec![RuntimeShardId(1)]);
}

#[test]
fn runtime_host_run_limited_all_reports_remaining_work_when_limit_is_reached() {
    let btc = symbol("BTC-USDT");
    let mut host = RuntimeHost::new_for_symbols_with_config(
        vec![btc.clone()],
        MatchingRuntimeConfig::default(),
    )
    .expect("manual runtime host should be supported");
    let mut journal_client = matching_core::output_commit_boundary::OutputJournalClient::new();
    let mut output = TestJournalOutputAppender::new();

    assert_eq!(host.enqueue_input(command_entry(1, btc.clone())), Ok(()));
    assert_eq!(host.enqueue_input(command_entry(2, btc.clone())), Ok(()));

    let report = host
        .run_limited_all(
            &mut journal_client,
            &mut output,
            RuntimeLoopRunOnceLimits {
                max_input_entries_per_symbol: 1,
                max_output_requests_per_symbol: 1,
            },
            RuntimeLoopRunLimit { max_cycles: 1 },
        )
        .expect("manual runtime host should run all shard run limits");

    assert!(!report.is_idle());
    assert!(report.has_work_remaining());
    assert_eq!(
        report
            .shard_report(RuntimeShardId(0))
            .map(|item| item.run_report.stop_reason),
        Some(RuntimeLoopRunStopReason::RunLimitReached)
    );
}
