use matching_core::journal_adapter::{
    JournalAdapterError, JournalInputEntry, JournalOutputAppender, JournalOutputEntry,
};
use matching_core::matching_engine::EngineEvent;
use matching_core::order::{Command, Order};
use matching_core::runtime_config::{
    MatchingRuntimeConfig, RuntimeShardId, RuntimeTopologyConfig, SymbolAssignmentPolicy,
    SymbolShardAssignment,
};
use matching_core::runtime_topology::RuntimeTopologyError;
use matching_core::shard_runtime::{ShardRuntime, ShardRuntimeError, ShardRuntimeRunOnceLimits};
use matching_core::types::{CommandId, JournalSeq, OrderId, Price, Quantity, Side, Symbol};

struct TestJournalOutputAppender {
    entries: Vec<JournalOutputEntry>,
}

impl TestJournalOutputAppender {
    fn new() -> Self {
        Self {
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
fn shard_runtime_builds_single_default_runtime_from_public_api() {
    let btc = symbol("BTC-USDT");
    let eth = symbol("ETH-USDT");

    let runtimes = ShardRuntime::from_symbols_with_config(
        vec![btc.clone(), eth.clone()],
        MatchingRuntimeConfig::default(),
    )
    .expect("default shard runtime topology should resolve");

    assert_eq!(runtimes.len(), 1);
    assert_eq!(runtimes[0].shard_id(), RuntimeShardId(0));
    assert_eq!(runtimes[0].symbols(), &[btc, eth]);
}

#[test]
fn shard_runtime_builds_runtimes_from_declaration_order_topology() {
    let btc = symbol("BTC-USDT");
    let eth = symbol("ETH-USDT");
    let sol = symbol("SOL-USDT");
    let mut config = MatchingRuntimeConfig::default();
    config.topology = RuntimeTopologyConfig {
        shard_count: 2,
        assignment_policy: SymbolAssignmentPolicy::DeclarationOrder,
    };

    let runtimes =
        ShardRuntime::from_symbols_with_config(vec![btc.clone(), eth.clone(), sol.clone()], config)
            .expect("declaration-order shard runtime topology should resolve");

    assert_eq!(runtimes.len(), 2);
    assert_eq!(runtimes[0].shard_id(), RuntimeShardId(0));
    assert_eq!(runtimes[0].symbols(), &[btc, sol]);
    assert_eq!(runtimes[1].shard_id(), RuntimeShardId(1));
    assert_eq!(runtimes[1].symbols(), &[eth]);
}

#[test]
fn shard_runtime_rejects_inputs_for_symbols_owned_by_other_shards() {
    let btc = symbol("BTC-USDT");
    let eth = symbol("ETH-USDT");
    let mut config = MatchingRuntimeConfig::default();
    config.topology = RuntimeTopologyConfig {
        shard_count: 2,
        assignment_policy: SymbolAssignmentPolicy::DeclarationOrder,
    };

    let mut runtimes =
        ShardRuntime::from_symbols_with_config(vec![btc.clone(), eth.clone()], config)
            .expect("declaration-order shard runtime topology should resolve");

    assert_eq!(
        runtimes[0].enqueue_input(command_entry(1, eth.clone())),
        Err(ShardRuntimeError::UnregisteredHandoff(eth))
    );
    assert_eq!(
        runtimes[1].enqueue_input(command_entry(1, btc.clone())),
        Err(ShardRuntimeError::UnregisteredHandoff(btc))
    );
}

#[test]
fn shard_runtime_delegates_run_once_execution_to_underlying_shard_runtime() {
    let btc = symbol("BTC-USDT");
    let mut runtimes =
        ShardRuntime::from_symbols_with_config(vec![btc.clone()], MatchingRuntimeConfig::default())
            .expect("default shard runtime topology should resolve");
    let mut journal_client = matching_core::output_commit_boundary::OutputJournalClient::new();
    let mut output = TestJournalOutputAppender::new();

    assert_eq!(
        runtimes[0].enqueue_input(command_entry(1, btc.clone())),
        Ok(())
    );
    let report = runtimes[0]
        .run_once(
            &mut journal_client,
            &mut output,
            ShardRuntimeRunOnceLimits {
                max_input_entries_per_symbol: 1,
                max_output_requests_per_symbol: 1,
            },
        )
        .expect("shard runtime run_once should execute");

    assert_eq!(
        report
            .symbol_report(&btc)
            .map(|item| item.input_processed_count),
        Some(1)
    );
}

#[test]
fn shard_runtime_propagates_topology_resolution_errors() {
    let btc = symbol("BTC-USDT");
    let eth = symbol("ETH-USDT");
    let mut config = MatchingRuntimeConfig::default();
    config.topology = RuntimeTopologyConfig {
        shard_count: 2,
        assignment_policy: SymbolAssignmentPolicy::ExplicitMap(vec![SymbolShardAssignment {
            symbol: btc.clone(),
            shard_id: RuntimeShardId(0),
        }]),
    };

    let result = ShardRuntime::from_symbols_with_config(vec![btc, eth.clone()], config);

    assert!(matches!(
        result,
        Err(RuntimeTopologyError::MissingSymbolAssignment(symbol)) if symbol == eth
    ));
}
