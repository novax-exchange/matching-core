use crate::journal_adapter::{JournalInputEntry, JournalOutputAppender};
use crate::output_commit_boundary::{OutputCommitBlockDecision, OutputJournalClient};
use crate::runtime_config::{MatchingRuntimeConfig, RuntimeShardId, RuntimeTopologyConfig};
use crate::runtime_loop::{
    RuntimeLoop, RuntimeLoopError, RuntimeLoopInputStatus, RuntimeLoopRunLimit,
    RuntimeLoopRunReport, RuntimeLoopTickLimits, RuntimeLoopTickReport,
};
use crate::runtime_manager::{RuntimeManagerError, SymbolRuntimeStatus};
use crate::runtime_topology::{RuntimeTopology, RuntimeTopologyError};
use crate::types::Symbol;

pub struct RuntimeShardRunner {
    shard_id: RuntimeShardId,
    symbols: Vec<Symbol>,
    runtime_loop: RuntimeLoop,
}

impl RuntimeShardRunner {
    pub fn from_symbols_with_config(
        symbols: Vec<Symbol>,
        config: MatchingRuntimeConfig,
    ) -> Result<Vec<Self>, RuntimeTopologyError> {
        let topology = RuntimeTopology::resolve(&symbols, &config.topology)?;

        Ok(topology
            .shards()
            .iter()
            .map(|shard| {
                let symbols = shard.symbols.clone();
                let mut shard_config = config.clone();
                shard_config.topology = RuntimeTopologyConfig::default();

                Self {
                    shard_id: shard.id,
                    symbols: symbols.clone(),
                    runtime_loop: RuntimeLoop::new_for_symbols_with_config(symbols, shard_config),
                }
            })
            .collect())
    }

    pub fn shard_id(&self) -> RuntimeShardId {
        self.shard_id
    }

    pub fn symbols(&self) -> &[Symbol] {
        &self.symbols
    }

    pub fn run_tick(
        &mut self,
        journal_client: &mut OutputJournalClient,
        output: &mut dyn JournalOutputAppender,
        limits: RuntimeLoopTickLimits,
    ) -> Result<RuntimeLoopTickReport, RuntimeLoopError> {
        self.runtime_loop.run_tick(journal_client, output, limits)
    }

    pub fn run_limited(
        &mut self,
        journal_client: &mut OutputJournalClient,
        output: &mut dyn JournalOutputAppender,
        limits: RuntimeLoopTickLimits,
        limit: RuntimeLoopRunLimit,
    ) -> Result<RuntimeLoopRunReport, RuntimeLoopError> {
        self.runtime_loop
            .run_limited(journal_client, output, limits, limit)
    }

    pub fn enqueue_input(&mut self, entry: JournalInputEntry) -> Result<(), RuntimeLoopError> {
        self.runtime_loop.enqueue_input(entry)
    }

    pub fn enqueue_inputs(
        &mut self,
        entries: Vec<JournalInputEntry>,
    ) -> Result<usize, RuntimeLoopError> {
        self.runtime_loop.enqueue_inputs(entries)
    }

    pub fn pending_input_status(&self, symbol: &Symbol) -> Option<RuntimeLoopInputStatus> {
        self.runtime_loop.pending_input_status(symbol)
    }

    pub fn symbol_status(&self, symbol: &Symbol) -> Option<SymbolRuntimeStatus> {
        self.runtime_loop.symbol_status(symbol)
    }

    pub fn quarantine_symbol_output_commit_escalation(
        &mut self,
        symbol: &Symbol,
    ) -> Result<Option<OutputCommitBlockDecision>, RuntimeManagerError> {
        self.runtime_loop
            .quarantine_symbol_output_commit_escalation(symbol)
    }

    pub fn clear_symbol_output_commit_quarantine(
        &mut self,
        symbol: &Symbol,
    ) -> Result<Option<OutputCommitBlockDecision>, RuntimeManagerError> {
        self.runtime_loop
            .clear_symbol_output_commit_quarantine(symbol)
    }
}
