use crate::runtime_config::{
    RuntimeShardId, RuntimeTopologyConfig, SymbolAssignmentPolicy, SymbolShardAssignment,
};
use crate::types::Symbol;
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeTopology {
    shards: Vec<RuntimeShard>,
}

impl RuntimeTopology {
    pub fn resolve(
        symbols: &[Symbol],
        config: &RuntimeTopologyConfig,
    ) -> Result<Self, RuntimeTopologyError> {
        if config.shard_count == 0 {
            return Err(RuntimeTopologyError::ZeroShardCount);
        }

        let mut shard_symbols = vec![Vec::new(); config.shard_count];

        match &config.assignment_policy {
            SymbolAssignmentPolicy::DeclarationOrder => {
                for (index, symbol) in symbols.iter().enumerate() {
                    let shard_index = index % config.shard_count;
                    shard_symbols[shard_index].push(symbol.clone());
                }
            }
            SymbolAssignmentPolicy::StableHash => {
                for symbol in symbols {
                    let shard_index = stable_symbol_hash(symbol) % config.shard_count;
                    shard_symbols[shard_index].push(symbol.clone());
                }
            }
            SymbolAssignmentPolicy::ExplicitMap(assignments) => {
                resolve_explicit_map(symbols, assignments, config.shard_count, &mut shard_symbols)?;
            }
        }

        Ok(Self {
            shards: shard_symbols
                .into_iter()
                .enumerate()
                .map(|(index, symbols)| RuntimeShard {
                    id: RuntimeShardId(index),
                    symbols,
                })
                .collect(),
        })
    }

    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }

    pub fn shards(&self) -> &[RuntimeShard] {
        &self.shards
    }

    pub fn symbols_for_shard(&self, shard_id: RuntimeShardId) -> Option<&[Symbol]> {
        self.shards
            .get(shard_id.0)
            .map(|shard| shard.symbols.as_slice())
    }

    pub fn shard_for_symbol(&self, symbol: &Symbol) -> Option<RuntimeShardId> {
        self.shards
            .iter()
            .find(|shard| shard.symbols.iter().any(|candidate| candidate == symbol))
            .map(|shard| shard.id)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeShard {
    pub id: RuntimeShardId,
    pub symbols: Vec<Symbol>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RuntimeTopologyError {
    ZeroShardCount,
    DuplicateSymbolAssignment(Symbol),
    MissingSymbolAssignment(Symbol),
    UnknownSymbolAssignment(Symbol),
    ShardIdOutOfRange(RuntimeShardId),
}

fn resolve_explicit_map(
    symbols: &[Symbol],
    assignments: &[SymbolShardAssignment],
    shard_count: usize,
    shard_symbols: &mut [Vec<Symbol>],
) -> Result<(), RuntimeTopologyError> {
    let known_symbols: HashSet<Symbol> = symbols.iter().cloned().collect();
    let mut assigned_symbols = HashMap::new();

    for assignment in assignments {
        if assignment.shard_id.0 >= shard_count {
            return Err(RuntimeTopologyError::ShardIdOutOfRange(assignment.shard_id));
        }

        if !known_symbols.contains(&assignment.symbol) {
            return Err(RuntimeTopologyError::UnknownSymbolAssignment(
                assignment.symbol.clone(),
            ));
        }

        if assigned_symbols
            .insert(assignment.symbol.clone(), assignment.shard_id)
            .is_some()
        {
            return Err(RuntimeTopologyError::DuplicateSymbolAssignment(
                assignment.symbol.clone(),
            ));
        }
    }

    for symbol in symbols {
        let shard_id = assigned_symbols
            .get(symbol)
            .ok_or_else(|| RuntimeTopologyError::MissingSymbolAssignment(symbol.clone()))?;
        shard_symbols[shard_id.0].push(symbol.clone());
    }

    Ok(())
}

fn stable_symbol_hash(symbol: &Symbol) -> usize {
    let mut hash: u64 = 0xcbf29ce484222325;

    for byte in symbol.0.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }

    hash as usize
}

#[cfg(test)]
mod tests {
    use super::*;

    fn symbol(value: &str) -> Symbol {
        Symbol(value.to_string())
    }

    #[test]
    fn stable_symbol_hash_returns_same_value_for_same_symbol() {
        assert_eq!(
            stable_symbol_hash(&symbol("BTC-USDT")),
            stable_symbol_hash(&symbol("BTC-USDT"))
        );
    }
}
