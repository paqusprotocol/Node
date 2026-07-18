#[allow(clippy::module_inception)] // Preserve the established public module path.
pub mod miner;

pub use miner::{
    MiningConfig, MiningResult, mine_candidate_block, mine_prepared_block, prepare_candidate_block,
};

#[cfg(test)]
mod test;
