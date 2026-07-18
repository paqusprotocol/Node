use crate::runtime::mempool::{ExtensionMempool, Mempool};
use paqus::block::{Block, Nonce};
use paqus::consensus::{Consensus, ConsensusError};
use paqus::crypto::Address;
use paqus::ledger::Ledger;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MiningConfig {
    pub difficulty: u32,
    pub start_nonce: u64,
    pub max_attempts: u64,
    pub transaction_limit: usize,
    pub min_fee_rate: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MiningResult {
    pub block: Block,
    pub attempts: u64,
}

pub fn mine_candidate_block(
    mempool: &Mempool,
    extension_mempool: &ExtensionMempool,
    ledger: &Ledger,
    consensus: &Consensus,
    miner_address: Address,
    timestamp: u64,
    config: MiningConfig,
) -> Result<Option<MiningResult>, ConsensusError> {
    let block = prepare_candidate_block(
        mempool,
        extension_mempool,
        ledger,
        miner_address,
        timestamp,
        config.transaction_limit,
        config.min_fee_rate,
        config.difficulty,
    )?;
    mine_prepared_block(block, consensus, config)
}

#[allow(clippy::too_many_arguments)] // Consensus candidate inputs are explicit at this boundary.
pub fn prepare_candidate_block(
    mempool: &Mempool,
    extension_mempool: &ExtensionMempool,
    ledger: &Ledger,
    miner_address: Address,
    timestamp: u64,
    transaction_limit: usize,
    min_fee_rate: u64,
    difficulty: u32,
) -> Result<Block, ConsensusError> {
    let mut block = mempool
        .create_candidate_block_with_min_fee_rate(
            ledger,
            miner_address,
            timestamp,
            Nonce(0),
            transaction_limit,
            min_fee_rate,
        )
        .map_err(|_| ConsensusError::InvalidBlock(paqus::block::BlockError::InvalidStateRoot))?;
    if !extension_mempool.is_empty() {
        extension_mempool.append_selected_to_block(&mut block, transaction_limit, min_fee_rate);
        block.set_state_root(paqus::crypto::StateRoot::ZERO);
        let state_root = ledger.state_root_after_block(&block).map_err(|_| {
            ConsensusError::InvalidBlock(paqus::block::BlockError::InvalidStateRoot)
        })?;
        block.set_state_root(state_root);
    }
    block.header.difficulty = difficulty;
    Ok(block)
}

pub fn mine_prepared_block(
    mut block: Block,
    consensus: &Consensus,
    config: MiningConfig,
) -> Result<Option<MiningResult>, ConsensusError> {
    for attempt in 0..config.max_attempts {
        block.header.nonce = Nonce(config.start_nonce.wrapping_add(attempt));
        if config.difficulty == 0 {
            return Ok(Some(MiningResult {
                block,
                attempts: attempt.saturating_add(1),
            }));
        }

        let hash = consensus.proof_of_work_hash(&block)?;
        if consensus
            .validate_proof_of_work_hash_with_difficulty(&hash, config.difficulty)
            .is_ok()
        {
            return Ok(Some(MiningResult {
                block,
                attempts: attempt.saturating_add(1),
            }));
        }
    }

    Ok(None)
}
