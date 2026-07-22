use crate::runtime::mempool::{ExtensionMempool, Mempool};
use paqus::block::{Block, Nonce};
use paqus::consensus::{Consensus, ConsensusError};
use paqus::crypto::Address;
use paqus::genesis::{GenesisConfig, create_genesis_block};
use paqus::ledger::{Ledger, LedgerError};

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
    if ledger.tip_height().is_none() {
        let mut genesis = create_genesis_block(GenesisConfig {
            miner_address,
            timestamp,
        });
        genesis.header.difficulty = difficulty;
        return Ok(genesis);
    }
    let mut block = mempool
        .create_candidate_block_with_min_fee_rate(
            ledger,
            miner_address,
            timestamp,
            Nonce(0),
            transaction_limit,
            min_fee_rate,
        )
        .map_err(ledger_to_consensus_error)?;
    if !extension_mempool.is_empty() {
        extension_mempool.append_selected_to_block(&mut block, transaction_limit, min_fee_rate);
        block.set_state_root(paqus::crypto::StateRoot::ZERO);
    }
    block.header.difficulty = difficulty;
    block.set_state_root(paqus::crypto::StateRoot::ZERO);
    let (_, execution) = ledger
        .execute_block(&block)
        .map_err(ledger_to_consensus_error)?;
    block.set_state_root(execution.state_root_after);
    Ok(block)
}

fn ledger_to_consensus_error(error: LedgerError) -> ConsensusError {
    match error {
        LedgerError::InvalidConsensus(error) => error,
        LedgerError::InvalidBlock(error) => ConsensusError::InvalidBlock(error),
        LedgerError::InvalidBlockHeight => ConsensusError::InvalidHeight,
        LedgerError::InvalidPreviousHash | LedgerError::InvalidParent => {
            ConsensusError::InvalidPreviousHash
        }
        LedgerError::InvalidTimestamp => ConsensusError::InvalidTimestamp,
        LedgerError::InvalidStateRoot => {
            ConsensusError::InvalidBlock(paqus::block::BlockError::InvalidStateRoot)
        }
        _ => ConsensusError::InvalidBlock(paqus::block::BlockError::InvalidStateRoot),
    }
}

pub fn mine_prepared_block(
    block: Block,
    consensus: &Consensus,
    config: MiningConfig,
) -> Result<Option<MiningResult>, ConsensusError> {
    mine_prepared_block_until(block, consensus, config, || false)
}

pub fn mine_prepared_block_until(
    block: Block,
    consensus: &Consensus,
    config: MiningConfig,
    should_stop: impl Fn() -> bool,
) -> Result<Option<MiningResult>, ConsensusError> {
    mine_prepared_block_until_with_attempts(block, consensus, config, should_stop)
        .map(|(result, _attempts)| result)
}

pub fn mine_prepared_block_until_with_attempts(
    mut block: Block,
    consensus: &Consensus,
    config: MiningConfig,
    should_stop: impl Fn() -> bool,
) -> Result<(Option<MiningResult>, u64), ConsensusError> {
    let max_attempts = if config.max_attempts == 0 {
        u64::MAX
    } else {
        config.max_attempts
    };
    for attempt in 0..max_attempts {
        if attempt % 1024 == 0 && should_stop() {
            return Ok((None, attempt));
        }
        block.header.nonce = Nonce(config.start_nonce.wrapping_add(attempt));
        if config.difficulty == 0 {
            let attempts = attempt.saturating_add(1);
            return Ok((Some(MiningResult { block, attempts }), attempts));
        }

        let hash = consensus.proof_of_work_hash(&block)?;
        if consensus
            .validate_proof_of_work_hash_with_difficulty(&hash, config.difficulty)
            .is_ok()
        {
            let attempts = attempt.saturating_add(1);
            return Ok((Some(MiningResult { block, attempts }), attempts));
        }
    }

    Ok((None, max_attempts))
}
