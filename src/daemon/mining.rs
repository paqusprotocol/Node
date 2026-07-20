use crate::command::config::RunConfig;
use crate::command::display::{pow_target_description, short_hash};
use crate::runtime::miner::{MiningConfig, mine_prepared_block, prepare_candidate_block};
use crate::runtime::node::Node;
use crate::runtime::params::MAX_BLOCK_TXS;
use paqus::block::Block;
use paqus::crypto::BlockHash;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[derive(Default)]
pub struct MiningStats {
    pub last_hashrate_hps: AtomicU64,
    pub last_attempts: AtomicU64,
    next_nonce: AtomicU64,
}

pub fn mine_once(
    node_state: &Arc<Mutex<Node>>,
    config: &RunConfig,
    mining_stats: &MiningStats,
) -> Result<Option<Block>, String> {
    let timestamp = unix_timestamp()?;
    let (candidate, consensus, mining_config) = {
        let mut node = node_state
            .lock()
            .map_err(|_| "node state lock poisoned".to_string())?;
        node.mempool.prune_expired(timestamp);
        let difficulty = node.next_difficulty().map_err(|error| error.to_string())?;
        let mempool_len = node.mempool.len() + node.extension_mempool.len();
        let miner_min_fee_rate = config
            .miner_min_fee_rate
            .unwrap_or_else(|| node.mempool.dynamic_market_fee_rate());
        println!(
            "pow:: |algo::sha3-512|difficulty_bits::{}|target::{}|",
            difficulty,
            pow_target_description(difficulty)
        );
        println!(
            "mempool:: |txs::{}|miner_min_fee_rate_per_byte::{}|",
            mempool_len, miner_min_fee_rate
        );
        let candidate = prepare_candidate_block(
            &node.mempool,
            &node.extension_mempool,
            &node.ledger,
            config.miner_address,
            timestamp,
            MAX_BLOCK_TXS,
            miner_min_fee_rate,
            difficulty,
        )
        .map_err(|error| format!("failed to prepare mining candidate: {error}"))?;
        (
            candidate,
            node.consensus,
            MiningConfig {
                difficulty,
                start_nonce: mining_stats
                    .next_nonce
                    .fetch_add(config.mine_attempts, Ordering::Relaxed),
                max_attempts: config.mine_attempts,
                transaction_limit: MAX_BLOCK_TXS,
                min_fee_rate: miner_min_fee_rate,
            },
        )
    };

    let mining_genesis = candidate.is_genesis();
    let parent_hash = BlockHash::from(candidate.previous_hash().as_hash());
    let started = Instant::now();
    let mined = mine_prepared_block(candidate, &consensus, mining_config)
        .map_err(|error| format!("mining failed: {error}"))?;
    let elapsed = started.elapsed();
    let Some(result) = mined else {
        update_stats(mining_stats, mining_config.max_attempts, elapsed);
        println!(
            "mining batch:: |result::exhausted|start_nonce::{}|attempts::{}|",
            mining_config.start_nonce, mining_config.max_attempts
        );
        return Ok(None);
    };
    update_stats(mining_stats, result.attempts, elapsed);

    let mut node = node_state
        .lock()
        .map_err(|_| "node state lock poisoned".to_string())?;
    let candidate_still_extends_tip = if mining_genesis {
        node.tip_hash().is_none()
    } else {
        node.tip_hash() == Some(parent_hash)
    };
    if !candidate_still_extends_tip {
        println!("mining discarded:: |reason::tip_changed|");
        return Ok(None);
    }
    node.apply_block(result.block.clone())
        .map_err(|error| format!("failed to apply mined block: {error}"))?;
    node.flush_to_storage()
        .map_err(|error| format!("failed to flush mined block: {error}"))?;
    println!(
        "mined:: |height::{}|hash::{}|difficulty::{}|txs::{}|attempts::{}|timestamp::{}|",
        result.block.height().0,
        short_hash(Some(result.block.hash())),
        result.block.difficulty(),
        result.block.transactions.len(),
        result.attempts,
        result.block.timestamp()
    );
    Ok(Some(result.block))
}

fn update_stats(mining_stats: &MiningStats, attempts: u64, elapsed: Duration) {
    let elapsed_nanos = elapsed.as_nanos().max(1);
    let hashrate =
        ((attempts as u128) * 1_000_000_000u128 / elapsed_nanos).min(u64::MAX as u128) as u64;
    mining_stats
        .last_hashrate_hps
        .store(hashrate, Ordering::Relaxed);
    mining_stats
        .last_attempts
        .store(attempts, Ordering::Relaxed);
}

fn unix_timestamp() -> Result<u64, String> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .map_err(|_| "system clock is before unix epoch".to_string())
}
