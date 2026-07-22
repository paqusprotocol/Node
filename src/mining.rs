use crate::command::config::RunConfig;
use crate::command::display::{pow_target_description, short_hash};
use crate::runtime::miner::{
    MiningConfig, mine_prepared_block_until_with_attempts, prepare_candidate_block,
};
use crate::runtime::node::Node;
use crate::runtime::params::MAX_BLOCK_TXS;
use paqus::block::Block;
use paqus::crypto::BlockHash;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[derive(Default)]
pub struct MiningStats {
    pub last_hashrate_hps: AtomicU64,
    pub last_attempts: AtomicU64,
    next_nonce: AtomicU64,
}

const UNLIMITED_MINE_NONCE_RESERVATION: u64 = u64::MAX;

pub fn mine_once(
    node_state: &Arc<Mutex<Node>>,
    config: &RunConfig,
    mining_stats: &MiningStats,
    shutdown_requested: &AtomicBool,
) -> Result<Option<Block>, String> {
    let now = unix_timestamp()?;
    let (candidate, consensus, mining_config) = {
        let mut node = node_state
            .lock()
            .map_err(|_| "node state lock poisoned".to_string())?;
        node.mempool.prune_expired(now);
        let timestamp = candidate_timestamp(&node, now);
        let difficulty = node.next_difficulty().map_err(|error| error.to_string())?;
        let mempool_len = node.mempool.len() + node.extension_mempool.len();
        let miner_min_fee_rate = config
            .miner_min_fee_rate
            .unwrap_or_else(|| node.mempool.dynamic_market_fee_rate());
        println!(
            "[POW] algo=sha3-512 difficulty_bits={} target=\"{}\"",
            difficulty,
            pow_target_description(difficulty)
        );
        println!(
            "[MEMPOOL] txs={} miner_min_fee_rate_per_byte={}",
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
                    .fetch_add(nonce_reservation(config.mine_attempts), Ordering::Relaxed),
                max_attempts: config.mine_attempts,
                transaction_limit: MAX_BLOCK_TXS,
                min_fee_rate: miner_min_fee_rate,
            },
        )
    };

    let mining_genesis = candidate.is_genesis();
    let parent_hash = BlockHash::from(candidate.previous_hash().as_hash());
    let started = Instant::now();
    let rebuild_deadline = (config.mine_attempts == 0)
        .then(|| started.checked_add(config.mine_interval).unwrap_or(started));
    let (mined, attempted) =
        mine_prepared_block_until_with_attempts(candidate, &consensus, mining_config, || {
            shutdown_requested.load(Ordering::Relaxed)
                || rebuild_deadline.is_some_and(|deadline| Instant::now() >= deadline)
        })
        .map_err(|error| format!("mining failed: {error}"))?;
    let elapsed = started.elapsed();
    let Some(result) = mined else {
        update_stats(mining_stats, attempted, elapsed);
        let result = if shutdown_requested.load(Ordering::Relaxed) {
            "stopped"
        } else if rebuild_deadline.is_some() {
            "rebuild"
        } else {
            "exhausted"
        };
        println!(
            "[MINE] result={} start_nonce={} attempts={} elapsed_ms={}",
            result,
            mining_config.start_nonce,
            attempted,
            elapsed.as_millis()
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
        println!("[MINE] result=discarded reason=tip_changed");
        return Ok(None);
    }
    node.apply_block(result.block.clone())
        .map_err(|error| format!("failed to apply mined block: {error}"))?;
    node.flush_to_storage()
        .map_err(|error| format!("failed to flush mined block: {error}"))?;
    println!(
        "[BLOCK] mined height={} hash={} difficulty={} txs={} attempts={} timestamp={}",
        result.block.height().0,
        short_hash(Some(result.block.hash())),
        result.block.difficulty(),
        result.block.transactions.len(),
        result.attempts,
        result.block.timestamp()
    );
    Ok(Some(result.block))
}

fn candidate_timestamp(node: &Node, now: u64) -> u64 {
    let Some(tip_height) = node.ledger.tip_height() else {
        return now;
    };
    node.ledger
        .block(&tip_height)
        .map(|tip| now.max(tip.timestamp().saturating_add(1)))
        .unwrap_or(now)
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

fn nonce_reservation(mine_attempts: u64) -> u64 {
    if mine_attempts == 0 {
        UNLIMITED_MINE_NONCE_RESERVATION
    } else {
        mine_attempts
    }
}

fn unix_timestamp() -> Result<u64, String> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .map_err(|_| "system clock is before unix epoch".to_string())
}
