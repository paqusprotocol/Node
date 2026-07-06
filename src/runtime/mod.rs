#![allow(dead_code, unused_imports)]

pub mod cache;
pub mod mempool;
pub mod miner;
pub mod network;
pub mod node;
pub mod storage;
pub mod wallet;

pub mod params {
    pub use paqus::block::MAX_BLOCK_TXS;
    pub use paqus::consensus::supply::{
        BLOCK_REWARD, DECIMALS, MAX_MINED_SUPPLY, MAX_UNIT_SUPPLY, TAIL_EMISSION, XPQ,
    };
    pub use paqus::consensus::{
        BLOCK_TIME, DIFFICULTY_ADJUSTMENT_INTERVAL, DIFFICULTY_START, MAX_FUTURE_TIME,
        MIN_DIFFICULTY,
    };
    pub use paqus::crypto::{ADDRESS_SIZE, HASH_SIZE};
    pub use paqus::genesis::CURRENT_CHAIN_PARAMS;
    pub use paqus::ledger::{BLOCK_REWARD_MATURITY, CONFIRMATION_DEPTH, FINALITY_DEPTH};

    pub const CHAIN_NAME: &str = CURRENT_CHAIN_PARAMS.chain_name;
    pub const CHAIN_ID: u16 = CURRENT_CHAIN_PARAMS.chain_id;
    pub const COIN_NAME: &str = CURRENT_CHAIN_PARAMS.coin_name;
    pub const PROTOCOL_STAGE: &str = CURRENT_CHAIN_PARAMS.protocol_stage;
    pub const PROTOCOL_VERSION: u8 = CURRENT_CHAIN_PARAMS.protocol_version;
    pub const NETWORK_MAGIC: [u8; 4] = CURRENT_CHAIN_PARAMS.network_magic;
    pub const GENESIS_PREMINE: u64 = 0;

    const MINUTE: u64 = 60;
    const DAY: u64 = 24 * 60 * MINUTE;

    pub const STORAGE_VERSION: u8 = 1;
    pub const MAX_RELAY_TRANSACTION_AGE_SECS: u64 = DAY;
    pub const MAX_RELAY_TRANSACTION_FUTURE_SECS: u64 = BLOCK_TIME as u64;
    pub const LOW_FEE_EXPIRY_SECS: u64 = 30 * MINUTE;
    pub const MEMPOOL_EXPIRY_SECS: u64 = DAY;
    pub const MAX_MEMPOOL_TXS: usize = 1_000;
    pub const MAX_MEMPOOL_BYTES: usize = 10 * 1024 * 1024;
    pub const MAX_NETWORK_MESSAGE_SIZE: usize = 8 * 1024 * 1024;
    pub const BASE_FEE: u64 = 2;
    pub const DEFAULT_TRANSACTION_FEE: u64 = BASE_FEE;
    pub const MIN_RELAY_FEE_FLOOR: u64 = 1;
    pub const DEFAULT_MIN_RELAY_FEE: u64 = MIN_RELAY_FEE_FLOOR;
    pub const DEFAULT_MARKET_FEE: u64 = DEFAULT_TRANSACTION_FEE;
}
