#![allow(dead_code, unused_imports)]

pub mod cache;
pub mod mempool;
pub mod miner;
pub mod network;
pub mod node;
pub mod storage;
pub mod wallet;

pub mod params {
    pub use paqus::params::*;

    pub const STORAGE_VERSION: u8 = 1;
    pub const MEMPOOL_EXPIRY_SECS: u64 = 30 * paqus::params::MINUTE as u64;
    pub const MAX_MEMPOOL_TXS: usize = 1_000;
    pub const MAX_MEMPOOL_BYTES: usize = 10 * 1024 * 1024;
    pub const MAX_NETWORK_MESSAGE_SIZE: usize = 8 * 1024 * 1024;
}
