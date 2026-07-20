use crate::runtime::params::{BLOCK_TIME, CHAIN_NAME, COIN_NAME, PROTOCOL_STAGE, PROTOCOL_VERSION};
use paqus::consensus::DIFFICULTY_START;
use paqus::crypto::Hash;
use paqus::ledger::{CONFIRMATION_DEPTH, FINALITY_DEPTH};

pub fn format_hash<T>(hash: Option<T>) -> String
where
    T: Into<Hash>,
{
    hash.map(|hash| hex::encode(hash.into().0))
        .unwrap_or_else(|| "none".to_string())
}

pub fn short_hash<T>(hash: Option<T>) -> String
where
    T: Into<Hash>,
{
    let hash = format_hash(hash);
    if hash.len() <= 16 {
        return hash;
    }
    format!("{}..{}", &hash[..8], &hash[hash.len() - 8..])
}

pub fn format_difficulty(difficulty: Result<u32, impl std::fmt::Display>) -> String {
    difficulty
        .map(|difficulty| difficulty.to_string())
        .unwrap_or_else(|error| format!("error:{error}"))
}

pub fn pow_target_description(difficulty: u32) -> String {
    if difficulty == 0 {
        return "disabled_for_test".to_string();
    }
    let zero_bytes = difficulty / 8;
    let zero_bits = difficulty % 8;
    if zero_bits == 0 {
        format!("hash_prefix_zero_bytes>={zero_bytes}")
    } else {
        let mask = 0xff_u8 << (8 - zero_bits);
        format!(
            "hash_prefix_zero_bytes>={zero_bytes},next_byte_mask=0x{mask:02x},leading_zero_bits>={difficulty}"
        )
    }
}

pub fn print_help() {
    println!(
        "\
paqusd

Usage:
  paqusd                         Run the node daemon with default config
  paqusd --help
  paqusd version
  paqusd node info
  paqusd node config [config-path]
  paqusd node init [db-path] [miner-address]
  paqusd node db check [db-path]
  paqusd node db backup <db-path> <backup-path>
  paqusd node db restore <backup-path> <db-path>
  paqusd node run [db-path] [--config path] [--listen addr] [--rpc-listen addr] [--peer addr] [--peers-file path] [--gateway host:port] [--public-addr host:port] [--min-relay-fee paqus-per-byte] [--market-fee paqus-per-byte] [--miner-min-fee-rate paqus-per-byte] [--low-fee-expiry-secs n] [--mempool-expiry-secs n] [--wallet path] [--miner address] [--miner-secret-key key-hex] [--mine]

RPC:
  GET  /status
  GET  /health
  GET  /metrics
  GET  /chain
  GET  /stats
  GET  /peers
  GET  /balance/<address>
  GET  /blocks/latest
  GET  /blocks/<height>
  GET  /blocks/hash/<block-hash>
  GET  /tx/<tx-hash>
  GET  /address/<address>
  GET  /accounts
  GET  /mempool
  GET  /qcash/mempool
  POST /tx              JSON: {{\"tx\":\"signed-transaction-hex\"}}
  POST /qcash/tx        JSON: {{\"tx\":\"signed-qcash-transaction-hex\"}}

To bootstrap mining with your own account:
  1. Create a wallet: wallet-cli new wallet.json
  2. Create config: paqusd node config
  3. Edit ./data/paqus/node.json once
  4. Run: paqusd
"
    );
}

pub fn print_version() {
    println!(
        "{} {} ({}, protocol {})",
        CHAIN_NAME,
        env!("CARGO_PKG_VERSION"),
        PROTOCOL_STAGE,
        PROTOCOL_VERSION
    );
}

pub fn print_network_info() {
    println!("chain: {CHAIN_NAME}");
    println!("coin: {COIN_NAME}");
    println!("stage: {PROTOCOL_STAGE}");
    println!("protocol_version: {PROTOCOL_VERSION}");
    println!("block_time_secs: {BLOCK_TIME}");
    println!("confirmation_depth: {CONFIRMATION_DEPTH}");
    println!("finality_depth: {FINALITY_DEPTH}");
    println!("difficulty_start: {DIFFICULTY_START}");
}
