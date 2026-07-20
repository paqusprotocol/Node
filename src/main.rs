mod command;
mod daemon;
mod gateway;
mod p2p;
mod rpc;
mod runtime;

use axum::{
    Json, Router,
    body::Body,
    extract::{Path as AxumPath, Query, State},
    http::{Request, StatusCode, header},
    middleware::{self, Next},
    response::{
        IntoResponse, Sse,
        sse::{Event as SseEvent, KeepAlive},
    },
    routing::{get, post},
};
#[cfg(test)]
use command::config::encrypted_wallet_address as load_encrypted_wallet_address;
use command::config::{
    RunConfig, dedupe as dedupe_socket_addrs, format_socket_addrs, parse as parse_run_config,
    write_default as write_default_run_config,
};
#[cfg(test)]
use command::database::{backup as backup_node_database, restore as restore_node_database};
use command::display::{
    format_difficulty, format_hash, print_help, print_network_info, print_version, short_hash,
};
use command::parse::{
    address as parse_address, address_string as parse_address_string, hash_hex as parse_hash_hex,
    signed_protocol_transaction as signed_protocol_transaction_from_hex,
    signed_qcash_transaction as signed_qcash_transaction_from_hex,
    signed_transaction as signed_transaction_from_hex,
};
use daemon::mining::{MiningStats, mine_once as mine_once_unlocked};
use futures_util::stream;
use gateway::{heartbeat_peer, register_peer, request_gateway_peers};
use p2p::gossip::{BroadcastReport, broadcast_to_peers};
use p2p::{
    PERSISTENT_PEER_TIMEOUT, PeerConnection, PeerPoll, PeerState, dedupe_peers, load_peers_file,
    poll_peer_connection, request_peers_connection, save_peers_file, sync_from_peers_parallel,
    sync_mempool_connection,
};
#[cfg(test)]
use paqus::block::Nonce;
use paqus::block::{Block, Height};
use paqus::codec::{block_bytes, decode_block};
#[cfg(test)]
use paqus::consensus::supply::Amount;
use paqus::consensus::{ASERT_HALF_LIFE, Consensus, DIFFICULTY_START};
use paqus::crypto::{
    Address, BlockHash, Hash, TransactionHash, WitnessTransactionHash, address_from_public_key,
    address_from_string, address_to_string, derive_public_key,
};
use paqus::event::{EventId, ProtocolEvent, ProtocolEventKind};
use paqus::genesis::CURRENT_CHAIN_PARAMS;
use paqus::ledger::{BLOCK_REWARD_MATURITY, CONFIRMATION_DEPTH, FINALITY_DEPTH};
use paqus::transaction::SignedProtocolTransaction;
#[cfg(test)]
use paqus::transaction::{SignedTransaction, Transaction};
use rpc::api::{LogCounters, RpcMetrics, RpcState, start_rpc_server};
use rpc::transport::{bind_nonblocking, configure_stream};
use runtime::mempool::MempoolConfig;
use runtime::miner::prepare_candidate_block;
use runtime::network::NetworkError;
use runtime::network::{
    InventoryItem, NetworkMessage, PeerInfo, handle_message, read_message, write_message,
};
use runtime::node::Node;
use runtime::params::{
    BLOCK_TIME, CHAIN_ID, CHAIN_NAME, COIN_NAME, GENESIS_PREMINE, MAX_BLOCK_TXS, NETWORK_MAGIC,
    PROTOCOL_STAGE, PROTOCOL_VERSION, STORAGE_VERSION,
};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet, VecDeque, hash_map::Entry};
use std::convert::Infallible;
use std::env;
use std::fs;
use std::io;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::process::ExitCode;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const DEFAULT_NODE_DB: &str = "./data/paqus";
const DEFAULT_CONFIG_FILE: &str = "./data/paqus/node.json";
const MAX_PEER_FAILURES: u32 = 3;
const ACTIVITY_LOG_INTERVAL: Duration = Duration::from_secs(15);
static SHUTDOWN_REQUESTED: AtomicBool = AtomicBool::new(false);

fn main() -> ExitCode {
    if let Err(error) = ctrlc::set_handler(|| {
        SHUTDOWN_REQUESTED.store(true, Ordering::SeqCst);
    }) {
        eprintln!("warning: failed to install shutdown signal handler: {error}");
    }
    let mut args: Vec<String> = env::args().skip(1).collect();
    if args.is_empty()
        && env::current_exe()
            .ok()
            .and_then(|path| path.file_stem().map(|name| name == "paqusd"))
            .unwrap_or(false)
    {
        args = vec!["node".to_string(), "run".to_string()];
    }
    match run(args) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("error: {error}");
            ExitCode::FAILURE
        }
    }
}

fn run(args: Vec<String>) -> Result<(), String> {
    match args.first().map(String::as_str) {
        None => {
            print_help();
            Ok(())
        }
        Some("-h") | Some("--help") | Some("help") => {
            print_help();
            Ok(())
        }
        Some("-V") | Some("--version") | Some("version") => {
            print_version();
            Ok(())
        }
        Some("node") => node_command(&args[1..]),
        Some(command) => Err(format!("unknown command `{command}`. Try `paqusd --help`.")),
    }
}

fn node_command(args: &[String]) -> Result<(), String> {
    match args.first().map(String::as_str) {
        Some("init") => {
            let path = args.get(1).map(String::as_str).unwrap_or(DEFAULT_NODE_DB);
            let miner_address = parse_address(args.get(2)).unwrap_or(Address([9; 20]));
            if args.get(3).is_some() {
                return Err(
                    "premine address is fixed by protocol and cannot be overridden".to_string(),
                );
            }
            let node = open_node(path, miner_address)?;

            println!("database: {path}");
            println!("tip_height: {:?}", node.tip_height());
            println!("tip_hash: {}", format_hash(node.tip_hash()));
            println!("miner_address: {}", address_to_string(&miner_address));
            println!("genesis: pending first mined block");
            Ok(())
        }
        Some("run") => run_node(&args[1..]),
        Some("db") => command::database::run(&args[1..], DEFAULT_NODE_DB),
        Some("config") => node_config_command(&args[1..]),
        Some("info") => {
            print_network_info();
            Ok(())
        }
        _ => Err("usage: paqus node <info|init|config|run|db> [options]".to_string()),
    }
}

fn open_node(path: &str, miner_address: Address) -> Result<Node, String> {
    let _ = miner_address;
    Node::init_or_load(path, Consensus::with_default_config())
        .map_err(|error| format!("failed to open node storage: {error}"))
}

fn node_config_command(args: &[String]) -> Result<(), String> {
    let path = args
        .first()
        .map(String::as_str)
        .unwrap_or(DEFAULT_CONFIG_FILE);
    write_default_run_config(path)?;
    println!("node config written: {path}");
    println!("run with: cargo run -- node run");
    Ok(())
}

include!("daemon/service.rs");

include!("daemon/peer_helpers.rs");

include!("daemon/bootstrap.rs");

fn unix_timestamp() -> Result<u64, String> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .map_err(|_| "system clock is before unix epoch".to_string())
}

#[cfg(test)]
mod test;
