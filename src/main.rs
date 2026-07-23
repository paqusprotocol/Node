mod command;
mod gateway;
mod mining;
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
use command::config::wallet_address as load_wallet_address;
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
use futures_util::stream;
use gateway::{heartbeat_peer, register_peer, request_gateway_peers};
use mining::{MiningStats, mine_once as mine_once_unlocked};
use p2p::gossip::broadcast_to_peers;
use p2p::{
    PeerConnection, PeerPoll, PeerState, dedupe_peers, load_peers_file, poll_peer_connection,
    request_peers_connection, save_peers_file, sync_from_peers_parallel, sync_mempool_connection,
};
#[cfg(test)]
use paqus::block::Nonce;
use paqus::block::{Block, Height};
use paqus::codec::{block_bytes, decode_block};
#[cfg(test)]
use paqus::consensus::supply::Amount;
use paqus::consensus::{ASERT_HALF_LIFE, Consensus, DIFFICULTY_START};
#[cfg(test)]
use paqus::crypto::address_from_public_key;
use paqus::crypto::{
    Address, BlockHash, Hash, TransactionHash, WitnessTransactionHash, address_from_string,
    address_to_string,
};
use paqus::event::{EventId, ProtocolEvent, ProtocolEventKind};
use paqus::genesis::CURRENT_CHAIN_PARAMS;
use paqus::ledger::{
    BLOCK_REWARD_MATURITY, CONFIRMATION_DEPTH, FINALITY_DEPTH, QCASH_WITHDRAW_MATURITY,
};
use paqus::transaction::SignedProtocolTransaction;
#[cfg(test)]
use paqus::transaction::{SignedTransaction, Transaction};
use rpc::api::{LogCounters, RpcMetrics, RpcState, start_rpc_server};
use rpc::transport::{bind_nonblocking, configure_stream};
use runtime::mempool::MempoolConfig;
use runtime::miner::prepare_candidate_block;
use runtime::network::{NetworkError, NetworkMessage, handle_message, read_message, write_message};
use runtime::node::Node;
use runtime::params::{
    BLOCK_TIME, CHAIN_ID, CHAIN_NAME, COIN_NAME, GENESIS_PREMINE, MAX_BLOCK_TXS, NETWORK_MAGIC,
    PROTOCOL_STAGE, PROTOCOL_VERSION, STORAGE_VERSION,
};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::convert::Infallible;
use std::env;
use std::fs;
use std::io::ErrorKind;
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
const DEFAULT_WALLET_CANDIDATES: &[&str] = &["../wallet.json", "wallet.json"];
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
        args = default_run_args();
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
        Some("mine") => run_mine_shortcut(&args[1..]),
        Some("node") => node_command(&args[1..]),
        Some(command) => Err(format!("unknown command `{command}`. Try `paqusd --help`.")),
    }
}

fn default_run_args() -> Vec<String> {
    let Some(wallet_path) = default_wallet_path() else {
        return vec!["node".to_string(), "run".to_string()];
    };
    vec![
        "node".to_string(),
        "run".to_string(),
        "--wallet".to_string(),
        wallet_path,
        "--mine".to_string(),
    ]
}

fn default_wallet_path() -> Option<String> {
    DEFAULT_WALLET_CANDIDATES
        .iter()
        .copied()
        .find(|path| fs::metadata(path).is_ok())
        .map(str::to_string)
}

fn run_mine_shortcut(args: &[String]) -> Result<(), String> {
    let wallet_path = args
        .first()
        .cloned()
        .or_else(default_wallet_path)
        .ok_or_else(|| {
            "mining wallet not found; create one with `cd ../wallet-cli && cargo run -- new ../wallet.json`".to_string()
        })?;
    let db_path = args
        .get(1)
        .cloned()
        .unwrap_or_else(|| DEFAULT_NODE_DB.to_string());
    run_node(&[
        db_path,
        "--wallet".to_string(),
        wallet_path,
        "--mine".to_string(),
    ])
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
    Node::init_or_load(path, Consensus::with_default_config()).map_err(|error| {
        let error = error.to_string();
        if error.contains("stored block failed validation") {
            format!(
                "failed to open node storage: {error}. Existing data was created under a different protocol/genesis identity; reset this local node database with `rm -rf {path}`"
            )
        } else {
            format!("failed to open node storage: {error}")
        }
    })
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

fn print_core_startup_info() {
    println!(
        "[INFO] core chain={} chain_id={} coin={} stage={} protocol={} storage={} magic={}",
        CHAIN_NAME,
        CHAIN_ID,
        COIN_NAME,
        PROTOCOL_STAGE,
        PROTOCOL_VERSION,
        STORAGE_VERSION,
        hex::encode(NETWORK_MAGIC)
    );
    println!(
        "[INFO] consensus block_time={}s confirmation={} finality={} reward_maturity={} difficulty_start={} asert_half_life={}s",
        BLOCK_TIME,
        CONFIRMATION_DEPTH,
        FINALITY_DEPTH,
        BLOCK_REWARD_MATURITY,
        DIFFICULTY_START,
        ASERT_HALF_LIFE
    );
}

fn warn_if_public_rpc(config: &RunConfig) {
    if !config.rpc_addr.ip().is_loopback() {
        eprintln!(
            "[WARN] rpc_public addr={} message=\"expose this only behind trusted network controls\"",
            config.rpc_addr
        );
    }
}

fn run_node(args: &[String]) -> Result<(), String> {
    let mut config = parse_run_config(args)?;
    print_core_startup_info();
    warn_if_public_rpc(&config);
    if let Some(path) = &config.peers_file {
        config.peers.extend(load_peers_file(path)?);
    }
    dedupe_peers(&mut config.peers);
    if config.peers.len() > config.max_peers {
        config.peers.truncate(config.max_peers);
    }
    if config.listen_addrs.is_empty() {
        return Err("at least one --listen address is required".to_string());
    }
    dedupe_socket_addrs(&mut config.listen_addrs);
    dedupe_socket_addrs(&mut config.public_addrs);

    let mut node = open_node(&config.db_path, config.miner_address)?;
    node.mempool = runtime::mempool::Mempool::with_config(MempoolConfig {
        min_relay_fee: config.min_relay_fee,
        market_fee: config.market_fee,
        low_fee_ttl_secs: config.low_fee_expiry.as_secs(),
        transaction_ttl_secs: config.mempool_expiry.as_secs(),
        ..MempoolConfig::default()
    });
    node.next_difficulty()
        .map_err(|error| format!("failed to calculate next difficulty: {error}"))?;

    let mut listeners = Vec::new();
    let mut bound_addrs = Vec::new();
    for addr in &config.listen_addrs {
        let listener = bind_nonblocking(*addr, "p2p")?;
        bound_addrs.push(
            listener
                .local_addr()
                .map_err(|error| format!("failed to read listener address: {error}"))?,
        );
        listeners.push(listener);
    }

    let peers = Arc::new(Mutex::new(
        config
            .peers
            .iter()
            .copied()
            .map(|peer| (peer, PeerState::new(peer)))
            .collect::<HashMap<_, _>>(),
    ));
    let peer_connections = Arc::new(Mutex::new(HashMap::new()));
    let inbound_connections = Arc::new(Mutex::new(HashMap::new()));
    let log_counters = Arc::new(LogCounters::default());
    let mining_stats = Arc::new(MiningStats::default());
    let rpc_metrics = Arc::new(RpcMetrics::default());
    let node = Arc::new(Mutex::new(node));

    {
        let node = node
            .lock()
            .map_err(|_| "node state lock poisoned".to_string())?;
        println!(
            "[OK] preflight height={} tip={} difficulty={} mempool={} mining={}",
            node.tip_height().unwrap_or(Height(0)).0,
            short_hash(node.tip_hash()),
            format_difficulty(node.next_difficulty()),
            node.mempool.len() + node.extension_mempool.len(),
            config.mine
        );
        println!(
            "[NODE] db={} p2p={} rpc={} height={} tip={} difficulty={} peers={} mining={} relay_fee={} market_fee={} dynamic_fee={} miner_fee={} low_fee_expiry={}s mempool_expiry={}s",
            config.db_path,
            format_socket_addrs(&bound_addrs),
            config.rpc_addr,
            node.tip_height().unwrap_or(Height(0)).0,
            short_hash(node.tip_hash()),
            format_difficulty(node.next_difficulty()),
            config.peers.len(),
            config.mine,
            config.min_relay_fee,
            config.market_fee,
            node.mempool.dynamic_market_fee_rate(),
            config
                .miner_min_fee_rate
                .map(|rate| rate.to_string())
                .unwrap_or_else(|| "dynamic".to_string()),
            config.low_fee_expiry.as_secs(),
            config.mempool_expiry.as_secs()
        );
    }
    if !config.mine {
        println!("[HINT] mining=off start_mining=\"cargo run -- mine\" wallet=\"../wallet.json\"");
    }

    let _rpc = start_rpc_server(
        RpcState {
            node: node.clone(),
            peers: peers.clone(),
            peer_connections: peer_connections.clone(),
            inbound_connections: inbound_connections.clone(),
            mining: config.mine,
            log_counters: log_counters.clone(),
            mining_stats: mining_stats.clone(),
            metrics: rpc_metrics,
            db_path: config.db_path.clone(),
        },
        config.rpc_addr,
    )?;

    let mut last_network = Instant::now()
        .checked_sub(ACTIVITY_LOG_INTERVAL)
        .unwrap_or_else(Instant::now);
    let mut last_gateway = Instant::now()
        .checked_sub(config.gateway_heartbeat)
        .unwrap_or_else(Instant::now);
    while !SHUTDOWN_REQUESTED.load(Ordering::SeqCst) {
        service_network_once(
            &listeners,
            &node,
            &config,
            &peers,
            &peer_connections,
            &inbound_connections,
        );
        service_gateway_once(
            &node,
            &config,
            &peers,
            &mut last_gateway,
            bound_addrs.first().copied(),
        );
        if config.mine {
            let _ = mine_once_unlocked(&node, &config, &mining_stats, &SHUTDOWN_REQUESTED)?;
            if SHUTDOWN_REQUESTED.load(Ordering::SeqCst) {
                break;
            }
        }
        if last_network.elapsed() >= ACTIVITY_LOG_INTERVAL {
            last_network = Instant::now();
            let node = node
                .lock()
                .map_err(|_| "node state lock poisoned".to_string())?;
            println!(
                "[STATUS] height={} tip={} difficulty={} peers={} outbound={} inbound={} mining={} hashrate_hps={} accepted_tx={} broadcast_tx={}",
                node.tip_height().unwrap_or(Height(0)).0,
                short_hash(node.tip_hash()),
                format_difficulty(node.next_difficulty()),
                peers.lock().map(|peers| peers.len()).unwrap_or_default(),
                peer_connections
                    .lock()
                    .map(|connections| connections.len())
                    .unwrap_or_default(),
                inbound_connections
                    .lock()
                    .map(|connections| connections.len())
                    .unwrap_or_default(),
                config.mine,
                mining_stats
                    .last_hashrate_hps
                    .load(std::sync::atomic::Ordering::Relaxed),
                log_counters
                    .accepted_tx_total
                    .load(std::sync::atomic::Ordering::Relaxed),
                log_counters
                    .broadcast_tx_total
                    .load(std::sync::atomic::Ordering::Relaxed)
            );
        }
        if !config.mine || config.mine_attempts != 0 {
            interruptible_sleep(config.mine_interval);
        }
    }
    println!("[OK] shutdown complete");
    Ok(())
}

fn interruptible_sleep(duration: Duration) {
    let deadline = Instant::now()
        .checked_add(duration)
        .unwrap_or_else(Instant::now);
    while !SHUTDOWN_REQUESTED.load(Ordering::SeqCst) {
        let now = Instant::now();
        if now >= deadline {
            break;
        }
        thread::sleep((deadline - now).min(Duration::from_millis(100)));
    }
}

fn spawn_inbound_peer(addr: SocketAddr, mut stream: TcpStream, node: Arc<Mutex<Node>>) {
    thread::spawn(move || {
        if let Err(error) = configure_stream(&stream, Duration::from_secs(30)) {
            eprintln!("[P2P] inbound_config_failed peer={addr} error=\"{error}\"");
            return;
        }
        loop {
            let envelope = match read_message(&mut stream) {
                Ok(envelope) => envelope,
                Err(NetworkError::Io(error))
                    if matches!(
                        error.kind(),
                        ErrorKind::WouldBlock | ErrorKind::TimedOut | ErrorKind::UnexpectedEof
                    ) =>
                {
                    break;
                }
                Err(error) => {
                    eprintln!("[P2P] inbound_read_failed peer={addr} error=\"{error}\"");
                    break;
                }
            };
            let response = {
                let mut node = match node.lock() {
                    Ok(node) => node,
                    Err(_) => {
                        eprintln!("[P2P] inbound_node_lock_poisoned peer={addr}");
                        break;
                    }
                };
                match handle_message(&mut node, envelope.message) {
                    Ok(response) => response,
                    Err(error) => {
                        eprintln!("[P2P] inbound_handle_failed peer={addr} error=\"{error}\"");
                        break;
                    }
                }
            };
            if let Some(response) = response
                && let Err(error) = write_message(&mut stream, &response.to_envelope())
            {
                eprintln!("[P2P] inbound_write_failed peer={addr} error=\"{error}\"");
                break;
            }
        }
    });
}

fn service_network_once(
    listeners: &[TcpListener],
    node: &Arc<Mutex<Node>>,
    config: &RunConfig,
    peers: &Arc<Mutex<HashMap<SocketAddr, PeerState>>>,
    peer_connections: &Arc<Mutex<HashMap<SocketAddr, PeerConnection>>>,
    _inbound_connections: &Arc<Mutex<HashMap<SocketAddr, PeerConnection>>>,
) {
    for listener in listeners {
        loop {
            match listener.accept() {
                Ok((stream, addr)) => {
                    spawn_inbound_peer(addr, stream, node.clone());
                }
                Err(error) if error.kind() == ErrorKind::WouldBlock => break,
                Err(error) => {
                    eprintln!("[P2P] accept_failed error=\"{error}\"");
                    break;
                }
            }
        }
    }

    let addrs = match peers.lock() {
        Ok(peers) => peers.keys().copied().collect::<Vec<_>>(),
        Err(_) => return,
    };
    if !addrs.is_empty() {
        let sync_window = addrs
            .iter()
            .filter_map(|addr| peers.lock().ok()?.get(addr).map(|peer| peer.sync_window))
            .max()
            .unwrap_or(64);
        match sync_from_peers_parallel(addrs.clone(), node, &config.public_addrs, sync_window) {
            Ok(report) if report.applied_blocks > 0 => println!(
                "[SYNC] remote_tip={} applied={} peers={}",
                report.remote_tip.0, report.applied_blocks, report.used_peers
            ),
            Ok(_) => {}
            Err(error) => eprintln!("[SYNC] failed error=\"{error}\""),
        }
    }

    for addr in addrs {
        let should_connect = {
            let peers = match peers.lock() {
                Ok(peers) => peers,
                Err(_) => return,
            };
            let connected = peer_connections
                .lock()
                .map(|connections| connections.contains_key(&addr))
                .unwrap_or(false);
            !connected
                && peers
                    .get(&addr)
                    .map(|peer| Instant::now() >= peer.next_attempt)
                    .unwrap_or(false)
        };
        if should_connect {
            match PeerConnection::connect(addr) {
                Ok(connection) => {
                    if let Ok(mut connections) = peer_connections.lock() {
                        connections.insert(addr, connection);
                    }
                }
                Err(error) => {
                    eprintln!("[P2P] connect_failed peer={addr} error=\"{error}\"");
                    if let Ok(mut peers) = peers.lock()
                        && let Some(peer) = peers.get_mut(&addr)
                    {
                        peer.mark_failed();
                    }
                }
            }
        }

        let mut connection = match peer_connections
            .lock()
            .ok()
            .and_then(|mut connections| connections.remove(&addr))
        {
            Some(connection) => connection,
            None => continue,
        };
        let sync_window = peers
            .lock()
            .ok()
            .and_then(|peers| peers.get(&addr).map(|peer| peer.sync_window))
            .unwrap_or(64);
        let result = poll_peer_connection(&mut connection, node, &config.public_addrs, sync_window)
            .and_then(|poll| {
                let _ = sync_mempool_connection(&mut connection, node);
                let discovered = request_peers_connection(&mut connection).unwrap_or_default();
                if let Ok(mut peers) = peers.lock() {
                    for info in discovered {
                        if let Ok(addr) = info.address.parse::<SocketAddr>() {
                            peers.entry(addr).or_insert_with(|| PeerState::new(addr));
                        }
                    }
                }
                Ok(poll)
            });
        match result {
            Ok(PeerPoll::Idle { remote_tip }) => {
                if let Ok(mut peers) = peers.lock()
                    && let Some(peer) = peers.get_mut(&addr)
                {
                    peer.mark_ok(Some(remote_tip));
                }
                if let Ok(mut connections) = peer_connections.lock() {
                    connections.insert(addr, connection);
                }
            }
            Ok(PeerPoll::Synced {
                remote_tip,
                synced_blocks,
            }) => {
                if let Ok(mut peers) = peers.lock()
                    && let Some(peer) = peers.get_mut(&addr)
                {
                    peer.mark_synced(remote_tip, synced_blocks);
                }
                if let Ok(mut connections) = peer_connections.lock() {
                    connections.insert(addr, connection);
                }
            }
            Err(error) => {
                eprintln!("[P2P] poll_failed peer={addr} error=\"{error}\"");
                if let Ok(mut peers) = peers.lock()
                    && let Some(peer) = peers.get_mut(&addr)
                {
                    peer.mark_failed();
                    if peer.failures > MAX_PEER_FAILURES {
                        peers.remove(&addr);
                    }
                }
            }
        }
    }

    if let Some(path) = &config.peers_file
        && let Ok(peers) = peers.lock()
    {
        let _ = save_peers_file(path, peers.keys().copied().collect());
    }
}

fn service_gateway_once(
    node: &Arc<Mutex<Node>>,
    config: &RunConfig,
    peers: &Arc<Mutex<HashMap<SocketAddr, PeerState>>>,
    last_gateway: &mut Instant,
    public_fallback: Option<SocketAddr>,
) {
    let Some(gateway_url) = config.gateway_url.as_deref() else {
        return;
    };
    if last_gateway.elapsed() < config.gateway_heartbeat {
        return;
    }
    *last_gateway = Instant::now();
    let public_addr = config
        .public_addrs
        .first()
        .copied()
        .or(public_fallback)
        .unwrap_or(config.rpc_addr);
    let (height, tip_hash) = match node.lock() {
        Ok(node) => (
            node.tip_height().map(|height| height.0),
            node.tip_hash().map(|hash| hex::encode(hash.0)),
        ),
        Err(_) => (None, None),
    };
    let _ = register_peer(gateway_url, public_addr, height, tip_hash.clone());
    let _ = heartbeat_peer(gateway_url, public_addr, height, tip_hash);
    match request_gateway_peers(gateway_url, config.max_peers, Some(public_addr)) {
        Ok(discovered) => {
            if let Ok(mut peers) = peers.lock() {
                for info in discovered {
                    if let Ok(addr) = info.address.parse::<SocketAddr>() {
                        peers.entry(addr).or_insert_with(|| PeerState::new(addr));
                    }
                }
            }
        }
        Err(error) => eprintln!("[GATEWAY] peer_request_failed error=\"{error}\""),
    }
}

fn unix_timestamp() -> Result<u64, String> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .map_err(|_| "system clock is before unix epoch".to_string())
}

#[cfg(test)]
mod test;
