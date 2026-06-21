use axum::{
    Json, Router,
    extract::{Path as AxumPath, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
};
use borsh::BorshDeserialize;
use paqus::block::Block;
use paqus::consensus::Consensus;
use paqus::crypto::{address_to_string, derive_public_key};
use paqus::genesis::GENESIS_PREMINE_ADDRESS;
use paqus::network::{
    NetworkMessage, PeerInfo, TipInfo, VersionInfo, handle_message, read_message, write_message,
};
use paqus::node::Node;
use paqus::params::{
    BASE_FEE, BLOCK_REWARD_MATURITY, BLOCK_TIME, CHAIN_ID, CHAIN_NAME, COIN_NAME,
    DIFFICULTY_ADJUSTMENT_INTERVAL, DIFFICULTY_START, FINALITY_DEPTH, MAX_BLOCK_TXS, NETWORK_MAGIC,
    PROTOCOL_STAGE, PROTOCOL_VERSION, STORAGE_VERSION,
};
use paqus::transaction::{SignedTransaction, Transaction};
use paqus::types::{Address, Amount, Height, Nonce, SecretKey};
use paqus::wallet::Wallet;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::env;
use std::fs;
use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::process::ExitCode;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const DEFAULT_NODE_DB: &str = "./data/paqus";
const DEFAULT_LISTEN_ADDR: &str = "0.0.0.0:30333";
const DEFAULT_RPC_ADDR: &str = "127.0.0.1:9933";
const DEFAULT_MINING_INTERVAL: Duration = Duration::from_secs(BLOCK_TIME as u64);
const DEFAULT_SYNC_INTERVAL: Duration = Duration::from_secs(5);
const PEER_RETRY_BASE: Duration = Duration::from_secs(2);
const PEER_RETRY_MAX: Duration = Duration::from_secs(60);
const MAX_BLOCKS_PER_SYNC: u64 = 64;
const DEFAULT_MAX_PEERS: usize = 128;
const DEFAULT_SHUTDOWN_FILE: &str = "./data/paqus/STOP";

fn main() -> ExitCode {
    match run(env::args().skip(1).collect()) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("error: {error}");
            ExitCode::FAILURE
        }
    }
}

fn run(args: Vec<String>) -> Result<(), String> {
    match args.first().map(String::as_str) {
        None | Some("-h") | Some("--help") | Some("help") => {
            print_help();
            Ok(())
        }
        Some("-V") | Some("--version") | Some("version") => {
            print_version();
            Ok(())
        }
        Some("wallet") => wallet_command(&args[1..]),
        Some("node") => node_command(&args[1..]),
        Some(command) => Err(format!("unknown command `{command}`. Try `paqus --help`.")),
    }
}

fn wallet_command(args: &[String]) -> Result<(), String> {
    match args.first().map(String::as_str) {
        Some("new") => {
            let show_secret = args.iter().any(|arg| arg == "--show-secret");
            let output_path = args.iter().skip(1).find(|arg| !arg.starts_with('-'));
            let wallet = Wallet::generate();

            let address_str = wallet.wallet_address().to_string();
            let public_key_hex = hex::encode(wallet.public_key.0);
            let secret_key_hex = hex::encode(wallet.secret_key.0);

            if let Some(path) = output_path {
                let json_data = serde_json::json!({
                    "address": address_str,
                    "public_key": public_key_hex,
                    "secret_key": secret_key_hex,
                });
                let json_str = serde_json::to_string_pretty(&json_data)
                    .map_err(|error| format!("failed to serialize wallet: {error}"))?;
                fs::write(path, json_str)
                    .map_err(|error| format!("failed to write wallet file `{path}`: {error}"))?;
                println!("Wallet successfully saved to `{path}`");
                println!("address: {address_str}");
            } else {
                println!("address: {address_str}");
                println!("public_key: {public_key_hex}");
                if show_secret {
                    println!("secret_key: {secret_key_hex}");
                } else {
                    println!("secret_key: hidden (rerun with --show-secret to print it)");
                }
            }
            Ok(())
        }
        Some("address") => {
            let secret_key = parse_secret_key(args.get(1))?;
            let public_key = derive_public_key(&secret_key);
            let address = paqus::crypto::address_from_public_key(&public_key);
            println!("{}", address_to_string(&address));
            Ok(())
        }
        Some("balance") => {
            let address = parse_address(args.get(1))?;
            let db_path = args.get(2).map(String::as_str).unwrap_or(DEFAULT_NODE_DB);
            let node = open_node(db_path, Address([9; 20]))?;
            println!("{}", balance_json(&node, &address));
            Ok(())
        }
        Some("send") => wallet_send_command(&args[1..]),
        _ => Err("usage: paqus wallet <new|address|balance|send> [options]".to_string()),
    }
}

fn wallet_send_command(args: &[String]) -> Result<(), String> {
    let mut wallet_path = None;
    let mut to = None;
    let mut amount = None;
    let mut fee = Amount(BASE_FEE);
    let mut nonce = None;
    let mut rpc_addr = DEFAULT_RPC_ADDR.to_string();
    let mut submit = false;
    let mut index = 0;

    while index < args.len() {
        match args[index].as_str() {
            "--wallet" => {
                index += 1;
                wallet_path = args.get(index).cloned();
            }
            "--to" => {
                index += 1;
                to = Some(parse_address(args.get(index))?);
            }
            "--amount" => {
                index += 1;
                amount = Some(parse_amount(args.get(index), "--amount")?);
            }
            "--fee" => {
                index += 1;
                fee = parse_amount(args.get(index), "--fee")?;
            }
            "--nonce" => {
                index += 1;
                nonce = Some(parse_nonce(args.get(index))?);
            }
            "--rpc" | "--rpc-addr" => {
                index += 1;
                rpc_addr = args
                    .get(index)
                    .ok_or_else(|| "missing value for --rpc".to_string())?
                    .clone();
            }
            "--submit" => submit = true,
            value => return Err(format!("unknown wallet send option `{value}`")),
        }
        index += 1;
    }

    let wallet_path = wallet_path.ok_or_else(|| "missing --wallet path".to_string())?;
    let to = to.ok_or_else(|| "missing --to address".to_string())?;
    let amount = amount.ok_or_else(|| "missing --amount".to_string())?;
    let nonce = nonce.ok_or_else(|| "missing --nonce".to_string())?;
    let wallet = load_wallet(&wallet_path)?;
    let transaction = Transaction::new(wallet.address, to, amount, fee, nonce);
    let signed = wallet
        .sign_transaction(transaction)
        .map_err(|error| format!("failed to sign transaction: {error}"))?;
    let tx_hex = signed_transaction_to_hex(&signed)?;

    if submit {
        let body = format!("{{\"tx\":\"{tx_hex}\"}}");
        let response = http_post_json(&rpc_addr, "/tx", &body)?;
        println!("{response}");
    } else {
        println!(
            "{{\"tx\":\"{}\",\"hash\":\"{}\",\"from\":\"{}\",\"to\":\"{}\",\"amount\":{},\"fee\":{},\"nonce\":{}}}",
            tx_hex,
            hex::encode(signed.hash().0),
            address_to_string(&signed.payload.from),
            address_to_string(&signed.payload.to),
            signed.payload.amount.0,
            signed.payload.fee.0,
            signed.payload.nonce.0
        );
    }

    Ok(())
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
            println!(
                "premine_address: {}",
                address_to_string(&GENESIS_PREMINE_ADDRESS)
            );
            Ok(())
        }
        Some("run") => run_node(&args[1..]),
        Some("info") => {
            print_network_info();
            Ok(())
        }
        _ => Err("usage: paqus node <info|init|run> [options]".to_string()),
    }
}

fn open_node(path: &str, miner_address: Address) -> Result<Node, String> {
    let _ = miner_address;
    Node::init_or_load(path, Consensus::with_default_config())
        .map_err(|error| format!("failed to open node storage: {error}"))
}

#[derive(Debug)]
struct RunConfig {
    db_path: String,
    listen_addr: SocketAddr,
    rpc_addr: SocketAddr,
    peers: Vec<SocketAddr>,
    peers_file: Option<String>,
    shutdown_file: String,
    max_peers: usize,
    miner_address: Address,
    miner_secret_key: Option<SecretKey>,
    mine: bool,
    mine_interval: Duration,
    mine_attempts: u64,
}

#[derive(Debug, Deserialize)]
struct WalletFile {
    address: String,
    secret_key: String,
}

#[derive(Debug, Deserialize)]
struct SubmitTxRequest {
    tx: String,
}

#[derive(Clone)]
struct RpcState {
    node: Arc<Mutex<Node>>,
    peers: Arc<Mutex<HashMap<SocketAddr, PeerState>>>,
    mining: bool,
}

#[derive(Serialize)]
struct StatusResponse {
    chain: &'static str,
    stage: &'static str,
    protocol_version: u8,
    height: u64,
    tip_hash: String,
    peers: usize,
    mining: bool,
}

#[derive(Serialize)]
struct HealthResponse {
    ok: bool,
}

#[derive(Serialize)]
struct PeerResponse {
    addr: String,
    failures: u32,
    last_tip: Option<u64>,
}

#[derive(Serialize)]
struct SubmitTxResponse {
    accepted: bool,
    hash: String,
}

#[derive(Serialize)]
struct ErrorResponse {
    error: String,
}

#[derive(Serialize)]
struct ChainResponse {
    chain: &'static str,
    coin: &'static str,
    stage: &'static str,
    protocol_version: u8,
    block_time_secs: u32,
    finality_depth: u32,
    difficulty_start: u32,
}

#[derive(Serialize)]
struct TxResponse {
    hash: String,
    from: String,
    to: String,
    amount: u32,
    fee: u32,
    nonce: u64,
    block_height: Option<u64>,
    block_hash: Option<String>,
    status: &'static str,
}

#[derive(Serialize)]
struct CoinbaseResponse {
    to: String,
    subsidy: u32,
    fees: u32,
    total: u32,
}

#[derive(Serialize)]
struct GenesisAllocationResponse {
    to: String,
    amount: u32,
}

#[derive(Serialize)]
struct BlockResponse {
    height: u64,
    hash: String,
    short_hash: String,
    previous_hash: String,
    merkle_root: String,
    state_root: String,
    miner_address: String,
    difficulty: u32,
    timestamp: u64,
    nonce: u64,
    tx_count: usize,
    size: usize,
    coinbase: Option<CoinbaseResponse>,
    genesis_allocations: Vec<GenesisAllocationResponse>,
    transactions: Vec<TxResponse>,
}

#[derive(Serialize)]
struct AddressResponse {
    address: String,
    balance: serde_json::Value,
    transactions: Vec<TxResponse>,
}

#[derive(Serialize)]
struct AccountResponse {
    address: String,
    confirmed: u32,
    available: u32,
    unspendable: u32,
    pending_incoming: u32,
    pending_outgoing: u32,
    nonce: u64,
    credits: usize,
}

#[derive(Serialize)]
struct MempoolResponse {
    size: usize,
    transactions: Vec<TxResponse>,
}

impl Default for RunConfig {
    fn default() -> Self {
        Self {
            db_path: DEFAULT_NODE_DB.to_string(),
            listen_addr: DEFAULT_LISTEN_ADDR
                .parse()
                .expect("default listen address must be valid"),
            rpc_addr: DEFAULT_RPC_ADDR
                .parse()
                .expect("default rpc address must be valid"),
            peers: Vec::new(),
            peers_file: None,
            shutdown_file: DEFAULT_SHUTDOWN_FILE.to_string(),
            max_peers: DEFAULT_MAX_PEERS,
            miner_address: Address([9; 20]),
            miner_secret_key: None,
            mine: false,
            mine_interval: DEFAULT_MINING_INTERVAL,
            mine_attempts: 10_000,
        }
    }
}

#[derive(Debug, Clone)]
struct PeerState {
    addr: SocketAddr,
    failures: u32,
    next_attempt: Instant,
    last_tip: Option<Height>,
}

impl PeerState {
    fn new(addr: SocketAddr) -> Self {
        Self {
            addr,
            failures: 0,
            next_attempt: Instant::now(),
            last_tip: None,
        }
    }

    fn mark_ok(&mut self, tip: Option<Height>) {
        self.failures = 0;
        self.last_tip = tip;
        self.next_attempt = Instant::now() + DEFAULT_SYNC_INTERVAL;
    }

    fn mark_failed(&mut self) {
        self.failures = self.failures.saturating_add(1);
        let shift = self.failures.saturating_sub(1).min(5);
        let secs = PEER_RETRY_BASE
            .as_secs()
            .saturating_mul(1_u64 << shift)
            .min(PEER_RETRY_MAX.as_secs());
        self.next_attempt = Instant::now() + Duration::from_secs(secs);
    }
}

struct NodeService {
    node: Arc<Mutex<Node>>,
    config: RunConfig,
    listener: TcpListener,
    peers: Arc<Mutex<HashMap<SocketAddr, PeerState>>>,
    last_mine: Instant,
    last_status: Instant,
}

impl NodeService {
    fn new(node: Arc<Mutex<Node>>, config: RunConfig, listener: TcpListener) -> Self {
        let peers = config
            .peers
            .iter()
            .copied()
            .map(|peer| (peer, PeerState::new(peer)))
            .collect();
        Self {
            node,
            config,
            listener,
            peers: Arc::new(Mutex::new(peers)),
            last_mine: Instant::now(),
            last_status: Instant::now(),
        }
    }

    fn preflight(&mut self) -> Result<(), String> {
        if fs::metadata(&self.config.shutdown_file).is_ok() {
            return Err(format!(
                "shutdown file `{}` exists; remove it before starting the node",
                self.config.shutdown_file
            ));
        }

        {
            let node = self
                .node
                .lock()
                .map_err(|_| "node state lock poisoned".to_string())?;
            node.next_difficulty()
                .map_err(|error| format!("failed to calculate next difficulty: {error}"))?;
        }

        if self.config.mine {
            let secret_key = self.config.miner_secret_key.ok_or_else(|| {
                "mining requires --wallet or --miner-secret-key so the miner identity is explicit"
                    .to_string()
            })?;
            let public_key = derive_public_key(&secret_key);
            let derived_address = paqus::crypto::address_from_public_key(&public_key);
            if derived_address != self.config.miner_address {
                return Err(format!(
                    "miner secret key does not match miner address {}",
                    address_to_string(&self.config.miner_address)
                ));
            }
        }

        let peers = self
            .peers
            .lock()
            .map_err(|_| "peer state lock poisoned".to_string())?
            .keys()
            .copied()
            .collect::<Vec<_>>();
        if !peers.is_empty() {
            println!(
                "preflight peers={} checking handshake and catch-up",
                peers.len()
            );
        }

        for peer in peers {
            let result = {
                let mut node = self
                    .node
                    .lock()
                    .map_err(|_| "node state lock poisoned".to_string())?;
                poll_peer(peer, &mut node)
            };
            match result {
                Ok(PeerPoll::Idle) | Ok(PeerPoll::Synced) => {
                    let tip = self
                        .node
                        .lock()
                        .map_err(|_| "node state lock poisoned".to_string())?
                        .tip_height();
                    if let Ok(mut peers) = self.peers.lock() {
                        if let Some(state) = peers.get_mut(&peer) {
                            state.mark_ok(tip);
                        }
                    }
                }
                Err(error) => {
                    if let Ok(mut peers) = self.peers.lock() {
                        if let Some(state) = peers.get_mut(&peer) {
                            state.mark_failed();
                        }
                    }
                    eprintln!("preflight peer {peer} failed: {error}");
                }
            }
        }

        let node = self
            .node
            .lock()
            .map_err(|_| "node state lock poisoned".to_string())?;
        println!(
            "preflight ok height={} tip={} difficulty={} mempool={} mining={}",
            node.tip_height().unwrap_or(Height(0)).0,
            short_hash(node.tip_hash()),
            format_difficulty(node.next_difficulty()),
            node.mempool.len(),
            self.config.mine
        );

        Ok(())
    }

    fn run(&mut self) -> Result<(), String> {
        loop {
            if fs::metadata(&self.config.shutdown_file).is_ok() {
                self.shutdown()?;
                return Ok(());
            }

            self.accept_p2p()?;
            self.sync_peers();

            if self.config.mine && self.last_mine.elapsed() >= self.config.mine_interval {
                let block = {
                    let mut node = self
                        .node
                        .lock()
                        .map_err(|_| "node state lock poisoned".to_string())?;
                    mine_once(&mut node, &self.config)?
                };
                if let Some(block) = block {
                    self.broadcast(NetworkMessage::Block(block));
                }
                self.last_mine = Instant::now();
            }

            if self.last_status.elapsed() >= Duration::from_secs(30) {
                let node = self
                    .node
                    .lock()
                    .map_err(|_| "node state lock poisoned".to_string())?;
                let peer_count = self
                    .peers
                    .lock()
                    .map_err(|_| "peer state lock poisoned".to_string())?
                    .len();
                println!(
                    "status height={} tip={} difficulty={} mempool={} peers={} mining={}",
                    node.tip_height().unwrap_or(Height(0)).0,
                    short_hash(node.tip_hash()),
                    format_difficulty(node.next_difficulty()),
                    node.mempool.len(),
                    peer_count,
                    self.config.mine
                );
                self.last_status = Instant::now();
            }

            thread::sleep(Duration::from_millis(50));
        }
    }

    fn shutdown(&mut self) -> Result<(), String> {
        let node = self
            .node
            .lock()
            .map_err(|_| "node state lock poisoned".to_string())?;
        node.flush_to_storage()
            .map_err(|error| format!("failed to flush node on shutdown: {error}"))?;
        self.save_peers()?;
        let peer_count = self
            .peers
            .lock()
            .map_err(|_| "peer state lock poisoned".to_string())?
            .len();
        println!(
            "shutdown height={} tip={} difficulty={} peers={}",
            node.tip_height().unwrap_or(Height(0)).0,
            short_hash(node.tip_hash()),
            format_difficulty(node.next_difficulty()),
            peer_count
        );
        Ok(())
    }

    fn accept_p2p(&mut self) -> Result<(), String> {
        loop {
            match self.listener.accept() {
                Ok((stream, peer)) => self.handle_p2p_stream(stream, peer)?,
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => return Ok(()),
                Err(error) => return Err(format!("failed to accept peer: {error}")),
            }
        }
    }

    fn handle_p2p_stream(&mut self, mut stream: TcpStream, peer: SocketAddr) -> Result<(), String> {
        configure_stream(&stream, Duration::from_secs(5))?;
        match read_message(&mut stream) {
            Ok(envelope) => {
                let response = match envelope.message {
                    NetworkMessage::GetPeers => Some(NetworkMessage::Peers(self.peer_infos())),
                    NetworkMessage::Peers(peers) => {
                        self.add_peer_infos(peers);
                        None
                    }
                    message => {
                        let mut node = self
                            .node
                            .lock()
                            .map_err(|_| "node state lock poisoned".to_string())?;
                        handle_message(&mut node, message).map_err(|error| {
                            format!("failed to handle message from {peer}: {error}")
                        })?
                    }
                };
                if let Some(response) = response {
                    write_message(&mut stream, &response.to_envelope())
                        .map_err(|error| format!("failed to respond to {peer}: {error}"))?;
                }
            }
            Err(error) => eprintln!("peer {peer} sent invalid message: {error}"),
        }
        Ok(())
    }

    fn sync_peers(&mut self) {
        let due_peers = match self.peers.lock() {
            Ok(peers) => peers
                .iter()
                .filter_map(|(addr, peer)| (Instant::now() >= peer.next_attempt).then_some(*addr))
                .collect::<Vec<_>>(),
            Err(_) => {
                eprintln!("peer state lock poisoned");
                return;
            }
        };

        for peer in due_peers {
            let result = match self.node.lock() {
                Ok(mut node) => poll_peer(peer, &mut node),
                Err(_) => {
                    eprintln!("node state lock poisoned");
                    return;
                }
            };
            match result {
                Ok(PeerPoll::Idle) | Ok(PeerPoll::Synced) => {
                    let tip = match self.node.lock() {
                        Ok(node) => node.tip_height(),
                        Err(_) => {
                            eprintln!("node state lock poisoned");
                            return;
                        }
                    };
                    if let Ok(mut peers) = self.peers.lock() {
                        if let Some(state) = peers.get_mut(&peer) {
                            state.mark_ok(tip);
                        }
                    }
                    if let Ok(infos) = request_peers(peer) {
                        self.add_peer_infos(infos);
                        let _ = self.save_peers();
                    }
                }
                Err(error) => {
                    if let Ok(mut peers) = self.peers.lock() {
                        if let Some(state) = peers.get_mut(&peer) {
                            state.mark_failed();
                        }
                    }
                    eprintln!("peer {peer} sync failed: {error}");
                }
            }
        }
    }

    fn add_peer_infos(&mut self, peers: Vec<PeerInfo>) {
        let Ok(mut current) = self.peers.lock() else {
            eprintln!("peer state lock poisoned");
            return;
        };
        let known = current.keys().copied().collect::<HashSet<_>>();
        for info in peers {
            if current.len() >= self.config.max_peers {
                break;
            }
            let Ok(addr) = info.address.parse::<SocketAddr>() else {
                continue;
            };
            if known.contains(&addr) {
                continue;
            }
            current.entry(addr).or_insert_with(|| PeerState::new(addr));
        }
    }

    fn save_peers(&self) -> Result<(), String> {
        let Some(path) = &self.config.peers_file else {
            return Ok(());
        };
        if let Some(parent) = std::path::Path::new(path).parent() {
            fs::create_dir_all(parent)
                .map_err(|error| format!("failed to create peers file parent: {error}"))?;
        }
        let peers = self
            .peers
            .lock()
            .map_err(|_| "peer state lock poisoned".to_string())?;
        let mut peers = peers.keys().map(SocketAddr::to_string).collect::<Vec<_>>();
        peers.sort();
        fs::write(path, peers.join("\n"))
            .map_err(|error| format!("failed to write peers file {path}: {error}"))
    }

    fn peer_infos(&self) -> Vec<PeerInfo> {
        let Ok(peers) = self.peers.lock() else {
            eprintln!("peer state lock poisoned");
            return Vec::new();
        };
        peers
            .keys()
            .map(|addr| PeerInfo {
                address: addr.to_string(),
            })
            .collect()
    }

    fn broadcast(&mut self, message: NetworkMessage) {
        let peers = match self.peers.lock() {
            Ok(peers) => peers.keys().copied().collect::<Vec<_>>(),
            Err(_) => {
                eprintln!("peer state lock poisoned");
                return;
            }
        };
        for peer in peers {
            if let Err(error) = send_message(peer, message.clone()) {
                eprintln!("broadcast to {peer} failed: {error}");
            }
        }
    }
}

fn balance_json(node: &Node, address: &Address) -> String {
    let address_str = address_to_string(address);
    let height = node.tip_height().unwrap_or(Height(0)).0;
    let Some(summary) = node.balance_summary(address) else {
        return format!(
            "{{\"address\":\"{address_str}\",\"height\":{height},\"exists\":false,\"confirmed\":0,\"available\":0,\"pending_incoming\":0,\"pending_outgoing\":0,\"nonce\":null,\"unspendable\":0}}"
        );
    };
    let account = node.account_view(address);
    let nonce = account
        .map(|account| account.nonce.0.to_string())
        .unwrap_or_else(|| "null".to_string());
    let unspendable = account.map(|account| account.unspendable.0).unwrap_or(0);

    format!(
        "{{\"address\":\"{address_str}\",\"height\":{height},\"exists\":true,\"confirmed\":{},\"available\":{},\"pending_incoming\":{},\"pending_outgoing\":{},\"nonce\":{nonce},\"unspendable\":{unspendable}}}",
        summary.confirmed.0,
        summary.available.0,
        summary.pending.incoming.0,
        summary.pending.outgoing.0
    )
}

fn start_rpc_server(state: RpcState, addr: SocketAddr) -> Result<thread::JoinHandle<()>, String> {
    let app = Router::new()
        .route("/", get(rpc_status))
        .route("/status", get(rpc_status))
        .route("/health", get(rpc_health))
        .route("/chain", get(rpc_chain))
        .route("/peers", get(rpc_peers))
        .route("/balance/:address", get(rpc_balance))
        .route("/blocks/latest", get(rpc_latest_blocks))
        .route("/blocks/:height", get(rpc_block_by_height))
        .route("/blocks/hash/:hash", get(rpc_block_by_hash))
        .route("/tx/:hash", get(rpc_tx))
        .route("/address/:address", get(rpc_address))
        .route("/accounts", get(rpc_accounts))
        .route("/mempool", get(rpc_mempool))
        .route("/tx", post(rpc_submit_tx))
        .route("/transaction", post(rpc_submit_tx))
        .with_state(state);

    thread::Builder::new()
        .name("paqus-rpc".to_string())
        .spawn(move || {
            let runtime = match tokio::runtime::Runtime::new() {
                Ok(runtime) => runtime,
                Err(error) => {
                    eprintln!("failed to start rpc runtime: {error}");
                    return;
                }
            };
            runtime.block_on(async move {
                let listener = match tokio::net::TcpListener::bind(addr).await {
                    Ok(listener) => listener,
                    Err(error) => {
                        eprintln!("failed to bind rpc {addr}: {error}");
                        return;
                    }
                };
                println!("rpc listening on {addr}");
                if let Err(error) = axum::serve(listener, app).await {
                    eprintln!("rpc server failed: {error}");
                }
            });
        })
        .map_err(|error| format!("failed to spawn rpc server: {error}"))
}

async fn rpc_status(State(state): State<RpcState>) -> impl IntoResponse {
    match (state.node.lock(), state.peers.lock()) {
        (Ok(node), Ok(peers)) => Json(StatusResponse {
            chain: CHAIN_NAME,
            stage: PROTOCOL_STAGE,
            protocol_version: PROTOCOL_VERSION,
            height: node.tip_height().unwrap_or(Height(0)).0,
            tip_hash: format_hash(node.tip_hash()),
            peers: peers.len(),
            mining: state.mining,
        })
        .into_response(),
        _ => rpc_error(StatusCode::INTERNAL_SERVER_ERROR, "state_lock_failed"),
    }
}

async fn rpc_health() -> impl IntoResponse {
    Json(HealthResponse { ok: true })
}

async fn rpc_chain() -> impl IntoResponse {
    Json(ChainResponse {
        chain: CHAIN_NAME,
        coin: COIN_NAME,
        stage: PROTOCOL_STAGE,
        protocol_version: PROTOCOL_VERSION,
        block_time_secs: BLOCK_TIME,
        finality_depth: FINALITY_DEPTH,
        difficulty_start: DIFFICULTY_START,
    })
}

async fn rpc_peers(State(state): State<RpcState>) -> impl IntoResponse {
    match state.peers.lock() {
        Ok(peers) => {
            let peers = peers
                .values()
                .map(|peer| PeerResponse {
                    addr: peer.addr.to_string(),
                    failures: peer.failures,
                    last_tip: peer.last_tip.map(|height| height.0),
                })
                .collect::<Vec<_>>();
            Json(peers).into_response()
        }
        Err(_) => rpc_error(StatusCode::INTERNAL_SERVER_ERROR, "state_lock_failed"),
    }
}

async fn rpc_balance(
    State(state): State<RpcState>,
    AxumPath(address): AxumPath<String>,
) -> impl IntoResponse {
    let address = match parse_address_hex(&address) {
        Ok(address) => address,
        Err(error) => return rpc_error(StatusCode::BAD_REQUEST, error),
    };
    match state.node.lock() {
        Ok(node) => (
            StatusCode::OK,
            [("content-type", "application/json")],
            balance_json(&node, &address),
        )
            .into_response(),
        Err(_) => rpc_error(StatusCode::INTERNAL_SERVER_ERROR, "state_lock_failed"),
    }
}

async fn rpc_latest_blocks(State(state): State<RpcState>) -> impl IntoResponse {
    match state.node.lock() {
        Ok(node) => {
            let tip = node.tip_height().unwrap_or(Height(0)).0;
            let start = tip.saturating_sub(9);
            let mut blocks = Vec::new();
            for height in (start..=tip).rev() {
                match node.storage.load_block_by_height(Height(height)) {
                    Ok(Some(block)) => blocks.push(block_response(&block, None)),
                    Ok(None) => {}
                    Err(error) => {
                        return rpc_error(
                            StatusCode::INTERNAL_SERVER_ERROR,
                            format!("failed to load block: {error}"),
                        );
                    }
                }
            }
            Json(blocks).into_response()
        }
        Err(_) => rpc_error(StatusCode::INTERNAL_SERVER_ERROR, "state_lock_failed"),
    }
}

async fn rpc_block_by_height(
    State(state): State<RpcState>,
    AxumPath(height): AxumPath<u64>,
) -> impl IntoResponse {
    match state.node.lock() {
        Ok(node) => match node.storage.load_block_by_height(Height(height)) {
            Ok(Some(block)) => Json(block_response(&block, None)).into_response(),
            Ok(None) => rpc_error(StatusCode::NOT_FOUND, "block_not_found"),
            Err(error) => rpc_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to load block: {error}"),
            ),
        },
        Err(_) => rpc_error(StatusCode::INTERNAL_SERVER_ERROR, "state_lock_failed"),
    }
}

async fn rpc_block_by_hash(
    State(state): State<RpcState>,
    AxumPath(hash): AxumPath<String>,
) -> impl IntoResponse {
    let hash = match parse_hash_hex(&hash) {
        Ok(hash) => hash,
        Err(error) => return rpc_error(StatusCode::BAD_REQUEST, error),
    };
    match state.node.lock() {
        Ok(node) => match node.storage.load_block_by_hash(&hash) {
            Ok(Some(block)) => Json(block_response(&block, None)).into_response(),
            Ok(None) => rpc_error(StatusCode::NOT_FOUND, "block_not_found"),
            Err(error) => rpc_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to load block: {error}"),
            ),
        },
        Err(_) => rpc_error(StatusCode::INTERNAL_SERVER_ERROR, "state_lock_failed"),
    }
}

async fn rpc_tx(
    State(state): State<RpcState>,
    AxumPath(hash): AxumPath<String>,
) -> impl IntoResponse {
    let hash = match parse_hash_hex(&hash) {
        Ok(hash) => hash,
        Err(error) => return rpc_error(StatusCode::BAD_REQUEST, error),
    };
    match state.node.lock() {
        Ok(node) => match find_transaction(&node, &hash) {
            Ok(Some(transaction)) => Json(transaction).into_response(),
            Ok(None) => rpc_error(StatusCode::NOT_FOUND, "transaction_not_found"),
            Err(error) => rpc_error(StatusCode::INTERNAL_SERVER_ERROR, error),
        },
        Err(_) => rpc_error(StatusCode::INTERNAL_SERVER_ERROR, "state_lock_failed"),
    }
}

async fn rpc_address(
    State(state): State<RpcState>,
    AxumPath(address): AxumPath<String>,
) -> impl IntoResponse {
    let address = match parse_address_hex(&address) {
        Ok(address) => address,
        Err(error) => return rpc_error(StatusCode::BAD_REQUEST, error),
    };
    match state.node.lock() {
        Ok(node) => match address_transactions(&node, &address) {
            Ok(transactions) => {
                let balance: serde_json::Value = serde_json::from_str(&balance_json(
                    &node, &address,
                ))
                .unwrap_or_else(|_| serde_json::json!({ "error": "balance_encode_failed" }));
                Json(AddressResponse {
                    address: address_to_string(&address),
                    balance,
                    transactions,
                })
                .into_response()
            }
            Err(error) => rpc_error(StatusCode::INTERNAL_SERVER_ERROR, error),
        },
        Err(_) => rpc_error(StatusCode::INTERNAL_SERVER_ERROR, "state_lock_failed"),
    }
}

async fn rpc_accounts(State(state): State<RpcState>) -> impl IntoResponse {
    match state.node.lock() {
        Ok(node) => {
            let height = node.tip_height().unwrap_or(Height(0));
            let accounts = node
                .ledger
                .accounts
                .values()
                .map(|account| {
                    let pending = node.pending_balance(&account.address);
                    AccountResponse {
                        address: address_to_string(&account.address),
                        confirmed: account.balance.0,
                        available: account.available_balance_at(height).0,
                        unspendable: account.unspendable_balance_at(height).0,
                        pending_incoming: pending.incoming.0,
                        pending_outgoing: pending.outgoing.0,
                        nonce: account.nonce.0,
                        credits: account.credits.len(),
                    }
                })
                .collect::<Vec<_>>();
            Json(accounts).into_response()
        }
        Err(_) => rpc_error(StatusCode::INTERNAL_SERVER_ERROR, "state_lock_failed"),
    }
}

async fn rpc_mempool(State(state): State<RpcState>) -> impl IntoResponse {
    match state.node.lock() {
        Ok(node) => {
            let transactions = node
                .mempool
                .transactions()
                .map(|transaction| tx_response(transaction, None, None, "pending"))
                .collect::<Vec<_>>();
            Json(MempoolResponse {
                size: node.mempool.len(),
                transactions,
            })
            .into_response()
        }
        Err(_) => rpc_error(StatusCode::INTERNAL_SERVER_ERROR, "state_lock_failed"),
    }
}

async fn rpc_submit_tx(
    State(state): State<RpcState>,
    Json(request): Json<SubmitTxRequest>,
) -> impl IntoResponse {
    let transaction = match signed_transaction_from_hex(&request.tx) {
        Ok(transaction) => transaction,
        Err(error) => return rpc_error(StatusCode::BAD_REQUEST, error),
    };
    let hash = transaction.hash();
    match state.node.lock() {
        Ok(mut node) => {
            if let Err(error) = node.submit_transaction(transaction.clone()) {
                return rpc_error(
                    StatusCode::BAD_REQUEST,
                    format!("failed to submit transaction: {error}"),
                );
            }
            if let Err(error) = node.flush_to_storage() {
                return rpc_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("failed to flush transaction: {error}"),
                );
            }
        }
        Err(_) => return rpc_error(StatusCode::INTERNAL_SERVER_ERROR, "state_lock_failed"),
    }
    broadcast_to_peers(&state.peers, NetworkMessage::Transaction(transaction));
    Json(SubmitTxResponse {
        accepted: true,
        hash: hex::encode(hash.0),
    })
    .into_response()
}

fn rpc_error(status: StatusCode, error: impl Into<String>) -> axum::response::Response {
    (
        status,
        Json(ErrorResponse {
            error: error.into(),
        }),
    )
        .into_response()
}

fn broadcast_to_peers(peers: &Arc<Mutex<HashMap<SocketAddr, PeerState>>>, message: NetworkMessage) {
    let peers = match peers.lock() {
        Ok(peers) => peers.keys().copied().collect::<Vec<_>>(),
        Err(_) => {
            eprintln!("peer state lock poisoned");
            return;
        }
    };
    for peer in peers {
        if let Err(error) = send_message(peer, message.clone()) {
            eprintln!("broadcast to {peer} failed: {error}");
        }
    }
}

fn block_response(block: &Block, status: Option<&'static str>) -> BlockResponse {
    let block_hash = block.hash();
    BlockResponse {
        height: block.height().0,
        hash: hex::encode(block_hash.0),
        short_hash: short_hash(Some(block_hash)),
        previous_hash: hex::encode(block.previous_hash().0),
        merkle_root: hex::encode(block.header.merkle_root.0),
        state_root: hex::encode(block.state_root().0),
        miner_address: address_to_string(&block.miner_address()),
        difficulty: block.difficulty(),
        timestamp: block.timestamp(),
        nonce: block.header.nonce.0,
        tx_count: block.transaction_count(),
        size: block.serialized_size(),
        coinbase: block.coinbase.as_ref().map(|coinbase| CoinbaseResponse {
            to: address_to_string(&coinbase.to),
            subsidy: coinbase.subsidy.0,
            fees: coinbase.fees.0,
            total: coinbase.total().0,
        }),
        genesis_allocations: block
            .genesis_allocations
            .iter()
            .map(|allocation| GenesisAllocationResponse {
                to: address_to_string(&allocation.to),
                amount: allocation.amount.0,
            })
            .collect(),
        transactions: block
            .transactions
            .iter()
            .map(|transaction| {
                tx_response(
                    transaction,
                    Some(block.height()),
                    Some(block_hash),
                    status.unwrap_or("confirmed"),
                )
            })
            .collect(),
    }
}

fn tx_response(
    transaction: &SignedTransaction,
    block_height: Option<Height>,
    block_hash: Option<paqus::types::Hash>,
    status: &'static str,
) -> TxResponse {
    TxResponse {
        hash: hex::encode(transaction.hash().0),
        from: address_to_string(&transaction.payload.from),
        to: address_to_string(&transaction.payload.to),
        amount: transaction.payload.amount.0,
        fee: transaction.payload.fee.0,
        nonce: transaction.payload.nonce.0,
        block_height: block_height.map(|height| height.0),
        block_hash: block_hash.map(|hash| hex::encode(hash.0)),
        status,
    }
}

fn find_transaction(node: &Node, hash: &paqus::types::Hash) -> Result<Option<TxResponse>, String> {
    for transaction in node.mempool.transactions() {
        if transaction.hash() == *hash {
            return Ok(Some(tx_response(transaction, None, None, "pending")));
        }
    }

    let tip = node.tip_height().unwrap_or(Height(0)).0;
    for height in 0..=tip {
        let block = node
            .storage
            .load_block_by_height(Height(height))
            .map_err(|error| format!("failed to load block: {error}"))?;
        let Some(block) = block else {
            continue;
        };
        for transaction in &block.transactions {
            if transaction.hash() == *hash {
                return Ok(Some(tx_response(
                    transaction,
                    Some(block.height()),
                    Some(block.hash()),
                    "confirmed",
                )));
            }
        }
    }
    Ok(None)
}

fn address_transactions(node: &Node, address: &Address) -> Result<Vec<TxResponse>, String> {
    let mut transactions = Vec::new();
    let tip = node.tip_height().unwrap_or(Height(0)).0;
    for height in 0..=tip {
        let block = node
            .storage
            .load_block_by_height(Height(height))
            .map_err(|error| format!("failed to load block: {error}"))?;
        let Some(block) = block else {
            continue;
        };
        for transaction in &block.transactions {
            if transaction.payload.from == *address || transaction.payload.to == *address {
                transactions.push(tx_response(
                    transaction,
                    Some(block.height()),
                    Some(block.hash()),
                    "confirmed",
                ));
            }
        }
    }

    for transaction in node.mempool.transactions() {
        if transaction.payload.from == *address || transaction.payload.to == *address {
            transactions.push(tx_response(transaction, None, None, "pending"));
        }
    }

    transactions.reverse();
    Ok(transactions)
}

fn run_node(args: &[String]) -> Result<(), String> {
    let mut config = parse_run_config(args)?;
    print_core_startup_info();
    if let Some(path) = &config.peers_file {
        config.peers.extend(load_peers_file(path)?);
    }
    dedupe_peers(&mut config.peers);
    if config.peers.len() > config.max_peers {
        config.peers.truncate(config.max_peers);
    }
    let node = open_node(&config.db_path, config.miner_address)?;
    let listener = bind_nonblocking(config.listen_addr, "p2p")?;
    let listen_addr = listener
        .local_addr()
        .map_err(|error| format!("failed to read listener address: {error}"))?;
    let node = Arc::new(Mutex::new(node));

    let mut service = NodeService::new(node.clone(), config, listener);
    service.preflight()?;

    let (height, tip_hash, difficulty) = {
        let node = node
            .lock()
            .map_err(|_| "node state lock poisoned".to_string())?;
        (
            node.tip_height().unwrap_or(Height(0)).0,
            short_hash(node.tip_hash()),
            format_difficulty(node.next_difficulty()),
        )
    };

    println!(
        "paqus node running db={} p2p={} rpc={} height={} tip={} difficulty={} peers={} mining={}",
        service.config.db_path,
        listen_addr,
        service.config.rpc_addr,
        height,
        tip_hash,
        difficulty,
        service.config.peers.len(),
        service.config.mine
    );

    let rpc_state = RpcState {
        node,
        peers: service.peers.clone(),
        mining: service.config.mine,
    };
    let _rpc_handle = start_rpc_server(rpc_state, service.config.rpc_addr)?;
    service.run()
}

fn print_core_startup_info() {
    println!(
        "core chain={} chain_id={} coin={} stage={} protocol={} storage={} magic={}",
        CHAIN_NAME,
        CHAIN_ID,
        COIN_NAME,
        PROTOCOL_STAGE,
        PROTOCOL_VERSION,
        STORAGE_VERSION,
        hex::encode(NETWORK_MAGIC)
    );
    println!(
        "consensus block_time={}s finality={} reward_maturity={} difficulty_start={} retarget_window={}",
        BLOCK_TIME,
        FINALITY_DEPTH,
        BLOCK_REWARD_MATURITY,
        DIFFICULTY_START,
        DIFFICULTY_ADJUSTMENT_INTERVAL
    );
}

fn bind_nonblocking(addr: SocketAddr, label: &str) -> Result<TcpListener, String> {
    let listener = TcpListener::bind(addr)
        .map_err(|error| format!("failed to bind {label} {addr}: {error}"))?;
    listener
        .set_nonblocking(true)
        .map_err(|error| format!("failed to set {label} listener nonblocking: {error}"))?;
    Ok(listener)
}

fn parse_run_config(args: &[String]) -> Result<RunConfig, String> {
    let mut config = RunConfig::default();
    let mut index = 0;

    while index < args.len() {
        match args[index].as_str() {
            "--db" | "--db-path" => {
                index += 1;
                config.db_path = args
                    .get(index)
                    .ok_or_else(|| "missing value for --db".to_string())?
                    .clone();
            }
            "--listen" => {
                index += 1;
                config.listen_addr = parse_socket(args.get(index), "--listen")?;
            }
            "--rpc-listen" => {
                index += 1;
                config.rpc_addr = parse_socket(args.get(index), "--rpc-listen")?;
            }
            "--peer" => {
                index += 1;
                config.peers.push(parse_socket(args.get(index), "--peer")?);
            }
            "--peers-file" => {
                index += 1;
                config.peers_file = Some(
                    args.get(index)
                        .ok_or_else(|| "missing value for --peers-file".to_string())?
                        .clone(),
                );
            }
            "--shutdown-file" => {
                index += 1;
                config.shutdown_file = args
                    .get(index)
                    .ok_or_else(|| "missing value for --shutdown-file".to_string())?
                    .clone();
            }
            "--max-peers" => {
                index += 1;
                config.max_peers = args
                    .get(index)
                    .ok_or_else(|| "missing value for --max-peers".to_string())?
                    .parse::<usize>()
                    .map_err(|error| format!("invalid max peers: {error}"))?
                    .max(1);
            }
            "--miner" => {
                index += 1;
                config.miner_address = parse_address(args.get(index))?;
            }
            "--wallet" => {
                index += 1;
                apply_wallet_file(&mut config, args.get(index))?;
            }
            "--miner-secret-key" => {
                index += 1;
                config.miner_secret_key = Some(parse_secret_key(args.get(index))?);
            }
            "--premine" => {
                return Err(
                    "premine address is fixed by protocol and cannot be overridden".to_string(),
                );
            }
            "--mine" => config.mine = true,
            "--mine-interval-secs" => {
                index += 1;
                let secs = args
                    .get(index)
                    .ok_or_else(|| "missing value for --mine-interval-secs".to_string())?
                    .parse::<u64>()
                    .map_err(|error| format!("invalid mining interval: {error}"))?;
                config.mine_interval = Duration::from_secs(secs);
            }
            "--mine-attempts" => {
                index += 1;
                config.mine_attempts = args
                    .get(index)
                    .ok_or_else(|| "missing value for --mine-attempts".to_string())?
                    .parse::<u64>()
                    .map_err(|error| format!("invalid mining attempts: {error}"))?;
            }
            value if !value.starts_with('-') && config.db_path == DEFAULT_NODE_DB => {
                config.db_path = value.to_string();
            }
            value => return Err(format!("unknown node run option `{value}`")),
        }
        index += 1;
    }

    Ok(config)
}

fn apply_wallet_file(config: &mut RunConfig, path: Option<&String>) -> Result<(), String> {
    let path = path.ok_or_else(|| "missing value for --wallet".to_string())?;
    let wallet = load_wallet(path)?;

    config.miner_address = wallet.address;
    config.miner_secret_key = Some(wallet.secret_key);
    Ok(())
}

fn load_wallet(path: &str) -> Result<Wallet, String> {
    let contents = fs::read_to_string(path)
        .map_err(|error| format!("failed to read wallet file {path}: {error}"))?;
    let wallet: WalletFile = serde_json::from_str(&contents)
        .map_err(|error| format!("failed to parse wallet file {path}: {error}"))?;
    let address_arg = Some(wallet.address);
    let secret_key_arg = Some(wallet.secret_key);
    let address = parse_address(address_arg.as_ref())?;
    let secret_key = parse_secret_key(secret_key_arg.as_ref())?;
    let wallet = Wallet::from_secret_key(secret_key);
    if wallet.address != address {
        return Err("wallet address does not match secret key".to_string());
    }
    Ok(wallet)
}

fn load_peers_file(path: &str) -> Result<Vec<SocketAddr>, String> {
    let contents = match fs::read_to_string(path) {
        Ok(contents) => contents,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(format!("failed to read peers file {path}: {error}")),
    };
    let mut peers = Vec::new();
    for (index, line) in contents.lines().enumerate() {
        let line = line.split('#').next().unwrap_or_default().trim();
        if line.is_empty() {
            continue;
        }
        peers.push(
            line.parse()
                .map_err(|error| format!("invalid peer in {path} line {}: {error}", index + 1))?,
        );
    }
    Ok(peers)
}

fn dedupe_peers(peers: &mut Vec<SocketAddr>) {
    let mut seen = HashSet::new();
    peers.retain(|peer| seen.insert(*peer));
}

fn parse_socket(value: Option<&String>, flag: &str) -> Result<SocketAddr, String> {
    value
        .ok_or_else(|| format!("missing value for {flag}"))?
        .parse()
        .map_err(|error| format!("invalid socket address for {flag}: {error}"))
}

fn configure_stream(stream: &TcpStream, timeout: Duration) -> Result<(), String> {
    stream
        .set_read_timeout(Some(timeout))
        .map_err(|error| format!("failed to set read timeout: {error}"))?;
    stream
        .set_write_timeout(Some(timeout))
        .map_err(|error| format!("failed to set write timeout: {error}"))?;
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PeerPoll {
    Idle,
    Synced,
}

fn poll_peer(peer: SocketAddr, node: &mut Node) -> Result<PeerPoll, String> {
    handshake_peer(peer, node)?;
    let tip = request_tip(peer)?;
    let local_height = node.tip_height().unwrap_or(Height(0)).0;
    if tip.0 <= local_height {
        return Ok(PeerPoll::Idle);
    }

    let target = tip.0.min(local_height.saturating_add(MAX_BLOCKS_PER_SYNC));
    for height in local_height.saturating_add(1)..=target {
        request_block(peer, node, Height(height))?;
    }
    Ok(PeerPoll::Synced)
}

fn handshake_peer(peer: SocketAddr, node: &Node) -> Result<(), String> {
    let version = VersionInfo::local(
        node.tip_height()
            .zip(node.tip_hash())
            .map(|(height, hash)| TipInfo { height, hash }),
    );
    match roundtrip(peer, NetworkMessage::Version(version))? {
        NetworkMessage::VerAck(remote) => remote
            .validate_compatibility()
            .map_err(|reason| format!("peer returned incompatible version: {reason:?}")),
        NetworkMessage::Reject { reason, message } => {
            Err(format!("peer rejected handshake: {reason:?}: {message}"))
        }
        _ => Err("peer returned unexpected handshake response".to_string()),
    }
}

fn request_tip(peer: SocketAddr) -> Result<Height, String> {
    match roundtrip(peer, NetworkMessage::GetTip)? {
        NetworkMessage::Tip(tip) => Ok(tip.height),
        _ => Err("peer returned unexpected tip response".to_string()),
    }
}

fn request_peers(peer: SocketAddr) -> Result<Vec<PeerInfo>, String> {
    match roundtrip(peer, NetworkMessage::GetPeers)? {
        NetworkMessage::Peers(peers) => Ok(peers),
        _ => Err("peer returned unexpected peers response".to_string()),
    }
}

fn request_block(peer: SocketAddr, node: &mut Node, height: Height) -> Result<(), String> {
    let response = roundtrip(peer, NetworkMessage::GetBlockByHeight { height })?;
    let NetworkMessage::Block(block) = response else {
        return Err(format!("peer did not return block at height {}", height.0));
    };
    node.apply_block(block)
        .map_err(|error| format!("failed to apply block {} from {peer}: {error}", height.0))?;
    node.flush_to_storage()
        .map_err(|error| format!("failed to flush synced block: {error}"))?;
    println!(
        "synced height={} tip={}",
        node.tip_height().unwrap_or(Height(0)).0,
        format_hash(node.tip_hash())
    );
    Ok(())
}

fn roundtrip(peer: SocketAddr, message: NetworkMessage) -> Result<NetworkMessage, String> {
    let mut stream = TcpStream::connect_timeout(&peer, Duration::from_secs(2))
        .map_err(|error| format!("connect failed: {error}"))?;
    configure_stream(&stream, Duration::from_secs(5))?;
    write_message(&mut stream, &message.to_envelope())
        .map_err(|error| format!("send failed: {error}"))?;
    read_message(&mut stream)
        .map(|envelope| envelope.message)
        .map_err(|error| format!("read failed: {error}"))
}

fn send_message(peer: SocketAddr, message: NetworkMessage) -> Result<(), String> {
    let mut stream = TcpStream::connect_timeout(&peer, Duration::from_secs(2))
        .map_err(|error| format!("connect failed: {error}"))?;
    configure_stream(&stream, Duration::from_secs(5))?;
    write_message(&mut stream, &message.to_envelope())
        .map_err(|error| format!("send failed: {error}"))
}

fn mine_once(node: &mut Node, _config: &RunConfig) -> Result<Option<Block>, String> {
    // Mining requires at least one transaction in the mempool
    // Transactions can be submitted via RPC or received from peers
    // For bootstrapping, see: paqus --help

    let timestamp = unix_timestamp()?;
    match node.mine_block(
        _config.miner_address,
        timestamp,
        _config.mine_attempts,
        MAX_BLOCK_TXS,
    ) {
        Ok(result) => {
            node.flush_to_storage()
                .map_err(|error| format!("failed to flush mined block: {error}"))?;
            println!(
                "mined height={} hash={} difficulty={} txs={} attempts={}",
                result.block.height().0,
                short_hash(Some(result.block.hash())),
                result.block.difficulty(),
                result.block.transactions.len(),
                result.attempts
            );
            Ok(Some(result.block))
        }
        Err(error) => {
            eprintln!("mining skipped: {error}");
            Ok(None)
        }
    }
}

fn parse_secret_key(value: Option<&String>) -> Result<SecretKey, String> {
    let Some(value) = value else {
        return Err("missing secret key hex".to_string());
    };
    let bytes = hex::decode(value).map_err(|_| "invalid secret key hex".to_string())?;
    let bytes = bytes
        .try_into()
        .map_err(|_| "secret key has invalid length".to_string())?;
    Ok(SecretKey(bytes))
}

fn parse_amount(value: Option<&String>, flag: &str) -> Result<Amount, String> {
    let value = value.ok_or_else(|| format!("missing value for {flag}"))?;
    value
        .parse::<u32>()
        .map(Amount)
        .map_err(|error| format!("invalid amount for {flag}: {error}"))
}

fn parse_nonce(value: Option<&String>) -> Result<Nonce, String> {
    let value = value.ok_or_else(|| "missing value for --nonce".to_string())?;
    value
        .parse::<u64>()
        .map(Nonce)
        .map_err(|error| format!("invalid nonce: {error}"))
}

fn signed_transaction_to_hex(transaction: &SignedTransaction) -> Result<String, String> {
    borsh::to_vec(transaction)
        .map(hex::encode)
        .map_err(|error| format!("failed to encode transaction: {error}"))
}

fn signed_transaction_from_hex(value: &str) -> Result<SignedTransaction, String> {
    let bytes = hex::decode(value).map_err(|_| "invalid transaction hex".to_string())?;
    SignedTransaction::try_from_slice(&bytes)
        .map_err(|error| format!("invalid signed transaction bytes: {error}"))
}

fn http_post_json(addr: &str, path: &str, body: &str) -> Result<String, String> {
    let addr = addr
        .parse::<SocketAddr>()
        .map_err(|error| format!("invalid rpc address: {error}"))?;
    let mut stream = TcpStream::connect_timeout(&addr, Duration::from_secs(3))
        .map_err(|error| format!("failed to connect rpc: {error}"))?;
    configure_stream(&stream, Duration::from_secs(5))?;
    let request = format!(
        "POST {path} HTTP/1.1\r\nhost: {addr}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
        body.len(),
        body
    );
    stream
        .write_all(request.as_bytes())
        .map_err(|error| format!("failed to write rpc request: {error}"))?;
    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .map_err(|error| format!("failed to read rpc response: {error}"))?;
    Ok(response
        .split_once("\r\n\r\n")
        .map(|(_, body)| body.to_string())
        .unwrap_or(response))
}

fn parse_address(value: Option<&String>) -> Result<Address, String> {
    let Some(value) = value else {
        return Err("missing address hex".to_string());
    };
    parse_address_hex(value)
}

fn parse_address_hex(value: &str) -> Result<Address, String> {
    let bytes = hex::decode(value).map_err(|_| "invalid address hex".to_string())?;
    let bytes = bytes
        .try_into()
        .map_err(|_| "address has invalid length".to_string())?;
    Ok(Address(bytes))
}

fn parse_hash_hex(value: &str) -> Result<paqus::types::Hash, String> {
    let bytes = hex::decode(value).map_err(|_| "invalid hash hex".to_string())?;
    let bytes = bytes
        .try_into()
        .map_err(|_| "hash has invalid length".to_string())?;
    Ok(paqus::types::Hash(bytes))
}

fn unix_timestamp() -> Result<u64, String> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .map_err(|_| "system clock is before unix epoch".to_string())
}

fn format_hash(hash: Option<paqus::types::Hash>) -> String {
    hash.map(|hash| hex::encode(hash.0))
        .unwrap_or_else(|| "none".to_string())
}

fn short_hash(hash: Option<paqus::types::Hash>) -> String {
    let hash = format_hash(hash);
    if hash.len() <= 16 {
        return hash;
    }
    format!("{}..{}", &hash[..8], &hash[hash.len() - 8..])
}

fn format_difficulty(difficulty: Result<u32, impl std::fmt::Display>) -> String {
    difficulty
        .map(|difficulty| difficulty.to_string())
        .unwrap_or_else(|error| format!("error:{error}"))
}

fn print_help() {
    println!(
        "\
paqus

Usage:
  paqus --help
  paqus version
  paqus node info
  paqus node init [db-path] [miner-address-hex]
  paqus node run [db-path] [--listen addr] [--rpc-listen addr] [--peer addr] [--peers-file path] [--wallet path] [--miner address-hex] [--miner-secret-key key-hex] [--mine]
  paqus wallet new [--show-secret]
  paqus wallet address <secret-key-hex>
  paqus wallet balance <address-hex> [db-path]
  paqus wallet send --wallet path --to address-hex --amount units --nonce n [--fee units] [--submit] [--rpc addr]

RPC:
  GET  /status
  GET  /health
  GET  /peers
  GET  /balance/<address-hex>
  POST /tx              JSON: {{\"tx\":\"signed-transaction-hex\"}}

Note: Mining requires at least one transaction in the mempool. Transactions can be submitted via RPC or received from peer nodes. To bootstrap mining with your own account:
  1. Create a wallet: paqus wallet new wallet.json
  2. Initialize node: paqus node init [db] [miner-addr]
  3. Run with mining enabled: paqus node run [db] --wallet wallet.json --mine
"
    );
}

fn print_version() {
    println!(
        "{} {} ({}, protocol {})",
        CHAIN_NAME,
        env!("CARGO_PKG_VERSION"),
        PROTOCOL_STAGE,
        PROTOCOL_VERSION
    );
}

fn print_network_info() {
    println!("chain: {CHAIN_NAME}");
    println!("coin: {COIN_NAME}");
    println!("stage: {PROTOCOL_STAGE}");
    println!("protocol_version: {PROTOCOL_VERSION}");
    println!("block_time_secs: {BLOCK_TIME}");
    println!("finality_depth: {FINALITY_DEPTH}");
    println!("difficulty_start: {DIFFICULTY_START}");
}
