mod gateway;
mod gossip;
mod libp2p_node;
mod mempool;
mod network;
mod p2p;
mod paquscore;
mod runtime;

use axum::{
    Json, Router,
    extract::{Path as AxumPath, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
};
use borsh::BorshDeserialize;
use gateway::{heartbeat_peer, register_peer, request_gateway_peers};
use gossip::{BroadcastReport, broadcast_to_peers};
use mempool::resolve_wallet_nonce;
use network::{bind_nonblocking, configure_stream, http_get, http_post_json};
use p2p::{
    PERSISTENT_PEER_TIMEOUT, PeerConnection, PeerPoll, PeerState, dedupe_peers, load_peers_file,
    poll_peer_connection, request_peers_connection, save_peers_file, sync_from_peers_parallel,
    sync_mempool_connection,
};
use paquscore::{
    Address, Amount, Block, BlockHash, Consensus, GENESIS_PREMINE_ADDRESS, Hash, Height,
    InventoryItem, NetworkMessage, Node, Nonce, PeerInfo, SecretKey, SignedTransaction,
    Transaction, TransactionHash, Wallet, address_from_public_key, address_from_string,
    address_to_string, derive_public_key, handle_message, read_message, write_message,
};
use paquscore::{
    BLOCK_REWARD_MATURITY, BLOCK_TIME, CHAIN_ID, CHAIN_NAME, COIN_NAME, CONFIRMATION_DEPTH,
    DEFAULT_TRANSACTION_FEE, DIFFICULTY_ADJUSTMENT_INTERVAL, DIFFICULTY_START, FINALITY_DEPTH,
    GENESIS_PREMINE, MAX_BLOCK_TXS, NETWORK_MAGIC, PROTOCOL_STAGE, PROTOCOL_VERSION,
    STORAGE_VERSION,
};
use runtime::mempool::MempoolConfig;
use runtime::miner::{MiningConfig, mine_prepared_block, prepare_candidate_block};
use runtime::network::NetworkError;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet, hash_map::Entry};
use std::env;
use std::fs;
use std::io;
use std::io::Write as IoWrite;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::process::ExitCode;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU64, Ordering},
};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const DEFAULT_NODE_DB: &str = "./data/paqus";
const DEFAULT_LISTEN_ADDR: &str = "0.0.0.0:5555";
const DEFAULT_RPC_ADDR: &str = "[2404:8000:1044:4d8:1202:b5ff:feb0:7020]:6666";
const DEFAULT_BOOTSTRAP_PEER: &str = "[2404:8000:1044:4d8:1202:b5ff:feb0:7020]:5555";
const DEFAULT_CONFIG_FILE: &str = "./data/paqus/node.json";
const DEFAULT_PEERS_FILE: &str = "./data/paqus/peers.json";
const DEFAULT_MINING_INTERVAL: Duration = Duration::from_secs(BLOCK_TIME as u64);
const DEFAULT_MAX_PEERS: usize = 128;
const DEFAULT_SHUTDOWN_FILE: &str = "./data/paqus/STOP";
const DEFAULT_GATEWAY_HEARTBEAT: Duration = Duration::from_secs(60);
const MAX_PEER_FAILURES: u32 = 3;
const ACTIVITY_LOG_INTERVAL: Duration = Duration::from_secs(15);

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
        None => interactive_menu(),
        Some("-h") | Some("--help") | Some("help") => {
            print_help();
            Ok(())
        }
        Some("-V") | Some("--version") | Some("version") => {
            print_version();
            Ok(())
        }
        Some("wallet") => wallet_command(&args[1..]),
        Some("node") => node_command(&args[1..]),
        Some("menu") | Some("cli") => interactive_menu(),
        Some(command) => Err(format!("unknown command `{command}`. Try `paqus --help`.")),
    }
}

fn interactive_menu() -> Result<(), String> {
    loop {
        println!();
        println!("Paqus Node CLI");
        println!("1. Create wallet");
        println!("2. Import wallet");
        println!("3. Run node");
        println!("4. Check balance");
        println!("5. Send");
        println!("6. Receive");
        println!("7. Explorer");
        println!("8. Exit");

        match prompt("Select menu")?.as_str() {
            "1" => menu_create_wallet()?,
            "2" => menu_import_wallet()?,
            "3" => {
                println!(
                    "Starting node. Press Ctrl+C to stop, or create the STOP file from another terminal."
                );
                return run_node(&[]);
            }
            "4" => menu_check_balance()?,
            "5" => menu_send()?,
            "6" => menu_receive()?,
            "7" => menu_explorer()?,
            "8" => return Ok(()),
            value => println!("Unknown menu `{value}`"),
        }
    }
}

fn menu_create_wallet() -> Result<(), String> {
    let path = prompt_default("Wallet file", "wallet.json")?;
    if std::path::Path::new(&path).exists() && !prompt_yes_no("File exists. Overwrite?")? {
        return Ok(());
    }
    let wallet = Wallet::generate();
    save_wallet_file(&path, &wallet)?;
    println!("wallet: {path}");
    println!("address: {}", wallet.wallet_address());
    Ok(())
}

fn menu_import_wallet() -> Result<(), String> {
    let secret_key = parse_secret_key(Some(&prompt("Secret key hex")?))?;
    let wallet = Wallet::from_secret_key(secret_key);
    let path = prompt_default("Wallet file", "wallet.json")?;
    if std::path::Path::new(&path).exists() && !prompt_yes_no("File exists. Overwrite?")? {
        return Ok(());
    }
    save_wallet_file(&path, &wallet)?;
    println!("wallet imported: {path}");
    println!("address: {}", wallet.wallet_address());
    Ok(())
}

fn menu_check_balance() -> Result<(), String> {
    let address = match choose_wallet("Select wallet for balance")? {
        Some((_, wallet)) => wallet.address,
        None => parse_address(Some(&prompt("Address hex")?))?,
    };
    let db_path = prompt_default("Node DB path", DEFAULT_NODE_DB)?;
    let node = open_node(&db_path, Address([9; 20]))?;
    println!("{}", balance_json(&node, &address));
    Ok(())
}

fn menu_send() -> Result<(), String> {
    let Some((wallet_path, _)) = choose_wallet("Select wallet to send from")? else {
        println!("No wallet selected.");
        return Ok(());
    };
    let to = parse_address(Some(&prompt("Recipient address")?))?;
    let amount = parse_amount(Some(&prompt("Amount")?), "amount")?;
    let fee = parse_amount(
        Some(&prompt_default(
            "Fee",
            &DEFAULT_TRANSACTION_FEE.to_string(),
        )?),
        "fee",
    )?;
    let rpc_addr = prompt_default("RPC address", DEFAULT_RPC_ADDR)?;
    submit_wallet_payment(&wallet_path, to, amount, fee, None, &rpc_addr)
}

fn menu_receive() -> Result<(), String> {
    let wallets = discover_wallets();
    if wallets.is_empty() {
        println!("No wallet files found. Create or import a wallet first.");
        return Ok(());
    }
    if wallets.len() == 1 {
        println!("{}", wallets[0].1.wallet_address());
        return Ok(());
    }
    for (index, (path, wallet)) in wallets.iter().enumerate() {
        println!("{}. {} ({})", index + 1, wallet.wallet_address(), path);
    }
    let choice = prompt("Select address")?;
    let index = choice
        .parse::<usize>()
        .map_err(|error| format!("invalid selection: {error}"))?
        .checked_sub(1)
        .ok_or_else(|| "invalid selection".to_string())?;
    let Some((_, wallet)) = wallets.get(index) else {
        return Err("invalid selection".to_string());
    };
    println!("{}", wallet.wallet_address());
    Ok(())
}

fn menu_explorer() -> Result<(), String> {
    let address = match choose_wallet("Select wallet/address for transactions")? {
        Some((_, wallet)) => wallet.address,
        None => parse_address(Some(&prompt("Address hex")?))?,
    };
    let rpc_addr = prompt_default("RPC address", DEFAULT_RPC_ADDR)?;
    let body = http_get(
        &rpc_addr,
        &format!("/address/{}", address_to_string(&address)),
    )?;
    let value: serde_json::Value = serde_json::from_str(&body)
        .map_err(|error| format!("failed to parse explorer response: {error}"))?;
    let transactions = value
        .get("transactions")
        .cloned()
        .unwrap_or_else(|| serde_json::json!([]));
    println!(
        "{}",
        serde_json::to_string_pretty(&transactions)
            .map_err(|error| format!("failed to render transactions: {error}"))?
    );
    Ok(())
}

fn choose_wallet(label: &str) -> Result<Option<(String, Wallet)>, String> {
    let wallets = discover_wallets();
    if wallets.is_empty() {
        return Ok(None);
    }
    println!("{label}");
    for (index, (path, wallet)) in wallets.iter().enumerate() {
        println!("{}. {} ({})", index + 1, wallet.wallet_address(), path);
    }
    println!("{}. Manual address", wallets.len() + 1);
    let choice = prompt("Select")?;
    let index = choice
        .parse::<usize>()
        .map_err(|error| format!("invalid selection: {error}"))?;
    if index == wallets.len() + 1 {
        return Ok(None);
    }
    wallets
        .get(index.saturating_sub(1))
        .cloned()
        .map(Some)
        .ok_or_else(|| "invalid selection".to_string())
}

fn discover_wallets() -> Vec<(String, Wallet)> {
    let mut wallets = Vec::new();
    for dir in [".", "./data/paqus"] {
        let Ok(entries) = fs::read_dir(dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().and_then(|ext| ext.to_str()) != Some("json") {
                continue;
            }
            let path_str = path.to_string_lossy().to_string();
            if let Ok(wallet) = load_wallet(&path_str) {
                wallets.push((path_str, wallet));
            }
        }
    }
    wallets.sort_by(|left, right| left.0.cmp(&right.0));
    wallets.dedup_by(|left, right| left.0 == right.0);
    wallets
}

fn save_wallet_file(path: &str, wallet: &Wallet) -> Result<(), String> {
    let json_data = serde_json::json!({
        "address": wallet.wallet_address(),
        "public_key": hex::encode(wallet.public_key.0),
        "secret_key": hex::encode(wallet.secret_key.0),
    });
    let json_str = serde_json::to_string_pretty(&json_data)
        .map_err(|error| format!("failed to serialize wallet: {error}"))?;
    fs::write(path, json_str)
        .map_err(|error| format!("failed to write wallet file `{path}`: {error}"))
}

fn prompt(label: &str) -> Result<String, String> {
    print!("{label}: ");
    io::stdout()
        .flush()
        .map_err(|error| format!("failed to flush stdout: {error}"))?;
    let mut line = String::new();
    io::stdin()
        .read_line(&mut line)
        .map_err(|error| format!("failed to read input: {error}"))?;
    Ok(line.trim().to_string())
}

fn prompt_default(label: &str, default: &str) -> Result<String, String> {
    let value = prompt(&format!("{label} [{default}]"))?;
    if value.is_empty() {
        Ok(default.to_string())
    } else {
        Ok(value)
    }
}

fn prompt_yes_no(label: &str) -> Result<bool, String> {
    let value = prompt(&format!("{label} [y/N]"))?;
    Ok(matches!(value.to_ascii_lowercase().as_str(), "y" | "yes"))
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
                if show_secret {
                    println!("secret_key: {secret_key_hex}");
                }
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
            let address = address_from_public_key(&public_key);
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
        Some("pay") => wallet_pay_command(&args[1..]),
        Some("send") => wallet_send_command(&args[1..]),
        _ => Err("usage: paqus wallet <new|address|balance|pay|send> [options]".to_string()),
    }
}

fn wallet_pay_command(args: &[String]) -> Result<(), String> {
    let to = parse_address(args.first())?;
    let amount = parse_amount(args.get(1), "amount")?;
    let mut wallet_path = "wallet.json".to_string();
    let mut rpc_addr = DEFAULT_RPC_ADDR.to_string();
    let mut fee = Amount(DEFAULT_TRANSACTION_FEE);
    let mut index = 2;

    while index < args.len() {
        match args[index].as_str() {
            "--wallet" => {
                index += 1;
                wallet_path = args
                    .get(index)
                    .ok_or_else(|| "missing value for --wallet".to_string())?
                    .clone();
            }
            "--rpc" | "--rpc-addr" => {
                index += 1;
                rpc_addr = args
                    .get(index)
                    .ok_or_else(|| "missing value for --rpc".to_string())?
                    .clone();
            }
            "--fee" => {
                index += 1;
                fee = parse_amount(args.get(index), "--fee")?;
            }
            value => return Err(format!("unknown wallet pay option `{value}`")),
        }
        index += 1;
    }

    submit_wallet_payment(&wallet_path, to, amount, fee, None, &rpc_addr)
}

fn wallet_send_command(args: &[String]) -> Result<(), String> {
    let short_form = args.len() >= 2 && !args[0].starts_with('-') && !args[1].starts_with('-');
    if short_form {
        let to = parse_address(args.first())?;
        let amount = parse_amount(args.get(1), "amount")?;
        let mut wallet_path = "wallet.json".to_string();
        let mut rpc_addr = DEFAULT_RPC_ADDR.to_string();
        let mut fee = Amount(DEFAULT_TRANSACTION_FEE);
        let mut nonce = None;
        let mut index = 2;

        while index < args.len() {
            match args[index].as_str() {
                "--wallet" => {
                    index += 1;
                    wallet_path = args
                        .get(index)
                        .ok_or_else(|| "missing value for --wallet".to_string())?
                        .clone();
                }
                "--rpc" | "--rpc-addr" => {
                    index += 1;
                    rpc_addr = args
                        .get(index)
                        .ok_or_else(|| "missing value for --rpc".to_string())?
                        .clone();
                }
                "--fee" => {
                    index += 1;
                    fee = parse_amount(args.get(index), "--fee")?;
                }
                "--nonce" => {
                    index += 1;
                    nonce = Some(parse_nonce(args.get(index))?);
                }
                value => return Err(format!("unknown wallet send option `{value}`")),
            }
            index += 1;
        }

        return submit_wallet_payment(&wallet_path, to, amount, fee, nonce, &rpc_addr);
    }

    let mut wallet_path = None;
    let mut to = None;
    let mut amount = None;
    let mut fee = Amount(DEFAULT_TRANSACTION_FEE);
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
    submit_wallet_transaction(&wallet_path, to, amount, fee, nonce, &rpc_addr, submit)
}

fn submit_wallet_payment(
    wallet_path: &str,
    to: Address,
    amount: Amount,
    fee: Amount,
    nonce: Option<Nonce>,
    rpc_addr: &str,
) -> Result<(), String> {
    submit_wallet_transaction(wallet_path, to, amount, fee, nonce, rpc_addr, true)
}

fn submit_wallet_transaction(
    wallet_path: &str,
    to: Address,
    amount: Amount,
    fee: Amount,
    nonce: Option<Nonce>,
    rpc_addr: &str,
    submit: bool,
) -> Result<(), String> {
    let wallet = load_wallet(wallet_path)?;
    let nonce = nonce.unwrap_or(resolve_wallet_nonce(&wallet.address, rpc_addr)?);
    let transaction =
        Transaction::new_at(wallet.address, to, amount, fee, nonce, unix_timestamp()?);
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
            "{{\"tx\":\"{}\",\"hash\":\"{}\",\"from\":\"{}\",\"to\":\"{}\",\"amount\":{},\"fee\":{},\"nonce\":{},\"timestamp\":{}}}",
            tx_hex,
            hex::encode(signed.hash().0),
            address_to_string(&signed.transaction.from),
            address_to_string(&signed.transaction.to),
            signed.transaction.amount.0,
            signed.transaction.fee.0,
            signed.transaction.nonce.0,
            signed.transaction.timestamp
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
        Some("config") => node_config_command(&args[1..]),
        Some("libp2p-info") => {
            print_libp2p_info()?;
            Ok(())
        }
        Some("info") => {
            print_network_info();
            Ok(())
        }
        _ => Err("usage: paqus node <info|libp2p-info|init|config|run> [options]".to_string()),
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

#[derive(Debug)]
struct RunConfig {
    db_path: String,
    listen_addrs: Vec<SocketAddr>,
    rpc_addr: SocketAddr,
    peers: Vec<SocketAddr>,
    peers_file: Option<String>,
    gateway_url: Option<String>,
    public_addrs: Vec<SocketAddr>,
    gateway_heartbeat: Duration,
    shutdown_file: String,
    max_peers: usize,
    min_relay_fee: u64,
    market_fee: u64,
    low_fee_expiry: Duration,
    mempool_expiry: Duration,
    miner_address: Address,
    miner_secret_key: Option<SecretKey>,
    miner_min_fee_rate: Option<u64>,
    mine: bool,
    mine_interval: Duration,
    mine_attempts: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RunConfigFile {
    db_path: String,
    listen_addr: OneOrMany<String>,
    rpc_addr: String,
    peers: Vec<String>,
    peers_file: Option<String>,
    gateway_url: Option<String>,
    public_addr: Option<OneOrMany<String>>,
    gateway_heartbeat_secs: u64,
    shutdown_file: String,
    max_peers: usize,
    #[serde(default)]
    min_relay_fee: Option<u64>,
    #[serde(default)]
    market_fee: Option<u64>,
    #[serde(default)]
    low_fee_expiry_secs: Option<u64>,
    #[serde(default)]
    mempool_expiry_secs: Option<u64>,
    wallet: Option<String>,
    miner_address: Option<String>,
    miner_secret_key: Option<String>,
    #[serde(default)]
    miner_min_fee_rate: Option<u64>,
    mine: bool,
    mine_interval_secs: u64,
    mine_attempts: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
enum OneOrMany<T> {
    One(T),
    Many(Vec<T>),
}

impl<T> OneOrMany<T> {
    fn into_vec(self) -> Vec<T> {
        match self {
            Self::One(value) => vec![value],
            Self::Many(values) => values,
        }
    }
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
    peer_connections: Arc<Mutex<HashMap<SocketAddr, PeerConnection>>>,
    inbound_connections: Arc<Mutex<HashMap<SocketAddr, PeerConnection>>>,
    mining: bool,
    log_counters: Arc<LogCounters>,
    mining_stats: Arc<MiningStats>,
}

#[derive(Default)]
struct LogCounters {
    accepted_tx_total: AtomicU64,
    broadcast_tx_total: AtomicU64,
}

#[derive(Default)]
struct MiningStats {
    last_hashrate_hps: AtomicU64,
    last_attempts: AtomicU64,
}

#[derive(Serialize)]
struct StatusResponse {
    chain: &'static str,
    stage: &'static str,
    protocol_version: u8,
    height: u64,
    tip_hash: String,
    peers: usize,
    known_peers: usize,
    outbound_peers: usize,
    inbound_peers: usize,
    mining: bool,
    hashrate_hps: u64,
    last_mine_attempts: u64,
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
    confirmation_depth: u32,
    finality_depth: u32,
    block_reward_maturity: u32,
    difficulty_start: u32,
}

#[derive(Serialize)]
struct ChainStatsResponse {
    chain: &'static str,
    coin: &'static str,
    height: u64,
    blocks: u64,
    average_block_time_secs: Option<u64>,
    target_block_time_secs: u32,
    genesis_premine: u64,
    mined_supply: u64,
    current_supply: u64,
    total_coinbase_rewards: u64,
    total_fees_collected: u64,
    total_transactions: u64,
    pending_transactions: u64,
    total_transfer_volume: u64,
    total_transaction_fees: u64,
    average_transfer_amount: u64,
}

#[derive(Serialize)]
struct TxResponse {
    hash: String,
    from: String,
    to: String,
    amount: u64,
    fee: u64,
    nonce: u64,
    timestamp: u64,
    age_secs: u64,
    block_height: Option<u64>,
    block_hash: Option<String>,
    status: &'static str,
}

#[derive(Serialize)]
struct CoinbaseResponse {
    to: String,
    subsidy: u64,
    fees: u64,
    total: u64,
}

#[derive(Serialize)]
struct GenesisAllocationResponse {
    to: String,
    amount: u64,
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
    age_secs: u64,
    confirmations: u64,
    block_time_secs: Option<u64>,
    target_block_time_secs: u32,
    block_time_delta_secs: Option<i64>,
    value_moved: u64,
    nonce: u64,
    tx_count: usize,
    size: usize,
    coinbase: Option<CoinbaseResponse>,
    genesis_allocations: Vec<GenesisAllocationResponse>,
    transactions: Vec<TxResponse>,
}

#[derive(Serialize)]
struct MinedBlockResponse {
    height: u64,
    hash: String,
    confirmations: u64,
    maturity_height: u64,
    matured: bool,
    subsidy: u64,
    fees: u64,
    total: u64,
    tx_count: usize,
    timestamp: u64,
}

struct AddressActivity {
    mined_blocks: Vec<MinedBlockResponse>,
    transactions: Vec<TxResponse>,
}

#[derive(Serialize)]
struct AddressResponse {
    address: String,
    balance: serde_json::Value,
    mined_blocks: Vec<MinedBlockResponse>,
    transactions: Vec<TxResponse>,
}

#[derive(Serialize)]
struct AccountResponse {
    address: String,
    confirmed: u64,
    available: u64,
    unspendable: u64,
    pending_incoming: u64,
    pending_outgoing: u64,
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
            listen_addrs: vec![
                DEFAULT_LISTEN_ADDR
                    .parse()
                    .expect("default listen address must be valid"),
            ],
            rpc_addr: DEFAULT_RPC_ADDR
                .parse()
                .expect("default rpc address must be valid"),
            peers: vec![
                DEFAULT_BOOTSTRAP_PEER
                    .parse()
                    .expect("default bootstrap peer must be valid"),
            ],
            peers_file: Some(DEFAULT_PEERS_FILE.to_string()),
            gateway_url: None,
            public_addrs: Vec::new(),
            gateway_heartbeat: DEFAULT_GATEWAY_HEARTBEAT,
            shutdown_file: DEFAULT_SHUTDOWN_FILE.to_string(),
            max_peers: DEFAULT_MAX_PEERS,
            min_relay_fee: runtime::params::DEFAULT_MIN_RELAY_FEE,
            market_fee: runtime::params::DEFAULT_MARKET_FEE,
            low_fee_expiry: Duration::from_secs(runtime::params::LOW_FEE_EXPIRY_SECS),
            mempool_expiry: Duration::from_secs(runtime::params::MEMPOOL_EXPIRY_SECS),
            miner_address: Address([9; 20]),
            miner_secret_key: None,
            miner_min_fee_rate: None,
            mine: false,
            mine_interval: DEFAULT_MINING_INTERVAL,
            mine_attempts: 10_000,
        }
    }
}

impl Default for RunConfigFile {
    fn default() -> Self {
        let defaults = RunConfig::default();
        Self {
            db_path: defaults.db_path,
            listen_addr: OneOrMany::Many(
                defaults
                    .listen_addrs
                    .into_iter()
                    .map(|addr| addr.to_string())
                    .collect(),
            ),
            rpc_addr: defaults.rpc_addr.to_string(),
            peers: defaults
                .peers
                .into_iter()
                .map(|peer| peer.to_string())
                .collect(),
            peers_file: Some(DEFAULT_PEERS_FILE.to_string()),
            gateway_url: None,
            public_addr: None,
            gateway_heartbeat_secs: defaults.gateway_heartbeat.as_secs(),
            shutdown_file: defaults.shutdown_file,
            max_peers: defaults.max_peers,
            min_relay_fee: Some(defaults.min_relay_fee),
            market_fee: Some(defaults.market_fee),
            low_fee_expiry_secs: Some(defaults.low_fee_expiry.as_secs()),
            mempool_expiry_secs: Some(defaults.mempool_expiry.as_secs()),
            wallet: None,
            miner_address: None,
            miner_secret_key: None,
            miner_min_fee_rate: defaults.miner_min_fee_rate,
            mine: false,
            mine_interval_secs: defaults.mine_interval.as_secs(),
            mine_attempts: defaults.mine_attempts,
        }
    }
}

struct NodeService {
    node: Arc<Mutex<Node>>,
    config: RunConfig,
    listeners: Vec<TcpListener>,
    peers: Arc<Mutex<HashMap<SocketAddr, PeerState>>>,
    peer_connections: Arc<Mutex<HashMap<SocketAddr, PeerConnection>>>,
    inbound_connections: Arc<Mutex<HashMap<SocketAddr, PeerConnection>>>,
    log_counters: Arc<LogCounters>,
    mining_stats: Arc<MiningStats>,
    requires_peer_sync_before_mining: bool,
    last_mine: Instant,
    last_status: Instant,
    last_gateway_heartbeat: Instant,
    last_activity_log: Instant,
    last_activity: NodeActivity,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NodeActivity {
    Starting,
    WaitingForPeers,
    WaitingForTransactions,
    Mining,
    Syncing,
    ServingPeers,
}

impl NodeService {
    fn new(
        node: Arc<Mutex<Node>>,
        config: RunConfig,
        listeners: Vec<TcpListener>,
        log_counters: Arc<LogCounters>,
        mining_stats: Arc<MiningStats>,
    ) -> Self {
        let requires_peer_sync_before_mining =
            config.mine && (!config.peers.is_empty() || config.gateway_url.is_some());
        let peers = config
            .peers
            .iter()
            .copied()
            .map(|peer| (peer, PeerState::new(peer)))
            .collect();
        let last_gateway_heartbeat = Instant::now()
            .checked_sub(config.gateway_heartbeat)
            .unwrap_or_else(Instant::now);
        Self {
            node,
            config,
            listeners,
            peers: Arc::new(Mutex::new(peers)),
            peer_connections: Arc::new(Mutex::new(HashMap::new())),
            inbound_connections: Arc::new(Mutex::new(HashMap::new())),
            log_counters,
            mining_stats,
            requires_peer_sync_before_mining,
            last_mine: Instant::now(),
            last_status: Instant::now(),
            last_gateway_heartbeat,
            last_activity_log: Instant::now()
                .checked_sub(ACTIVITY_LOG_INTERVAL)
                .unwrap_or_else(Instant::now),
            last_activity: NodeActivity::Starting,
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
            let secret_key = self.config.miner_secret_key.as_ref().ok_or_else(|| {
                "mining requires --wallet or --miner-secret-key so the miner identity is explicit"
                    .to_string()
            })?;
            let public_key = derive_public_key(&secret_key);
            let derived_address = address_from_public_key(&public_key);
            if derived_address != self.config.miner_address {
                return Err(format!(
                    "miner secret key does not match miner address {}",
                    address_to_string(&self.config.miner_address)
                ));
            }
        }

        self.refresh_gateway_peers(true);

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
            println!("preflight peer {peer} connecting");
            let result = PeerConnection::connect(peer).and_then(|mut connection| {
                println!("preflight peer {peer} connected; polling tip and sync state");
                let poll = poll_peer_connection(
                    &mut connection,
                    &self.node,
                    &self.config.public_addrs,
                    peer_state_sync_window(&self.peers, peer),
                )?;
                let infos = request_peers_connection(&mut connection).unwrap_or_else(|error| {
                    eprintln!("preflight peer {peer} discovery failed: {error}");
                    Vec::new()
                });
                if !infos.is_empty() {
                    println!("preflight peer {peer} discovered peers={}", infos.len());
                    if self.add_peer_infos(infos) {
                        let _ = self.save_peers();
                    }
                }
                if let Ok(accepted) = sync_mempool_connection(&mut connection, &self.node) {
                    if accepted > 0 {
                        println!("preflight mempool synced:: |peer::{peer}|txs::{accepted}|");
                    }
                }
                Ok(poll)
            });
            match result {
                Ok(PeerPoll::Idle { remote_tip }) => {
                    println!(
                        "preflight peer {peer} ok:: |remote_tip::{}|state::idle|",
                        remote_tip.0
                    );
                    if let Ok(mut peers) = self.peers.lock() {
                        if let Some(state) = peers.get_mut(&peer) {
                            state.mark_ok(Some(remote_tip));
                        }
                    }
                }
                Ok(PeerPoll::Synced {
                    remote_tip,
                    synced_blocks,
                }) => {
                    println!(
                        "preflight peer {peer} synced:: |remote_tip::{}|blocks::{}|",
                        remote_tip.0, synced_blocks
                    );
                    if let Ok(mut peers) = self.peers.lock() {
                        if let Some(state) = peers.get_mut(&peer) {
                            state.mark_synced(remote_tip, synced_blocks);
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
        self.save_peers()?;

        let node = self
            .node
            .lock()
            .map_err(|_| "node state lock poisoned".to_string())?;
        println!(
            "preflight ok |height::{}|tip::{}|difficulty::{}|mempool::{}|mining::{}",
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
            if self.last_gateway_heartbeat.elapsed() >= self.config.gateway_heartbeat {
                self.refresh_gateway_peers(false);
            }

            self.log_activity()?;

            if self.config.mine && self.last_mine.elapsed() >= self.config.mine_interval {
                if let Some(reason) = self.mining_wait_reason()? {
                    println!("mining waiting:: |reason::{reason}|");
                } else {
                    self.set_activity(NodeActivity::Mining)?;
                    let block = mine_once_unlocked(&self.node, &self.config, &self.mining_stats)?;
                    if let Some(block) = block {
                        let height = block.height().0;
                        let hash = short_hash(Some(block.hash()));
                        let tx_count = block.transactions.len();
                        let report = self.broadcast(NetworkMessage::Block(block));
                        println!(
                            "broadcast block:: |height::{}|hash::{}|txs::{}|peers::{}|sent::{}|failed::{}|",
                            height, hash, tx_count, report.attempted, report.sent, report.failed
                        );
                    }
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
                let outbound_count = self
                    .peer_connections
                    .lock()
                    .map_err(|_| "peer connection lock poisoned".to_string())?
                    .len();
                let inbound_count = self
                    .inbound_connections
                    .lock()
                    .map_err(|_| "inbound connection lock poisoned".to_string())?
                    .len();
                println!(
                    "status: |height::{}|tip::{}|difficulty::{}|known_peers::{}|outbound_peers::{}|inbound_peers::{}|mining::{}|hashrate_hps::{}|accepted_tx::{}|broadcast_tx::{}|",
                    node.tip_height().unwrap_or(Height(0)).0,
                    short_hash(node.tip_hash()),
                    format_difficulty(node.next_difficulty()),
                    peer_count,
                    outbound_count,
                    inbound_count,
                    self.config.mine,
                    self.mining_stats.last_hashrate_hps.load(Ordering::Relaxed),
                    self.log_counters.accepted_tx_total.load(Ordering::Relaxed),
                    self.log_counters.broadcast_tx_total.load(Ordering::Relaxed)
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

    fn log_activity(&mut self) -> Result<(), String> {
        let (mempool_len, mining, pending_sync) = {
            let node = self
                .node
                .lock()
                .map_err(|_| "node state lock poisoned".to_string())?;
            (
                node.mempool.len(),
                self.config.mine,
                node.has_pending_sync_work(),
            )
        };
        let peer_count = self
            .peers
            .lock()
            .map_err(|_| "peer state lock poisoned".to_string())?
            .len();

        let activity = if peer_count == 0 && self.config.gateway_url.is_some() {
            NodeActivity::WaitingForPeers
        } else if pending_sync
            || self.needs_peer_handshake_before_mining()?
            || self.peer_ahead_of_local_tip()?
        {
            NodeActivity::Syncing
        } else if mining && mempool_len == 0 {
            NodeActivity::WaitingForTransactions
        } else if mining {
            NodeActivity::Mining
        } else if peer_count > 0 {
            NodeActivity::Syncing
        } else {
            NodeActivity::ServingPeers
        };

        if activity != self.last_activity
            || self.last_activity_log.elapsed() >= ACTIVITY_LOG_INTERVAL
        {
            self.last_activity = activity;
            self.last_activity_log = Instant::now();
        }

        Ok(())
    }

    fn mining_wait_reason(&self) -> Result<Option<&'static str>, String> {
        let pending_sync = self
            .node
            .lock()
            .map_err(|_| "node state lock poisoned".to_string())?
            .has_pending_sync_work();
        if pending_sync {
            return Ok(Some("sync_pending"));
        }
        if self.needs_peer_handshake_before_mining()? {
            return Ok(Some("handshake_pending"));
        }
        if self.peer_ahead_of_local_tip()? {
            return Ok(Some("peer_ahead"));
        }
        Ok(None)
    }

    fn needs_peer_handshake_before_mining(&self) -> Result<bool, String> {
        if !self.requires_peer_sync_before_mining {
            return Ok(false);
        }
        let peers = self
            .peers
            .lock()
            .map_err(|_| "peer state lock poisoned".to_string())?;
        Ok(!peers.is_empty() && !peers.values().any(|peer| peer.last_tip.is_some()))
    }

    fn peer_ahead_of_local_tip(&self) -> Result<bool, String> {
        let local_height = self
            .node
            .lock()
            .map_err(|_| "node state lock poisoned".to_string())?
            .tip_height()
            .unwrap_or(Height(0))
            .0;
        let peers = self
            .peers
            .lock()
            .map_err(|_| "peer state lock poisoned".to_string())?;
        Ok(peers
            .values()
            .filter_map(|peer| peer.last_tip)
            .any(|height| height.0 > local_height))
    }

    fn set_activity(&mut self, activity: NodeActivity) -> Result<(), String> {
        if activity == self.last_activity
            && self.last_activity_log.elapsed() < ACTIVITY_LOG_INTERVAL
        {
            return Ok(());
        }
        self.last_activity = activity;
        self.last_activity_log = Instant::now();
        Ok(())
    }

    fn accept_p2p(&mut self) -> Result<(), String> {
        loop {
            let mut accepted = false;
            for index in 0..self.listeners.len() {
                match self.listeners[index].accept() {
                    Ok((stream, peer)) => {
                        accepted = true;
                        self.set_activity(NodeActivity::ServingPeers)?;
                        println!("p2p inbound:: |peer::{}|event::accepted|", peer);
                        let node = self.node.clone();
                        let peers = self.peers.clone();
                        let inbound_connections = self.inbound_connections.clone();
                        let public_addrs = self.config.public_addrs.clone();
                        let listen_addrs = self.config.listen_addrs.clone();
                        let max_peers = self.config.max_peers;
                        let peers_file = self.config.peers_file.clone();
                        if let Ok(mut inbound) = inbound_connections.lock() {
                            match stream.try_clone() {
                                Ok(writer) => match PeerConnection::from_stream(peer, writer) {
                                    Ok(connection) => {
                                        inbound.insert(peer, connection);
                                    }
                                    Err(error) => {
                                        eprintln!(
                                            "p2p inbound {peer} writer setup failed: {error}"
                                        );
                                    }
                                },
                                Err(error) => {
                                    eprintln!("p2p inbound {peer} clone failed: {error}");
                                }
                            }
                        }
                        thread::Builder::new()
                            .name(format!("paqus-p2p-{peer}"))
                            .spawn(move || {
                                if let Err(error) = Self::handle_p2p_stream_task(
                                    stream,
                                    peer,
                                    node,
                                    peers,
                                    public_addrs,
                                    listen_addrs,
                                    max_peers,
                                    peers_file,
                                ) {
                                    eprintln!("p2p inbound {peer} failed: {error}");
                                }
                                if let Ok(mut inbound) = inbound_connections.lock() {
                                    inbound.remove(&peer);
                                }
                            })
                            .map_err(|error| format!("failed to spawn p2p handler: {error}"))?;
                    }
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
                    Err(error) => return Err(format!("failed to accept peer: {error}")),
                }
            }
            if !accepted {
                return Ok(());
            }
        }
    }

    fn handle_p2p_stream_task(
        mut stream: TcpStream,
        peer: SocketAddr,
        node: Arc<Mutex<Node>>,
        peers: Arc<Mutex<HashMap<SocketAddr, PeerState>>>,
        public_addrs: Vec<SocketAddr>,
        listen_addrs: Vec<SocketAddr>,
        max_peers: usize,
        peers_file: Option<String>,
    ) -> Result<(), String> {
        configure_stream(&stream, PERSISTENT_PEER_TIMEOUT)?;
        loop {
            match read_message(&mut stream) {
                Ok(envelope) => {
                    let response = match envelope.message {
                        NetworkMessage::GetPeers => Some(NetworkMessage::Peers(
                            Self::peer_infos_from(&peers, &public_addrs),
                        )),
                        NetworkMessage::Peers(peer_infos) => {
                            if Self::add_peer_infos_to(
                                &peers,
                                peer_infos,
                                &public_addrs,
                                &listen_addrs,
                                max_peers,
                            ) {
                                let _ = Self::save_peers_from(&peers_file, &peers);
                            }
                            None
                        }
                        message => {
                            let inbound_log = inbound_message_log(&message, peer);
                            let mut node = node
                                .lock()
                                .map_err(|_| "node state lock poisoned".to_string())?;
                            let response = handle_message(&mut node, message).map_err(|error| {
                                format!("failed to handle message from {peer}: {error}")
                            })?;
                            if let Some(log) = inbound_log {
                                println!("{log}");
                            }
                            response
                        }
                    };
                    if let Some(response) = response {
                        write_message(&mut stream, &response.to_envelope())
                            .map_err(|error| format!("failed to respond to {peer}: {error}"))?;
                    }
                }
                Err(error) if is_peer_stream_closed(&error) => {
                    break;
                }
                Err(error) => {
                    eprintln!("peer {peer} sent invalid message: {error}");
                    break;
                }
            }
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

        if due_peers.len() > 1 {
            let sync_window = max_peer_sync_window(&self.peers, &due_peers);
            match sync_from_peers_parallel(
                due_peers.clone(),
                &self.node,
                &self.config.public_addrs,
                sync_window,
            ) {
                Ok(report) if report.applied_blocks > 0 => {
                    if let Ok(mut peers) = self.peers.lock() {
                        for peer in &report.used_peer_addrs {
                            if let Some(state) = peers.get_mut(peer) {
                                state.mark_synced(report.remote_tip, report.applied_blocks);
                            }
                        }
                        for peer in &report.failed_peer_addrs {
                            if let Some(state) = peers.get_mut(peer) {
                                state.mark_failed();
                            }
                        }
                    }
                    println!(
                        "parallel sync complete:: |blocks::{}|peers::{}|remote_tip::{}|",
                        report.applied_blocks, report.used_peers, report.remote_tip.0
                    );
                    return;
                }
                Ok(_) => {}
                Err(error) => {
                    eprintln!("parallel sync failed; falling back to peer polling: {error}")
                }
            }
        }

        for peer in due_peers {
            let result = self.poll_persistent_peer(peer);
            match result {
                Ok(PeerPoll::Idle { remote_tip }) => {
                    if let Ok(mut peers) = self.peers.lock() {
                        if let Some(state) = peers.get_mut(&peer) {
                            state.mark_ok(Some(remote_tip));
                        }
                    }
                    let infos = match self.peer_connections.lock() {
                        Ok(mut connections) => connections
                            .get_mut(&peer)
                            .and_then(|connection| request_peers_connection(connection).ok()),
                        Err(_) => {
                            eprintln!("peer connection lock poisoned");
                            None
                        }
                    };
                    if let Some(infos) = infos {
                        if self.add_peer_infos(infos) {
                            let _ = self.save_peers();
                        }
                    }
                    self.sync_mempool_from_peer(peer);
                }
                Ok(PeerPoll::Synced {
                    remote_tip,
                    synced_blocks,
                }) => {
                    if let Ok(mut peers) = self.peers.lock() {
                        if let Some(state) = peers.get_mut(&peer) {
                            state.mark_synced(remote_tip, synced_blocks);
                        }
                    }
                    let infos = match self.peer_connections.lock() {
                        Ok(mut connections) => connections
                            .get_mut(&peer)
                            .and_then(|connection| request_peers_connection(connection).ok()),
                        Err(_) => {
                            eprintln!("peer connection lock poisoned");
                            None
                        }
                    };
                    if let Some(infos) = infos {
                        if self.add_peer_infos(infos) {
                            let _ = self.save_peers();
                        }
                    }
                    self.sync_mempool_from_peer(peer);
                }
                Err(error) => {
                    let mut dropped = false;
                    if let Ok(mut peers) = self.peers.lock() {
                        if let Some(state) = peers.get_mut(&peer) {
                            state.mark_failed();
                            if state.failures >= MAX_PEER_FAILURES {
                                peers.remove(&peer);
                                dropped = true;
                            }
                        }
                    }
                    if dropped {
                        if let Ok(mut connections) = self.peer_connections.lock() {
                            connections.remove(&peer);
                        }
                        let _ = self.save_peers();
                        eprintln!(
                            "peer {peer} sync failed {MAX_PEER_FAILURES} times; dropped: {error}"
                        );
                    } else {
                        if let Ok(mut connections) = self.peer_connections.lock() {
                            connections.remove(&peer);
                        }
                        eprintln!("peer {peer} sync failed: {error}");
                    }
                }
            }
        }
    }

    fn poll_persistent_peer(&mut self, peer: SocketAddr) -> Result<PeerPoll, String> {
        let sync_window = peer_state_sync_window(&self.peers, peer);
        let mut connections = self
            .peer_connections
            .lock()
            .map_err(|_| "peer connection lock poisoned".to_string())?;
        if let Entry::Vacant(entry) = connections.entry(peer) {
            let connection = PeerConnection::connect(peer)?;
            println!("p2p outbound:: |peer::{peer}|event::connected|");
            entry.insert(connection);
        }
        let connection = connections
            .get_mut(&peer)
            .ok_or_else(|| format!("missing peer connection for {peer}"))?;
        poll_peer_connection(
            connection,
            &self.node,
            &self.config.public_addrs,
            sync_window,
        )
    }

    fn sync_mempool_from_peer(&mut self, peer: SocketAddr) {
        let result = match self.peer_connections.lock() {
            Ok(mut connections) => connections
                .get_mut(&peer)
                .ok_or_else(|| format!("missing peer connection for {peer}"))
                .and_then(|connection| sync_mempool_connection(connection, &self.node)),
            Err(_) => {
                eprintln!("peer connection lock poisoned");
                return;
            }
        };
        match result {
            Ok(accepted) if accepted > 0 => {
                println!("mempool synced:: |peer::{peer}|txs::{accepted}|");
            }
            Ok(_) => {}
            Err(error) => eprintln!("mempool sync from {peer} failed: {error}"),
        }
    }

    fn add_peer_infos(&mut self, peers: Vec<PeerInfo>) -> bool {
        Self::add_peer_infos_to(
            &self.peers,
            peers,
            &self.config.public_addrs,
            &self.config.listen_addrs,
            self.config.max_peers,
        )
    }

    fn add_peer_infos_to(
        peers_state: &Arc<Mutex<HashMap<SocketAddr, PeerState>>>,
        peers: Vec<PeerInfo>,
        public_addrs: &[SocketAddr],
        listen_addrs: &[SocketAddr],
        max_peers: usize,
    ) -> bool {
        let Ok(mut current) = peers_state.lock() else {
            eprintln!("peer state lock poisoned");
            return false;
        };
        let known = current.keys().copied().collect::<HashSet<_>>();
        let mut changed = false;
        for info in peers {
            if current.len() >= max_peers {
                break;
            }
            let Ok(addr) = info.address.parse::<SocketAddr>() else {
                continue;
            };
            if public_addrs.contains(&addr) || listen_addrs.contains(&addr) {
                continue;
            }
            if known.contains(&addr) {
                continue;
            }
            if let Entry::Vacant(entry) = current.entry(addr) {
                entry.insert(PeerState::new(addr));
                changed = true;
            }
        }
        changed
    }

    fn refresh_gateway_peers(&mut self, register: bool) {
        let Some(gateway_url) = self.config.gateway_url.clone() else {
            return;
        };

        let (best_height, tip_hash) = match self.node.lock() {
            Ok(node) => (
                node.tip_height().map(|height| height.0),
                node.tip_hash().map(|hash| hex::encode(hash.0)),
            ),
            Err(_) => {
                eprintln!("node state lock poisoned");
                return;
            }
        };

        if self.config.public_addrs.is_empty() {
            if register {
                eprintln!("gateway configured without --public-addr; querying peers only");
            }
        } else {
            for public_addr in &self.config.public_addrs {
                let result = if register {
                    register_peer(&gateway_url, *public_addr, best_height, tip_hash.clone())
                } else {
                    heartbeat_peer(&gateway_url, *public_addr, best_height, tip_hash.clone())
                };
                if let Err(error) = result {
                    eprintln!("gateway update failed for {public_addr}: {error}");
                }
            }
        }

        let available = match self.peers.lock() {
            Ok(peers) => self.config.max_peers.saturating_sub(peers.len()),
            Err(_) => {
                eprintln!("peer state lock poisoned");
                return;
            }
        };
        if available > 0 {
            match request_gateway_peers(
                &gateway_url,
                available.min(32),
                self.config
                    .public_addrs
                    .first()
                    .or_else(|| self.config.listen_addrs.first())
                    .copied(),
            ) {
                Ok(peers) => {
                    if !peers.is_empty() {
                        println!("gateway discovered peer::{}|", peers.len());
                        if self.add_peer_infos(peers) {
                            let _ = self.save_peers();
                        }
                    }
                }
                Err(error) => eprintln!("gateway peer query failed: {error}"),
            }
        }

        self.last_gateway_heartbeat = Instant::now();
    }

    fn save_peers(&self) -> Result<(), String> {
        Self::save_peers_from(&self.config.peers_file, &self.peers)
    }

    fn save_peers_from(
        peers_file: &Option<String>,
        peers_state: &Arc<Mutex<HashMap<SocketAddr, PeerState>>>,
    ) -> Result<(), String> {
        let Some(path) = peers_file else {
            return Ok(());
        };
        if let Some(parent) = std::path::Path::new(path).parent() {
            fs::create_dir_all(parent)
                .map_err(|error| format!("failed to create peers file parent: {error}"))?;
        }
        let peers = peers_state
            .lock()
            .map_err(|_| "peer state lock poisoned".to_string())?;
        let mut peers = peers.keys().copied().collect::<Vec<_>>();
        peers.sort();
        save_peers_file(path, peers)
    }

    fn peer_infos_from(
        peers_state: &Arc<Mutex<HashMap<SocketAddr, PeerState>>>,
        public_addrs: &[SocketAddr],
    ) -> Vec<PeerInfo> {
        let Ok(peers) = peers_state.lock() else {
            eprintln!("peer state lock poisoned");
            return Vec::new();
        };
        let mut infos = peers
            .keys()
            .map(|addr| PeerInfo {
                address: addr.to_string(),
            })
            .collect::<Vec<_>>();
        for public_addr in public_addrs {
            infos.push(PeerInfo {
                address: public_addr.to_string(),
            });
        }
        infos.sort_by(|left, right| left.address.cmp(&right.address));
        infos.dedup_by(|left, right| left.address == right.address);
        infos
    }

    fn broadcast(&mut self, message: NetworkMessage) -> BroadcastReport {
        let peers = match self.peers.lock() {
            Ok(peers) => peers.keys().copied().collect::<Vec<_>>(),
            Err(_) => {
                eprintln!("peer state lock poisoned");
                return BroadcastReport::default();
            }
        };
        let mut report = BroadcastReport {
            attempted: peers.len(),
            sent: 0,
            failed: 0,
        };
        let known_peers = peers.iter().copied().collect::<HashSet<_>>();
        for peer in peers {
            let result = {
                let mut connections = match self.peer_connections.lock() {
                    Ok(connections) => connections,
                    Err(_) => {
                        report.failed += 1;
                        eprintln!("peer connection lock poisoned");
                        continue;
                    }
                };
                if let Entry::Vacant(entry) = connections.entry(peer) {
                    match PeerConnection::connect(peer) {
                        Ok(connection) => {
                            println!("p2p outbound:: |peer::{peer}|event::connected|");
                            entry.insert(connection);
                        }
                        Err(error) => {
                            report.failed += 1;
                            eprintln!("broadcast to {peer} failed: {error}");
                            continue;
                        }
                    }
                }
                connections
                    .get_mut(&peer)
                    .ok_or_else(|| format!("missing peer connection for {peer}"))
                    .and_then(|connection| announce_or_send(connection, message.clone()))
            };
            match result {
                Ok(()) => report.sent += 1,
                Err(error) => {
                    report.failed += 1;
                    if let Ok(mut connections) = self.peer_connections.lock() {
                        connections.remove(&peer);
                    }
                    eprintln!("broadcast to {peer} failed: {error}");
                }
            }
        }
        let inbound_peers = match self.inbound_connections.lock() {
            Ok(connections) => connections.keys().copied().collect::<Vec<_>>(),
            Err(_) => {
                eprintln!("inbound connection lock poisoned");
                Vec::new()
            }
        };
        for peer in inbound_peers {
            if known_peers.contains(&peer) {
                continue;
            }
            report.attempted += 1;
            let result = {
                let mut connections = match self.inbound_connections.lock() {
                    Ok(connections) => connections,
                    Err(_) => {
                        report.failed += 1;
                        eprintln!("inbound connection lock poisoned");
                        continue;
                    }
                };
                connections
                    .get_mut(&peer)
                    .ok_or_else(|| format!("missing inbound connection for {peer}"))
                    .and_then(|connection| connection.send(message.clone()))
            };
            match result {
                Ok(()) => report.sent += 1,
                Err(error) => {
                    report.failed += 1;
                    if let Ok(mut connections) = self.inbound_connections.lock() {
                        connections.remove(&peer);
                    }
                    eprintln!("broadcast to inbound {peer} failed: {error}");
                }
            }
        }
        report
    }
}

fn inbound_message_log(message: &NetworkMessage, peer: SocketAddr) -> Option<String> {
    match message {
        NetworkMessage::Block(block) => Some(format!(
            "received block height {} from {} |hash::{}|txs::{}|",
            block.height().0,
            peer,
            short_hash(Some(block.hash())),
            block.transactions.len()
        )),
        NetworkMessage::Transaction(transaction) => Some(format!(
            "received tx:: |from::{}|hash::{}|amount::{}|fee::{}|nonce::{}|",
            peer,
            short_hash(Some(transaction.hash())),
            transaction.transaction.amount.0,
            transaction.transaction.fee.0,
            transaction.transaction.nonce.0
        )),
        _ => None,
    }
}

fn announce_or_send(
    connection: &mut PeerConnection,
    message: NetworkMessage,
) -> Result<(), String> {
    match message {
        NetworkMessage::Block(block) => {
            let hash = block.hash();
            match connection.request(NetworkMessage::Inventory(vec![InventoryItem::Block(hash)])) {
                Ok(NetworkMessage::GetData(items))
                    if items.contains(&InventoryItem::Block(hash)) =>
                {
                    connection.send(NetworkMessage::Block(block))
                }
                Ok(_) => Ok(()),
                Err(error) => Err(error),
            }
        }
        NetworkMessage::Transaction(transaction) => {
            let hash = transaction.hash();
            match connection.request(NetworkMessage::Inventory(vec![InventoryItem::Transaction(
                hash,
            )])) {
                Ok(NetworkMessage::GetData(items))
                    if items.contains(&InventoryItem::Transaction(hash)) =>
                {
                    connection.send(NetworkMessage::Transaction(transaction))
                }
                Ok(_) => Ok(()),
                Err(error) => Err(error),
            }
        }
        other => connection.send(other),
    }
}

fn is_peer_stream_closed(error: &NetworkError) -> bool {
    match error {
        NetworkError::Io(error) => matches!(
            error.kind(),
            io::ErrorKind::UnexpectedEof
                | io::ErrorKind::WouldBlock
                | io::ErrorKind::TimedOut
                | io::ErrorKind::ConnectionReset
                | io::ErrorKind::BrokenPipe
        ),
        _ => false,
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
        .route("/stats", get(rpc_stats))
        .route("/chain/stats", get(rpc_stats))
        .route("/peers", get(rpc_peers))
        .route("/balance/{address}", get(rpc_balance))
        .route("/blocks/latest", get(rpc_latest_blocks))
        .route("/blocks/{height}", get(rpc_block_by_height))
        .route("/blocks/hash/{hash}", get(rpc_block_by_hash))
        .route("/tx/{hash}", get(rpc_tx))
        .route("/address/{address}", get(rpc_address))
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
    match (
        state.node.lock(),
        state.peers.lock(),
        state.peer_connections.lock(),
        state.inbound_connections.lock(),
    ) {
        (Ok(node), Ok(peers), Ok(outbound), Ok(inbound)) => Json(StatusResponse {
            chain: CHAIN_NAME,
            stage: PROTOCOL_STAGE,
            protocol_version: PROTOCOL_VERSION,
            height: node.tip_height().unwrap_or(Height(0)).0,
            tip_hash: format_hash(node.tip_hash()),
            peers: peers.len(),
            known_peers: peers.len(),
            outbound_peers: outbound.len(),
            inbound_peers: inbound.len(),
            mining: state.mining,
            hashrate_hps: state.mining_stats.last_hashrate_hps.load(Ordering::Relaxed),
            last_mine_attempts: state.mining_stats.last_attempts.load(Ordering::Relaxed),
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
        confirmation_depth: CONFIRMATION_DEPTH,
        finality_depth: FINALITY_DEPTH,
        block_reward_maturity: BLOCK_REWARD_MATURITY,
        difficulty_start: DIFFICULTY_START,
    })
}

async fn rpc_stats(State(state): State<RpcState>) -> impl IntoResponse {
    match state.node.lock() {
        Ok(node) => match chain_stats(&node) {
            Ok(stats) => Json(stats).into_response(),
            Err(error) => rpc_error(StatusCode::INTERNAL_SERVER_ERROR, error),
        },
        Err(_) => rpc_error(StatusCode::INTERNAL_SERVER_ERROR, "state_lock_failed"),
    }
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
    let address = match parse_address_string(&address) {
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
                    Ok(Some(block)) => blocks.push(block_response(&node, &block, None)),
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
            Ok(Some(block)) => Json(block_response(&node, &block, None)).into_response(),
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
    let block_hash = BlockHash::from(hash);
    match state.node.lock() {
        Ok(node) => match node.storage.load_block_by_hash(&block_hash) {
            Ok(Some(block)) => Json(block_response(&node, &block, None)).into_response(),
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
    let address = match parse_address_string(&address) {
        Ok(address) => address,
        Err(error) => return rpc_error(StatusCode::BAD_REQUEST, error),
    };
    match state.node.lock() {
        Ok(node) => match address_activity(&node, &address) {
            Ok(activity) => {
                let balance: serde_json::Value = serde_json::from_str(&balance_json(
                    &node, &address,
                ))
                .unwrap_or_else(|_| serde_json::json!({ "error": "balance_encode_failed" }));
                Json(AddressResponse {
                    address: address_to_string(&address),
                    balance,
                    mined_blocks: activity.mined_blocks,
                    transactions: activity.transactions,
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
    state
        .log_counters
        .accepted_tx_total
        .fetch_add(1, Ordering::Relaxed);
    let _report = broadcast_to_peers(
        &state.peers,
        &state.peer_connections,
        &state.inbound_connections,
        NetworkMessage::Transaction(transaction),
    );
    state
        .log_counters
        .broadcast_tx_total
        .fetch_add(1, Ordering::Relaxed);
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

fn block_response(node: &Node, block: &Block, status: Option<&'static str>) -> BlockResponse {
    let block_hash = block.hash();
    let tip_height = node.tip_height().unwrap_or(Height(0)).0;
    let height = block.height().0;
    let now = unix_timestamp().unwrap_or(block.timestamp());
    let previous_timestamp = height
        .checked_sub(1)
        .and_then(|previous_height| {
            node.storage
                .load_block_by_height(Height(previous_height))
                .ok()
        })
        .flatten()
        .map(|previous_block| previous_block.timestamp());
    BlockResponse {
        height,
        hash: hex::encode(block_hash.0),
        short_hash: short_hash(Some(block_hash)),
        previous_hash: hex::encode(block.previous_hash().0),
        merkle_root: hex::encode(block.header.merkle_root.0),
        state_root: hex::encode(block.state_root().0),
        miner_address: address_to_string(&block.miner_address()),
        difficulty: block.difficulty(),
        timestamp: block.timestamp(),
        age_secs: now.saturating_sub(block.timestamp()),
        confirmations: tip_height.saturating_sub(height).saturating_add(1),
        block_time_secs: previous_timestamp
            .map(|timestamp| block.timestamp().saturating_sub(timestamp)),
        target_block_time_secs: BLOCK_TIME,
        block_time_delta_secs: previous_timestamp.map(|timestamp| {
            block.timestamp().saturating_sub(timestamp) as i64 - BLOCK_TIME as i64
        }),
        value_moved: block
            .transactions
            .iter()
            .map(|transaction| transaction.transaction.amount.0)
            .sum(),
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
                    Some(block_hash.into()),
                    status.unwrap_or("confirmed"),
                )
            })
            .collect(),
    }
}

fn tx_response(
    transaction: &SignedTransaction,
    block_height: Option<Height>,
    block_hash: Option<Hash>,
    status: &'static str,
) -> TxResponse {
    let now = unix_timestamp().unwrap_or(transaction.transaction.timestamp);
    TxResponse {
        hash: hex::encode(transaction.hash().0),
        from: address_to_string(&transaction.transaction.from),
        to: address_to_string(&transaction.transaction.to),
        amount: transaction.transaction.amount.0,
        fee: transaction.transaction.fee.0,
        nonce: transaction.transaction.nonce.0,
        timestamp: transaction.transaction.timestamp,
        age_secs: now.saturating_sub(transaction.transaction.timestamp),
        block_height: block_height.map(|height| height.0),
        block_hash: block_hash.map(|hash| hex::encode(hash.0)),
        status,
    }
}

fn find_transaction(node: &Node, hash: &Hash) -> Result<Option<TxResponse>, String> {
    for transaction in node.mempool.transactions() {
        if transaction.hash() == *hash {
            return Ok(Some(tx_response(transaction, None, None, "pending")));
        }
    }

    let hash = TransactionHash(hash.0);
    if let Some((location, transaction)) = node
        .storage
        .load_transaction(&hash)
        .map_err(|error| format!("failed to load indexed transaction: {error}"))?
    {
        return Ok(Some(tx_response(
            &transaction,
            Some(location.block_height),
            Some(location.block_hash.into()),
            "confirmed",
        )));
    }
    Ok(None)
}

fn address_activity(node: &Node, address: &Address) -> Result<AddressActivity, String> {
    let mut mined_blocks = Vec::new();
    let mut transactions = Vec::new();
    let tip = node.tip_height().unwrap_or(Height(0)).0;
    let mined_locations = node
        .storage
        .load_miner_block_locations(address)
        .map_err(|error| format!("failed to load miner block index: {error}"))?;
    for location in mined_locations {
        let height = location.block_height.0;
        let block = node
            .storage
            .load_block_by_height(location.block_height)
            .map_err(|error| format!("failed to load block: {error}"))?;
        let Some(block) = block else {
            continue;
        };
        if block.hash() != location.block_hash || block.miner_address() != *address {
            continue;
        }
        if let Some(coinbase) = block.coinbase.as_ref() {
            let maturity_height = height.saturating_add(BLOCK_REWARD_MATURITY as u64);
            mined_blocks.push(MinedBlockResponse {
                height,
                hash: hex::encode(block.hash().0),
                confirmations: tip.saturating_sub(height).saturating_add(1),
                maturity_height,
                matured: tip >= maturity_height,
                subsidy: coinbase.subsidy.0,
                fees: coinbase.fees.0,
                total: coinbase.total().0,
                tx_count: block.transaction_count(),
                timestamp: block.timestamp(),
            });
        }
    }

    let locations = node
        .storage
        .load_address_transaction_locations(address)
        .map_err(|error| format!("failed to load address transaction index: {error}"))?;
    for location in locations {
        if let Some((_, transaction)) = node
            .storage
            .load_transaction(&location.tx_hash)
            .map_err(|error| format!("failed to load indexed transaction: {error}"))?
        {
            transactions.push(tx_response(
                &transaction,
                Some(location.block_height),
                Some(location.block_hash.into()),
                "confirmed",
            ));
        }
    }

    for transaction in node.mempool.transactions_for_address(address) {
        transactions.push(tx_response(transaction, None, None, "pending"));
    }

    mined_blocks.reverse();
    transactions.reverse();
    Ok(AddressActivity {
        mined_blocks,
        transactions,
    })
}

fn chain_stats(node: &Node) -> Result<ChainStatsResponse, String> {
    let tip = node.tip_height().unwrap_or(Height(0)).0;
    let mut blocks = 0u64;
    let mut mined_supply = 0u64;
    let mut total_coinbase_rewards = 0u64;
    let mut total_fees_collected = 0u64;
    let mut total_transactions = 0u64;
    let mut total_transfer_volume = 0u64;
    let mut total_transaction_fees = 0u64;
    let mut previous_timestamp = None;
    let mut total_block_time_secs = 0u64;
    let mut block_time_samples = 0u64;

    for height in 0..=tip {
        let block = node
            .storage
            .load_block_by_height(Height(height))
            .map_err(|error| format!("failed to load block: {error}"))?;
        let Some(block) = block else {
            continue;
        };
        blocks = blocks.saturating_add(1);
        if let Some(previous_timestamp) = previous_timestamp {
            total_block_time_secs = total_block_time_secs
                .saturating_add(block.timestamp().saturating_sub(previous_timestamp));
            block_time_samples = block_time_samples.saturating_add(1);
        }
        previous_timestamp = Some(block.timestamp());
        if let Some(coinbase) = block.coinbase.as_ref() {
            mined_supply = mined_supply.saturating_add(coinbase.subsidy.0 as u64);
            total_fees_collected = total_fees_collected.saturating_add(coinbase.fees.0 as u64);
            total_coinbase_rewards =
                total_coinbase_rewards.saturating_add(coinbase.total().0 as u64);
        }
        for transaction in &block.transactions {
            total_transactions = total_transactions.saturating_add(1);
            total_transfer_volume =
                total_transfer_volume.saturating_add(transaction.transaction.amount.0 as u64);
            total_transaction_fees =
                total_transaction_fees.saturating_add(transaction.transaction.fee.0 as u64);
        }
    }

    let pending_transactions = node.mempool.len() as u64;
    let average_transfer_amount = if total_transactions == 0 {
        0
    } else {
        total_transfer_volume / total_transactions
    };
    let current_supply = (GENESIS_PREMINE as u64).saturating_add(mined_supply);
    let average_block_time_secs = if block_time_samples == 0 {
        None
    } else {
        Some(total_block_time_secs / block_time_samples)
    };

    Ok(ChainStatsResponse {
        chain: CHAIN_NAME,
        coin: COIN_NAME,
        height: tip,
        blocks,
        average_block_time_secs,
        target_block_time_secs: BLOCK_TIME,
        genesis_premine: GENESIS_PREMINE as u64,
        mined_supply,
        current_supply,
        total_coinbase_rewards,
        total_fees_collected,
        total_transactions,
        pending_transactions,
        total_transfer_volume,
        total_transaction_fees,
        average_transfer_amount,
    })
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
    let mut node = open_node(&config.db_path, config.miner_address)?;
    node.mempool = runtime::mempool::Mempool::with_config(MempoolConfig {
        min_relay_fee: config.min_relay_fee,
        market_fee: config.market_fee,
        low_fee_ttl_secs: config.low_fee_expiry.as_secs(),
        transaction_ttl_secs: config.mempool_expiry.as_secs(),
        ..MempoolConfig::default()
    });
    if config.listen_addrs.is_empty() {
        return Err("at least one --listen address is required".to_string());
    }
    dedupe_socket_addrs(&mut config.listen_addrs);
    dedupe_socket_addrs(&mut config.public_addrs);
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
    let node = Arc::new(Mutex::new(node));
    let log_counters = Arc::new(LogCounters::default());
    let mining_stats = Arc::new(MiningStats::default());

    let mut service = NodeService::new(
        node.clone(),
        config,
        listeners,
        log_counters.clone(),
        mining_stats.clone(),
    );
    service.preflight()?;

    let (height, tip_hash, difficulty, dynamic_market_fee_rate) = {
        let node = node
            .lock()
            .map_err(|_| "node state lock poisoned".to_string())?;
        (
            node.tip_height().unwrap_or(Height(0)).0,
            short_hash(node.tip_hash()),
            format_difficulty(node.next_difficulty()),
            node.mempool.dynamic_market_fee_rate(),
        )
    };

    println!(
        "Paqus Node db::{}|p2p::{}|rpc::{}|height::{}|tip::{}|difficulty::{}|peers::{}|mining::{}|min_relay_fee_rate_per_kib::{}|base_market_fee_rate_per_kib::{}|dynamic_market_fee_rate_per_kib::{}|miner_min_fee_rate_per_kib::{}|low_fee_expiry::{}s|mempool_expiry::{}s",
        service.config.db_path,
        format_socket_addrs(&bound_addrs),
        service.config.rpc_addr,
        height,
        tip_hash,
        difficulty,
        service.config.peers.len(),
        service.config.mine,
        service.config.min_relay_fee,
        service.config.market_fee,
        dynamic_market_fee_rate,
        service
            .config
            .miner_min_fee_rate
            .map(|rate| rate.to_string())
            .unwrap_or_else(|| "dynamic".to_string()),
        service.config.low_fee_expiry.as_secs(),
        service.config.mempool_expiry.as_secs()
    );

    let rpc_state = RpcState {
        node,
        peers: service.peers.clone(),
        peer_connections: service.peer_connections.clone(),
        inbound_connections: service.inbound_connections.clone(),
        mining: service.config.mine,
        log_counters,
        mining_stats,
    };
    let _rpc_handle = start_rpc_server(rpc_state, service.config.rpc_addr)?;
    service.run()
}

fn peer_state_sync_window(
    peers: &Arc<Mutex<HashMap<SocketAddr, PeerState>>>,
    peer: SocketAddr,
) -> u64 {
    peers
        .lock()
        .ok()
        .and_then(|peers| peers.get(&peer).map(|state| state.sync_window))
        .unwrap_or(64)
}

fn max_peer_sync_window(
    peers: &Arc<Mutex<HashMap<SocketAddr, PeerState>>>,
    due_peers: &[SocketAddr],
) -> u64 {
    peers
        .lock()
        .ok()
        .map(|peers| {
            due_peers
                .iter()
                .filter_map(|peer| peers.get(peer).map(|state| state.sync_window))
                .max()
                .unwrap_or(64)
        })
        .unwrap_or(64)
}

fn warn_if_public_rpc(config: &RunConfig) {
    let ip = config.rpc_addr.ip();
    if ip.is_loopback() {
        return;
    }
    eprintln!(
        "warning: rpc is listening on {}; keep fullnode rpc internal and expose public traffic through paqus-gateway",
        config.rpc_addr
    );
}

fn print_core_startup_info() {
    println!(
        "core chain::{}|chain_id::{}|coin::{}|stage::{}|protocol::{}|storage::{}|magic::{}",
        CHAIN_NAME,
        CHAIN_ID,
        COIN_NAME,
        PROTOCOL_STAGE,
        PROTOCOL_VERSION,
        STORAGE_VERSION,
        hex::encode(NETWORK_MAGIC)
    );
    println!(
        "consensus: block_time::{}s|confirmation::{}|finality::{}|reward_maturity::{}|difficulty_start::{}|retarget_window::{}",
        BLOCK_TIME,
        CONFIRMATION_DEPTH,
        FINALITY_DEPTH,
        BLOCK_REWARD_MATURITY,
        DIFFICULTY_START,
        DIFFICULTY_ADJUSTMENT_INTERVAL
    );
}

fn parse_run_config(args: &[String]) -> Result<RunConfig, String> {
    let args = args
        .iter()
        .map(|arg| arg.trim().to_string())
        .collect::<Vec<_>>();
    let mut config = RunConfig::default();
    let config_path = config_path_arg(&args).unwrap_or(DEFAULT_CONFIG_FILE);
    if let Some(file_config) = load_run_config_file_if_exists(config_path)? {
        apply_run_config_file(&mut config, file_config)?;
    }
    let mut listen_overridden = false;
    let mut public_overridden = false;
    let mut index = 0;

    while index < args.len() {
        match args[index].as_str() {
            "--config" => {
                index += 1;
                args.get(index)
                    .ok_or_else(|| "missing value for --config".to_string())?;
            }
            "--db" | "--db-path" => {
                index += 1;
                config.db_path = args
                    .get(index)
                    .ok_or_else(|| "missing value for --db".to_string())?
                    .clone();
            }
            "--listen" => {
                index += 1;
                if !listen_overridden {
                    config.listen_addrs.clear();
                    listen_overridden = true;
                }
                config
                    .listen_addrs
                    .push(parse_socket(args.get(index), "--listen")?);
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
            "--gateway" | "--gateway-url" => {
                index += 1;
                config.gateway_url = Some(
                    args.get(index)
                        .ok_or_else(|| "missing value for --gateway".to_string())?
                        .clone(),
                );
            }
            "--public-addr" => {
                index += 1;
                if !public_overridden {
                    config.public_addrs.clear();
                    public_overridden = true;
                }
                config
                    .public_addrs
                    .push(parse_socket(args.get(index), "--public-addr")?);
            }
            "--gateway-heartbeat-secs" => {
                index += 1;
                let secs = args
                    .get(index)
                    .ok_or_else(|| "missing value for --gateway-heartbeat-secs".to_string())?
                    .parse::<u64>()
                    .map_err(|error| format!("invalid gateway heartbeat interval: {error}"))?;
                config.gateway_heartbeat = Duration::from_secs(secs.max(1));
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
            "--min-relay-fee" => {
                index += 1;
                config.min_relay_fee = args
                    .get(index)
                    .ok_or_else(|| "missing value for --min-relay-fee".to_string())?
                    .parse::<u64>()
                    .map_err(|error| format!("invalid min relay fee: {error}"))?
                    .max(runtime::params::MIN_RELAY_FEE_FLOOR);
            }
            "--market-fee" => {
                index += 1;
                config.market_fee = args
                    .get(index)
                    .ok_or_else(|| "missing value for --market-fee".to_string())?
                    .parse::<u64>()
                    .map_err(|error| format!("invalid market fee: {error}"))?;
            }
            "--low-fee-expiry-secs" => {
                index += 1;
                let secs = args
                    .get(index)
                    .ok_or_else(|| "missing value for --low-fee-expiry-secs".to_string())?
                    .parse::<u64>()
                    .map_err(|error| format!("invalid low fee expiry: {error}"))?;
                config.low_fee_expiry = Duration::from_secs(secs.max(1));
            }
            "--mempool-expiry-secs" => {
                index += 1;
                let secs = args
                    .get(index)
                    .ok_or_else(|| "missing value for --mempool-expiry-secs".to_string())?
                    .parse::<u64>()
                    .map_err(|error| format!("invalid mempool expiry: {error}"))?;
                config.mempool_expiry = Duration::from_secs(secs.max(1));
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
            "--miner-min-fee-rate" => {
                index += 1;
                config.miner_min_fee_rate = Some(
                    args.get(index)
                        .ok_or_else(|| "missing value for --miner-min-fee-rate".to_string())?
                        .parse::<u64>()
                        .map_err(|error| format!("invalid miner min fee rate: {error}"))?,
                );
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

    dedupe_socket_addrs(&mut config.listen_addrs);
    dedupe_socket_addrs(&mut config.public_addrs);
    normalize_mempool_policy(&mut config);
    Ok(config)
}

fn dedupe_socket_addrs(addrs: &mut Vec<SocketAddr>) {
    let mut seen = HashSet::new();
    addrs.retain(|addr| seen.insert(*addr));
}

fn format_socket_addrs(addrs: &[SocketAddr]) -> String {
    addrs
        .iter()
        .map(SocketAddr::to_string)
        .collect::<Vec<_>>()
        .join(",")
}

fn config_path_arg(args: &[String]) -> Option<&str> {
    args.windows(2)
        .find_map(|window| (window[0] == "--config").then_some(window[1].as_str()))
}

fn write_default_run_config(path: &str) -> Result<(), String> {
    if let Some(parent) = std::path::Path::new(path).parent() {
        fs::create_dir_all(parent)
            .map_err(|error| format!("failed to create config directory: {error}"))?;
    }
    let contents = serde_json::to_string_pretty(&RunConfigFile::default())
        .map_err(|error| format!("failed to encode default config: {error}"))?;
    fs::write(path, contents).map_err(|error| format!("failed to write config {path}: {error}"))
}

fn load_run_config_file_if_exists(path: &str) -> Result<Option<RunConfigFile>, String> {
    let contents = match fs::read_to_string(path) {
        Ok(contents) => contents,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(format!("failed to read config {path}: {error}")),
    };
    serde_json::from_str(&contents)
        .map(Some)
        .map_err(|error| format!("failed to parse config {path}: {error}"))
}

fn apply_run_config_file(config: &mut RunConfig, file: RunConfigFile) -> Result<(), String> {
    config.db_path = file.db_path;
    config.listen_addrs = file
        .listen_addr
        .into_vec()
        .into_iter()
        .map(|addr| {
            addr.parse()
                .map_err(|error| format!("invalid listen_addr `{addr}` in config: {error}"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    config.rpc_addr = file
        .rpc_addr
        .parse()
        .map_err(|error| format!("invalid rpc_addr in config: {error}"))?;
    config.peers = file
        .peers
        .into_iter()
        .map(|peer| {
            peer.parse()
                .map_err(|error| format!("invalid peer `{peer}` in config: {error}"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    config.peers_file = file.peers_file;
    config.gateway_url = file.gateway_url;
    config.public_addrs = file
        .public_addr
        .map(OneOrMany::into_vec)
        .unwrap_or_default()
        .into_iter()
        .map(|addr| {
            addr.parse()
                .map_err(|error| format!("invalid public_addr `{addr}` in config: {error}"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    config.gateway_heartbeat = Duration::from_secs(file.gateway_heartbeat_secs.max(1));
    config.shutdown_file = file.shutdown_file;
    config.max_peers = file.max_peers.max(1);
    config.min_relay_fee = file
        .min_relay_fee
        .unwrap_or(config.min_relay_fee)
        .max(runtime::params::MIN_RELAY_FEE_FLOOR);
    config.market_fee = file.market_fee.unwrap_or(config.market_fee);
    if let Some(secs) = file.low_fee_expiry_secs {
        config.low_fee_expiry = Duration::from_secs(secs.max(1));
    }
    if let Some(secs) = file.mempool_expiry_secs {
        config.mempool_expiry = Duration::from_secs(secs.max(1));
    }
    config.mine = file.mine;
    config.mine_interval = Duration::from_secs(file.mine_interval_secs);
    config.mine_attempts = file.mine_attempts;

    if let Some(wallet_path) = file.wallet {
        apply_wallet_file(config, Some(&wallet_path))?;
    }
    if let Some(miner_address) = file.miner_address {
        config.miner_address = parse_address(Some(&miner_address))?;
    }
    if let Some(secret_key) = file.miner_secret_key {
        config.miner_secret_key = Some(parse_secret_key(Some(&secret_key))?);
    }
    config.miner_min_fee_rate = file.miner_min_fee_rate;

    Ok(())
}

fn normalize_mempool_policy(config: &mut RunConfig) {
    config.min_relay_fee = config
        .min_relay_fee
        .max(runtime::params::MIN_RELAY_FEE_FLOOR);
    config.market_fee = config.market_fee.max(config.min_relay_fee);
    if config.low_fee_expiry > config.mempool_expiry {
        config.low_fee_expiry = config.mempool_expiry;
    }
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

fn parse_socket(value: Option<&String>, flag: &str) -> Result<SocketAddr, String> {
    value
        .ok_or_else(|| format!("missing value for {flag}"))?
        .parse()
        .map_err(|error| format!("invalid socket address for {flag}: {error}"))
}

fn mine_once_unlocked(
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
        let mempool_len = node.mempool.len();
        let miner_min_fee_rate = config
            .miner_min_fee_rate
            .unwrap_or_else(|| node.mempool.dynamic_market_fee_rate());
        println!(
            "pow:: |algo::argon2id|difficulty_bits::{}|target::{}|",
            difficulty,
            pow_target_description(difficulty)
        );
        println!(
            "mempool:: |txs::{}|miner_min_fee_rate_per_kib::{}|",
            mempool_len, miner_min_fee_rate
        );
        let candidate = prepare_candidate_block(
            &node.mempool,
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
                max_attempts: config.mine_attempts,
                transaction_limit: MAX_BLOCK_TXS,
                min_fee_rate: miner_min_fee_rate,
            },
        )
    };

    let parent_hash = BlockHash::from(candidate.previous_hash().as_hash());
    let started = Instant::now();
    let mined = mine_prepared_block(candidate, &consensus, mining_config)
        .map_err(|error| format!("mining failed: {error}"))?;
    let elapsed = started.elapsed();
    let Some(result) = mined else {
        update_mining_stats(mining_stats, mining_config.max_attempts, elapsed);
        let node = node_state
            .lock()
            .map_err(|_| "node state lock poisoned".to_string())?;
        if node.mempool.is_empty() {
            println!("mining waiting:: |reason::empty_mempool|");
        } else {
            eprintln!("mining skipped: exhausted attempts");
        }
        return Ok(None);
    };
    update_mining_stats(mining_stats, result.attempts, elapsed);

    let mut node = node_state
        .lock()
        .map_err(|_| "node state lock poisoned".to_string())?;
    if node.tip_hash() != Some(parent_hash) {
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

fn update_mining_stats(mining_stats: &MiningStats, attempts: u64, elapsed: Duration) {
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
        .parse::<u64>()
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

fn parse_address(value: Option<&String>) -> Result<Address, String> {
    let Some(value) = value else {
        return Err("missing address".to_string());
    };
    parse_address_string(value)
}

fn parse_address_string(value: &str) -> Result<Address, String> {
    address_from_string(value).or_else(|_| parse_address_hex(value))
}

fn parse_address_hex(value: &str) -> Result<Address, String> {
    let bytes = hex::decode(value).map_err(|_| "invalid address hex".to_string())?;
    let bytes = bytes
        .try_into()
        .map_err(|_| "address has invalid length".to_string())?;
    Ok(Address(bytes))
}

fn parse_hash_hex(value: &str) -> Result<Hash, String> {
    let bytes = hex::decode(value).map_err(|_| "invalid hash hex".to_string())?;
    let bytes = bytes
        .try_into()
        .map_err(|_| "hash has invalid length".to_string())?;
    Ok(Hash(bytes))
}

fn unix_timestamp() -> Result<u64, String> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .map_err(|_| "system clock is before unix epoch".to_string())
}

fn format_hash<T>(hash: Option<T>) -> String
where
    T: Into<Hash>,
{
    hash.map(|hash| hex::encode(hash.into().0))
        .unwrap_or_else(|| "none".to_string())
}

fn short_hash<T>(hash: Option<T>) -> String
where
    T: Into<Hash>,
{
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

fn pow_target_description(difficulty: u32) -> String {
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

fn print_help() {
    println!(
        "\
paqus

Usage:
  paqus
  paqus menu
  paqus --help
  paqus version
  paqus node info
  paqus node libp2p-info
  paqus node config [config-path]
  paqus node init [db-path] [miner-address]
  paqus node run [db-path] [--config path] [--listen addr] [--rpc-listen addr] [--peer addr] [--peers-file path] [--gateway host:port] [--public-addr host:port] [--min-relay-fee units-per-kib] [--market-fee units-per-kib] [--miner-min-fee-rate units-per-kib] [--low-fee-expiry-secs n] [--mempool-expiry-secs n] [--wallet path] [--miner address] [--miner-secret-key key-hex] [--mine]
  paqus wallet new [wallet-path] [--show-secret]
  paqus wallet address <secret-key-hex>
  paqus wallet balance <address> [db-path]
  paqus wallet pay <address> <amount> [--wallet path] [--fee units] [--rpc addr]
  paqus wallet send <address> <amount> [--wallet path] [--nonce n] [--fee units] [--rpc addr]
  paqus wallet send --wallet path --to address --amount units [--nonce n] [--fee units] [--submit] [--rpc addr]

RPC:
  GET  /status
  GET  /health
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
  POST /tx              JSON: {{\"tx\":\"signed-transaction-hex\"}}

To bootstrap mining with your own account:
  1. Create a wallet: paqus wallet new wallet.json
  2. Create config: paqus node config
  3. Edit ./data/paqus/node.json once
  4. Run: paqus node run
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
    println!("confirmation_depth: {CONFIRMATION_DEPTH}");
    println!("finality_depth: {FINALITY_DEPTH}");
    println!("difficulty_start: {DIFFICULTY_START}");
}

fn print_libp2p_info() -> Result<(), String> {
    let swarm = libp2p_node::build_swarm()?;
    println!("peer_id: {}", swarm.local_peer_id());
    println!("block_topic: {}", libp2p_node::PAQUS_BLOCK_TOPIC);
    println!("tx_topic: {}", libp2p_node::PAQUS_TX_TOPIC);
    println!("request_protocol: {}", libp2p_node::PAQUS_REQUEST_PROTOCOL);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| value.to_string()).collect()
    }

    #[test]
    fn parse_address_accepts_wallet_address_string() {
        let address = Address([0xab; 20]);
        let encoded = address_to_string(&address);

        assert_eq!(parse_address_string(&encoded), Ok(address));
    }

    #[test]
    fn parse_address_accepts_legacy_hex() {
        let address = Address([0xab; 20]);
        let encoded = hex::encode(address.0);

        assert_eq!(parse_address_string(&encoded), Ok(address));
    }

    #[test]
    fn parse_run_config_accepts_pasted_flags_with_surrounding_spaces() {
        let config = parse_run_config(&args(&[
            "--config",
            "/tmp/paqus-node-missing-test-config.json",
            "./data/paqus",
            " --listen",
            "0.0.0.0:5555",
            " --listen",
            "[::]:5555",
            " --rpc-listen",
            "127.0.0.1:6666",
            " --public-addr",
            "[2404:8000:1044:4d8:822b:f9ff:fee2:365]:5555",
            " --peer",
            "[2404:8000:1044:4d8:1202:b5ff:feb0:7020]:5555",
            " --peer",
            "182.253.148.123:5555",
            " --mine",
            " --mine-attempts",
            "100000",
        ]))
        .expect("pasted flags should parse");

        assert_eq!(config.db_path, "./data/paqus");
        assert_eq!(config.listen_addrs.len(), 2);
        assert_eq!(config.peers.len(), 3);
        assert_eq!(config.public_addrs.len(), 1);
        assert_eq!(config.rpc_addr, "127.0.0.1:6666".parse().unwrap());
        assert!(config.mine);
        assert_eq!(config.mine_attempts, 100000);
    }
}
