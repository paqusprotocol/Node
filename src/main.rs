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
use network::{bind_nonblocking, configure_stream, http_get, http_post_json, send_message};
use p2p::{PeerPoll, PeerState, dedupe_peers, load_peers_file, poll_peer, request_peers};
use paquscore::{
    Address, Amount, Block, BlockHash, Consensus, GENESIS_PREMINE_ADDRESS, Hash, Height,
    NetworkMessage, Node, Nonce, PeerInfo, SecretKey, SignedTransaction, Transaction, Wallet,
    address_from_public_key, address_to_string, derive_public_key, handle_message, read_message,
    write_message,
};
use paquscore::{
    BLOCK_REWARD_MATURITY, BLOCK_TIME, CHAIN_ID, CHAIN_NAME, COIN_NAME, CONFIRMATION_DEPTH,
    DIFFICULTY_ADJUSTMENT_INTERVAL, DIFFICULTY_START, FINALITY_DEPTH, MAX_BLOCK_TXS, MIN_FEE,
    NETWORK_MAGIC, PROTOCOL_STAGE, PROTOCOL_VERSION, STORAGE_VERSION,
};
use runtime::miner::{MiningConfig, mine_prepared_block, prepare_candidate_block};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::env;
use std::fs;
use std::io;
use std::io::Write as IoWrite;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::process::ExitCode;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const DEFAULT_NODE_DB: &str = "./data/paqus";
const DEFAULT_LISTEN_ADDR: &str = "0.0.0.0:30333";
const DEFAULT_RPC_ADDR: &str = "127.0.0.1:9933";
const DEFAULT_CONFIG_FILE: &str = "./data/paqus/node.json";
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
    let fee = parse_amount(Some(&prompt_default("Fee", &MIN_FEE.to_string())?), "fee")?;
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
    let mut fee = Amount(MIN_FEE);
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
        let mut fee = Amount(MIN_FEE);
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
    let mut fee = Amount(MIN_FEE);
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
    listen_addr: SocketAddr,
    rpc_addr: SocketAddr,
    peers: Vec<SocketAddr>,
    peers_file: Option<String>,
    gateway_url: Option<String>,
    public_addr: Option<SocketAddr>,
    gateway_heartbeat: Duration,
    shutdown_file: String,
    max_peers: usize,
    miner_address: Address,
    miner_secret_key: Option<SecretKey>,
    mine: bool,
    mine_interval: Duration,
    mine_attempts: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RunConfigFile {
    db_path: String,
    listen_addr: String,
    rpc_addr: String,
    peers: Vec<String>,
    peers_file: Option<String>,
    gateway_url: Option<String>,
    public_addr: Option<String>,
    gateway_heartbeat_secs: u64,
    shutdown_file: String,
    max_peers: usize,
    wallet: Option<String>,
    miner_address: Option<String>,
    miner_secret_key: Option<String>,
    mine: bool,
    mine_interval_secs: u64,
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
    confirmation_depth: u32,
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
            gateway_url: None,
            public_addr: None,
            gateway_heartbeat: DEFAULT_GATEWAY_HEARTBEAT,
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

impl Default for RunConfigFile {
    fn default() -> Self {
        let defaults = RunConfig::default();
        Self {
            db_path: defaults.db_path,
            listen_addr: defaults.listen_addr.to_string(),
            rpc_addr: defaults.rpc_addr.to_string(),
            peers: Vec::new(),
            peers_file: Some("./data/paqus/peers.txt".to_string()),
            gateway_url: None,
            public_addr: None,
            gateway_heartbeat_secs: defaults.gateway_heartbeat.as_secs(),
            shutdown_file: defaults.shutdown_file,
            max_peers: defaults.max_peers,
            wallet: None,
            miner_address: None,
            miner_secret_key: None,
            mine: false,
            mine_interval_secs: defaults.mine_interval.as_secs(),
            mine_attempts: defaults.mine_attempts,
        }
    }
}

struct NodeService {
    node: Arc<Mutex<Node>>,
    config: RunConfig,
    listener: TcpListener,
    peers: Arc<Mutex<HashMap<SocketAddr, PeerState>>>,
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
    fn new(node: Arc<Mutex<Node>>, config: RunConfig, listener: TcpListener) -> Self {
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
            listener,
            peers: Arc::new(Mutex::new(peers)),
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
            let secret_key = self.config.miner_secret_key.ok_or_else(|| {
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
                self.set_activity(NodeActivity::Mining)?;
                let block = mine_once_unlocked(&self.node, &self.config)?;
                if let Some(block) = block {
                    let height = block.height().0;
                    let hash = short_hash(Some(block.hash()));
                    let tx_count = block.transactions.len();
                    let report = self.broadcast(NetworkMessage::Block(block));
                    println!(
                        "broadcasting block height {} |hash::{}|txs::{}|peers::{}|sent::{}|failed::{}|",
                        height, hash, tx_count, report.attempted, report.sent, report.failed
                    );
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
                    "status: |height::{}|tip::{}|difficulty::{}|mempool::{}|peers::{}|mining::{}",
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

    fn log_activity(&mut self) -> Result<(), String> {
        let (height, tip, mempool_len, mining) = {
            let node = self
                .node
                .lock()
                .map_err(|_| "node state lock poisoned".to_string())?;
            (
                node.tip_height().unwrap_or(Height(0)).0,
                short_hash(node.tip_hash()),
                node.mempool.len(),
                self.config.mine,
            )
        };
        let peer_count = self
            .peers
            .lock()
            .map_err(|_| "peer state lock poisoned".to_string())?
            .len();

        let activity = if peer_count == 0 && self.config.gateway_url.is_some() {
            NodeActivity::WaitingForPeers
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
            println!(
                "node activity:: |state::{:?}|height::{}|tip::{}|mempool::{}|peers::{}|mining::{}|",
                activity, height, tip, mempool_len, peer_count, mining
            );
            self.last_activity = activity;
            self.last_activity_log = Instant::now();
        }

        Ok(())
    }

    fn set_activity(&mut self, activity: NodeActivity) -> Result<(), String> {
        if activity == self.last_activity
            && self.last_activity_log.elapsed() < ACTIVITY_LOG_INTERVAL
        {
            return Ok(());
        }
        let (height, tip, mempool_len) = {
            let node = self
                .node
                .lock()
                .map_err(|_| "node state lock poisoned".to_string())?;
            (
                node.tip_height().unwrap_or(Height(0)).0,
                short_hash(node.tip_hash()),
                node.mempool.len(),
            )
        };
        let peer_count = self
            .peers
            .lock()
            .map_err(|_| "peer state lock poisoned".to_string())?
            .len();
        println!(
            "node activity:: |state::{:?}|height::{}|tip::{}|mempool::{}|peers::{}|mining::{}|",
            activity, height, tip, mempool_len, peer_count, self.config.mine
        );
        self.last_activity = activity;
        self.last_activity_log = Instant::now();
        Ok(())
    }

    fn accept_p2p(&mut self) -> Result<(), String> {
        loop {
            match self.listener.accept() {
                Ok((stream, peer)) => {
                    self.set_activity(NodeActivity::ServingPeers)?;
                    println!("p2p inbound:: |peer::{}|event::accepted|", peer);
                    self.handle_p2p_stream(stream, peer)?;
                }
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
                        let inbound_log = inbound_message_log(&message, peer);
                        let mut node = self
                            .node
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
                        let _ = self.save_peers();
                        eprintln!(
                            "peer {peer} sync failed {MAX_PEER_FAILURES} times; dropped: {error}"
                        );
                    } else {
                        eprintln!("peer {peer} sync failed: {error}");
                    }
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
            if Some(addr) == self.config.public_addr || addr == self.config.listen_addr {
                continue;
            }
            if known.contains(&addr) {
                continue;
            }
            current.entry(addr).or_insert_with(|| PeerState::new(addr));
        }
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

        if let Some(public_addr) = self.config.public_addr {
            let result = if register {
                register_peer(&gateway_url, public_addr, best_height, tip_hash.clone())
            } else {
                heartbeat_peer(&gateway_url, public_addr, best_height, tip_hash.clone())
            };
            if let Err(error) = result {
                eprintln!("gateway update failed: {error}");
            }
        } else if register {
            eprintln!("gateway configured without --public-addr; querying peers only");
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
                self.config.public_addr.or(Some(self.config.listen_addr)),
            ) {
                Ok(peers) => {
                    if !peers.is_empty() {
                        println!("gateway discovered {} peer(s)", peers.len());
                        self.add_peer_infos(peers);
                        let _ = self.save_peers();
                    }
                }
                Err(error) => eprintln!("gateway peer query failed: {error}"),
            }
        }

        self.last_gateway_heartbeat = Instant::now();
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
        for peer in peers {
            if let Err(error) = send_message(peer, message.clone()) {
                report.failed += 1;
                eprintln!("broadcast to {peer} failed: {error}");
            } else {
                report.sent += 1;
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
        confirmation_depth: CONFIRMATION_DEPTH,
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
    let block_hash = BlockHash::from(hash);
    match state.node.lock() {
        Ok(node) => match node.storage.load_block_by_hash(&block_hash) {
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
    println!(
        "accepted tx:: |hash::{}|amount::{}|fee::{}|nonce::{}|",
        short_hash(Some(hash)),
        transaction.transaction.amount.0,
        transaction.transaction.fee.0,
        transaction.transaction.nonce.0
    );
    let report = broadcast_to_peers(&state.peers, NetworkMessage::Transaction(transaction));
    println!(
        "broadcast tx:: |hash::{}|peers::{}|sent::{}|failed::{}|",
        short_hash(Some(hash)),
        report.attempted,
        report.sent,
        report.failed
    );
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
    TxResponse {
        hash: hex::encode(transaction.hash().0),
        from: address_to_string(&transaction.transaction.from),
        to: address_to_string(&transaction.transaction.to),
        amount: transaction.transaction.amount.0,
        fee: transaction.transaction.fee.0,
        nonce: transaction.transaction.nonce.0,
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
                    Some(block.hash().into()),
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
            if transaction.transaction.from == *address || transaction.transaction.to == *address {
                transactions.push(tx_response(
                    transaction,
                    Some(block.height()),
                    Some(block.hash().into()),
                    "confirmed",
                ));
            }
        }
    }

    for transaction in node.mempool.transactions() {
        if transaction.transaction.from == *address || transaction.transaction.to == *address {
            transactions.push(tx_response(transaction, None, None, "pending"));
        }
    }

    transactions.reverse();
    Ok(transactions)
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
        "Paqus Node db::{}|p2p::{}|rpc::{}|height::{}|tip::{}|difficulty::{}|peers::{}|mining::{}",
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
    let mut config = RunConfig::default();
    let config_path = config_path_arg(args).unwrap_or(DEFAULT_CONFIG_FILE);
    if let Some(file_config) = load_run_config_file_if_exists(config_path)? {
        apply_run_config_file(&mut config, file_config)?;
    }
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
                config.public_addr = Some(parse_socket(args.get(index), "--public-addr")?);
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
    config.listen_addr = file
        .listen_addr
        .parse()
        .map_err(|error| format!("invalid listen_addr in config: {error}"))?;
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
    config.public_addr = file
        .public_addr
        .map(|addr| {
            addr.parse()
                .map_err(|error| format!("invalid public_addr in config: {error}"))
        })
        .transpose()?;
    config.gateway_heartbeat = Duration::from_secs(file.gateway_heartbeat_secs.max(1));
    config.shutdown_file = file.shutdown_file;
    config.max_peers = file.max_peers.max(1);
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

    Ok(())
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
) -> Result<Option<Block>, String> {
    let timestamp = unix_timestamp()?;
    let (candidate, consensus, mining_config) = {
        let mut node = node_state
            .lock()
            .map_err(|_| "node state lock poisoned".to_string())?;
        node.mempool.prune_expired(timestamp);
        let difficulty = node.next_difficulty().map_err(|error| error.to_string())?;
        let candidate = prepare_candidate_block(
            &node.mempool,
            &node.ledger,
            config.miner_address,
            timestamp,
            MAX_BLOCK_TXS,
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
            },
        )
    };

    let parent_hash = BlockHash::from(candidate.previous_hash().as_hash());
    let Some(result) = mine_prepared_block(candidate, &consensus, mining_config)
        .map_err(|error| format!("mining failed: {error}"))?
    else {
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
        "mined:: |height::{}|hash::{}|difficulty::{}|txs::{}|attempts::{}|",
        result.block.height().0,
        short_hash(Some(result.block.hash())),
        result.block.difficulty(),
        result.block.transactions.len(),
        result.attempts
    );
    Ok(Some(result.block))
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
  paqus node init [db-path] [miner-address-hex]
  paqus node run [db-path] [--config path] [--listen addr] [--rpc-listen addr] [--peer addr] [--peers-file path] [--gateway host:port] [--public-addr host:port] [--wallet path] [--miner address-hex] [--miner-secret-key key-hex] [--mine]
  paqus wallet new [wallet-path] [--show-secret]
  paqus wallet address <secret-key-hex>
  paqus wallet balance <address-hex> [db-path]
  paqus wallet pay <address-hex> <amount> [--wallet path] [--fee units] [--rpc addr]
  paqus wallet send <address-hex> <amount> [--wallet path] [--nonce n] [--fee units] [--rpc addr]
  paqus wallet send --wallet path --to address-hex --amount units [--nonce n] [--fee units] [--submit] [--rpc addr]

RPC:
  GET  /status
  GET  /health
  GET  /chain
  GET  /peers
  GET  /balance/<address-hex>
  GET  /blocks/latest
  GET  /blocks/<height>
  GET  /blocks/hash/<block-hash>
  GET  /tx/<tx-hash>
  GET  /address/<address-hex>
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
