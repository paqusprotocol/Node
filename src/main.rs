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
use borsh::BorshDeserialize;
use futures_util::stream;
use gateway::{heartbeat_peer, register_peer, request_gateway_peers};
use p2p::gossip::{BroadcastReport, broadcast_to_peers};
use p2p::{
    PERSISTENT_PEER_TIMEOUT, PeerConnection, PeerPoll, PeerState, dedupe_peers, load_peers_file,
    poll_peer_connection, request_peers_connection, save_peers_file, sync_from_peers_parallel,
    sync_mempool_connection,
};
use paqus::block::{Block, Height, Nonce};
use paqus::codec::{block_bytes, decode_block};
use paqus::consensus::supply::Amount;
use paqus::consensus::{ASERT_HALF_LIFE, Consensus, DIFFICULTY_START};
use paqus::crypto::{
    Address, BlockHash, Hash, SecretKey, TransactionHash, WitnessTransactionHash,
    address_from_public_key, address_from_string, address_to_string, derive_public_key,
};
use paqus::event::{EventId, ProtocolEvent, ProtocolEventKind};
use paqus::genesis::{CURRENT_CHAIN_PARAMS, GENESIS_MINER_ADDRESS as GENESIS_PREMINE_ADDRESS};
use paqus::ledger::{BLOCK_REWARD_MATURITY, CONFIRMATION_DEPTH, FINALITY_DEPTH};
use paqus::transaction::{SignedEcashTransaction, SignedProtocolTransaction};
use paqus::transaction::{SignedTransaction, Transaction};
use rpc::transport::{bind_nonblocking, configure_stream, http_get, http_post_json};
use rpc::wallet_nonce::resolve_wallet_nonce;
use runtime::mempool::MempoolConfig;
use runtime::miner::{MiningConfig, mine_prepared_block, prepare_candidate_block};
use runtime::network::NetworkError;
use runtime::network::{
    InventoryItem, NetworkMessage, PeerInfo, handle_message, read_message, write_message,
};
use runtime::node::Node;
use runtime::params::{
    BLOCK_TIME, CHAIN_ID, CHAIN_NAME, COIN_NAME, DEFAULT_TRANSACTION_FEE, GENESIS_PREMINE,
    MAX_BLOCK_TXS, NETWORK_MAGIC, PROTOCOL_STAGE, PROTOCOL_VERSION, STORAGE_VERSION,
};
use runtime::wallet::Wallet;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet, VecDeque, hash_map::Entry};
use std::convert::Infallible;
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
const DEFAULT_LISTEN_ADDR: &str = "[::]:5555";
const DEFAULT_RPC_ADDR: &str = "127.0.0.1:6666";
const DEFAULT_CONFIG_FILE: &str = "./data/paqus/node.json";
const DEFAULT_PEERS_FILE: &str = "./data/paqus/peers.json";
const DEFAULT_MINING_INTERVAL: Duration = Duration::ZERO;
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
    Err(
        "wallet creation moved to wallet-cli so secrets are encrypted; run `wallet-cli new <path>`"
            .to_string(),
    )
}

fn menu_import_wallet() -> Result<(), String> {
    Err("wallet import moved to wallet-cli so secrets are encrypted; run `wallet-cli import <path>`"
        .to_string())
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
            "Fee (default dihitung 1 paqus/virtual-byte)",
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
                return Err(format!(
                    "refusing to write plaintext wallet `{path}`; use `wallet-cli new {path}`"
                ));
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
    let timestamp = unix_timestamp()?;
    let transaction = Transaction::new_at(wallet.address, to, amount, fee, nonce, timestamp);
    let mut signed = wallet
        .sign_transaction(transaction)
        .map_err(|error| format!("failed to sign transaction: {error}"))?;
    if fee.0 == DEFAULT_TRANSACTION_FEE {
        let fee = Amount(signed.virtual_size() as u64);
        signed = wallet
            .sign_transaction(Transaction::new_at(
                wallet.address,
                to,
                amount,
                fee,
                nonce,
                timestamp,
            ))
            .map_err(|error| format!("failed to sign transaction: {error}"))?;
    }
    let tx_hex = signed_transaction_to_hex(&signed)?;

    if submit {
        let body = format!("{{\"tx\":\"{tx_hex}\"}}");
        let response = http_post_json(rpc_addr, "/tx", &body)?;
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
        Some("db") => node_db_command(&args[1..]),
        Some("config") => node_config_command(&args[1..]),
        Some("libp2p-info") => {
            print_libp2p_info()?;
            Ok(())
        }
        Some("info") => {
            print_network_info();
            Ok(())
        }
        _ => Err("usage: paqus node <info|libp2p-info|init|config|run|db> [options]".to_string()),
    }
}

fn node_db_command(args: &[String]) -> Result<(), String> {
    match args.first().map(String::as_str) {
        Some("check") => {
            let path = args.get(1).map(String::as_str).unwrap_or(DEFAULT_NODE_DB);
            let node = open_node(path, Address([9; 20]))?;
            node.storage
                .validate_chain_integrity()
                .map_err(|error| format!("database integrity check failed: {error}"))?;
            println!("database integrity ok: {path}");
            Ok(())
        }
        Some("backup") => {
            let source = args.get(1).map(String::as_str).unwrap_or(DEFAULT_NODE_DB);
            let destination = args
                .get(2)
                .ok_or_else(|| "usage: paqus node db backup <database> <backup>".to_string())?;
            backup_node_database(source, destination)
        }
        Some("restore") => {
            let backup = args
                .get(1)
                .ok_or_else(|| "usage: paqus node db restore <backup> <database>".to_string())?;
            let destination = args
                .get(2)
                .ok_or_else(|| "usage: paqus node db restore <backup> <database>".to_string())?;
            restore_node_database(backup, destination)
        }
        _ => Err("usage: paqus node db <check|backup|restore> [paths]".to_string()),
    }
}

fn backup_node_database(source: &str, destination: &str) -> Result<(), String> {
    let node = open_node(source, Address([9; 20]))?;
    node.flush_to_storage()
        .map_err(|error| format!("failed to flush database before backup: {error}"))?;
    node.storage
        .validate_chain_integrity()
        .map_err(|error| format!("refusing to back up invalid database: {error}"))?;
    drop(node);

    let destination = std::path::Path::new(destination);
    if destination.exists() {
        return Err("backup destination already exists".to_string());
    }
    fs::create_dir_all(destination)
        .map_err(|error| format!("failed to create backup directory: {error}"))?;
    fs::copy(
        std::path::Path::new(source).join("data.mdb"),
        destination.join("data.mdb"),
    )
    .map_err(|error| format!("failed to copy database backup: {error}"))?;
    println!("database backup created: {}", destination.display());
    Ok(())
}

fn restore_node_database(backup: &str, destination: &str) -> Result<(), String> {
    let backup_node = open_node(backup, Address([9; 20]))?;
    backup_node
        .storage
        .validate_chain_integrity()
        .map_err(|error| format!("refusing to restore invalid backup: {error}"))?;
    drop(backup_node);

    let destination = std::path::Path::new(destination);
    if destination.exists() {
        return Err("restore destination already exists".to_string());
    }
    fs::create_dir_all(destination)
        .map_err(|error| format!("failed to create restore directory: {error}"))?;
    fs::copy(
        std::path::Path::new(backup).join("data.mdb"),
        destination.join("data.mdb"),
    )
    .map_err(|error| format!("failed to restore database: {error}"))?;
    let restored = open_node(destination.to_string_lossy().as_ref(), Address([9; 20]))?;
    restored
        .storage
        .validate_chain_integrity()
        .map_err(|error| format!("restored database failed integrity check: {error}"))?;
    println!("database restored: {}", destination.display());
    Ok(())
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
struct SubmitTxRequest {
    tx: String,
}

#[derive(Debug, Deserialize)]
struct MiningTemplateQuery {
    miner: String,
}

#[derive(Debug, Deserialize)]
struct SubmitBlockRequest {
    block: String,
}

#[derive(Serialize)]
struct MiningTemplateResponse {
    job_id: String,
    block: String,
    height: u64,
    previous_hash: String,
    difficulty: u32,
    algorithm: &'static str,
}

#[derive(Serialize)]
struct SubmitBlockResponse {
    accepted: bool,
    height: u64,
    hash: String,
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
    metrics: Arc<RpcMetrics>,
    db_path: String,
}

#[derive(Default)]
struct RpcMetrics {
    requests_total: AtomicU64,
    errors_total: AtomicU64,
    latency_micros_total: AtomicU64,
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
    next_nonce: AtomicU64,
}

#[derive(Serialize)]
struct StatusResponse {
    chain: &'static str,
    stage: &'static str,
    protocol_version: u8,
    pow_algorithm: &'static str,
    difficulty_algorithm: &'static str,
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
    wtxid: String,
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
    pow_algorithm: &'static str,
    difficulty_algorithm: &'static str,
    asert_half_life_secs: u64,
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
struct ProtocolTxResponse {
    family: &'static str,
    operation: &'static str,
    txid: String,
    wtxid: String,
    signer: String,
    witness_addresses: Vec<String>,
    recipient: Option<String>,
    amount: Option<u64>,
    fee: u64,
    nonce: u64,
    valid_from: u64,
    valid_until: u64,
    timestamp: Option<u64>,
    age_secs: Option<u64>,
    stripped_size: usize,
    witness_size: usize,
    virtual_size: usize,
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
    version: u8,
    height: u64,
    hash: String,
    short_hash: String,
    previous_hash: String,
    merkle_root: String,
    witness_root: String,
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
    stripped_size: usize,
    witness_size: usize,
    weight: usize,
    coinbase: Option<CoinbaseResponse>,
    genesis_allocations: Vec<GenesisAllocationResponse>,
    transactions: Vec<ProtocolTxResponse>,
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
    transactions: Vec<ProtocolTxResponse>,
}

#[derive(Serialize)]
struct AddressResponse {
    address: String,
    balance: serde_json::Value,
    mined_blocks: Vec<MinedBlockResponse>,
    transactions: Vec<ProtocolTxResponse>,
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
    transactions: Vec<ProtocolTxResponse>,
}

#[derive(Serialize)]
struct ProtocolEventResponse {
    id: String,
    event: ProtocolEvent,
}

#[derive(Debug, Default, Deserialize)]
struct EventQuery {
    offset: Option<usize>,
    limit: Option<usize>,
    kind: Option<String>,
    from_height: Option<u64>,
    to_height: Option<u64>,
}

#[derive(Debug, Default, Deserialize)]
struct EventStreamQuery {
    from_height: Option<u64>,
    kind: Option<String>,
    address: Option<String>,
}

struct ProtocolEventStreamState {
    rpc: RpcState,
    next_height: u64,
    kind: Option<String>,
    address: Option<Address>,
    pending: VecDeque<ProtocolEvent>,
    poll_immediately: bool,
}

#[derive(Serialize)]
struct ProtocolEventListResponse {
    total: usize,
    offset: usize,
    limit: usize,
    events: Vec<ProtocolEventResponse>,
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
            peers: Vec::new(),
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
            mine_attempts: 100_000,
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
            if let Some(secret_key) = self.config.miner_secret_key.as_ref() {
                let public_key = derive_public_key(secret_key);
                let derived_address = address_from_public_key(&public_key);
                if derived_address != self.config.miner_address {
                    return Err(format!(
                        "miner secret key does not match miner address {}",
                        address_to_string(&self.config.miner_address)
                    ));
                }
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
                if let Ok(accepted) = sync_mempool_connection(&mut connection, &self.node)
                    && accepted > 0
                {
                    println!("preflight mempool synced:: |peer::{peer}|txs::{accepted}|");
                }
                Ok(poll)
            });
            match result {
                Ok(PeerPoll::Idle { remote_tip }) => {
                    println!(
                        "preflight peer {peer} ok:: |remote_tip::{}|state::idle|",
                        remote_tip.0
                    );
                    if let Ok(mut peers) = self.peers.lock()
                        && let Some(state) = peers.get_mut(&peer)
                    {
                        state.mark_ok(Some(remote_tip));
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
                    if let Ok(mut peers) = self.peers.lock()
                        && let Some(state) = peers.get_mut(&peer)
                    {
                        state.mark_synced(remote_tip, synced_blocks);
                    }
                }
                Err(error) => {
                    if let Ok(mut peers) = self.peers.lock()
                        && let Some(state) = peers.get_mut(&peer)
                    {
                        state.mark_failed();
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
            node.mempool.len() + node.extension_mempool.len(),
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
                node.mempool.len() + node.extension_mempool.len(),
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

    #[allow(clippy::too_many_arguments)] // Detached connection task receives its owned context.
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
                    if let Ok(mut peers) = self.peers.lock()
                        && let Some(state) = peers.get_mut(&peer)
                    {
                        state.mark_ok(Some(remote_tip));
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
                    if let Some(infos) = infos
                        && self.add_peer_infos(infos)
                    {
                        let _ = self.save_peers();
                    }
                    self.sync_mempool_from_peer(peer);
                }
                Ok(PeerPoll::Synced {
                    remote_tip,
                    synced_blocks,
                }) => {
                    if let Ok(mut peers) = self.peers.lock()
                        && let Some(state) = peers.get_mut(&peer)
                    {
                        state.mark_synced(remote_tip, synced_blocks);
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
                    if let Some(infos) = infos
                        && self.add_peer_infos(infos)
                    {
                        let _ = self.save_peers();
                    }
                    self.sync_mempool_from_peer(peer);
                }
                Err(error) => {
                    let mut dropped = false;
                    if let Ok(mut peers) = self.peers.lock()
                        && let Some(state) = peers.get_mut(&peer)
                    {
                        state.mark_failed();
                        if state.failures >= MAX_PEER_FAILURES {
                            peers.remove(&peer);
                            dropped = true;
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
            "received tx:: |peer::{}|family::{:?}|hash::{}|fee::{}|nonce::{}|",
            peer,
            transaction.family(),
            short_hash(Some(transaction.hash())),
            transaction.fee().0,
            transaction.nonce().0
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
    let metrics = state.metrics.clone();
    let app = Router::new()
        .route("/", get(rpc_status))
        .route("/status", get(rpc_status))
        .route("/health", get(rpc_health))
        .route("/metrics", get(rpc_metrics))
        .route("/chain", get(rpc_chain))
        .route("/stats", get(rpc_stats))
        .route("/chain/stats", get(rpc_stats))
        .route("/peers", get(rpc_peers))
        .route("/balance/{address}", get(rpc_balance))
        .route("/blocks/latest", get(rpc_latest_blocks))
        .route("/blocks/{height}", get(rpc_block_by_height))
        .route("/blocks/hash/{hash}", get(rpc_block_by_hash))
        .route("/blocks/{height}/events", get(rpc_block_events))
        .route("/tx/{hash}", get(rpc_tx))
        .route("/tx/{hash}/events", get(rpc_transaction_events))
        .route("/address/{address}", get(rpc_address))
        .route("/address/{address}/events", get(rpc_address_events))
        .route("/events/stream", get(rpc_event_stream))
        .route("/events/{id}", get(rpc_event))
        .route("/accounts", get(rpc_accounts))
        .route("/mempool", get(rpc_mempool))
        .route("/ecash/mempool", get(rpc_ecash_mempool))
        .route("/mining/template", get(rpc_mining_template))
        .route("/mining/submit", post(rpc_submit_mined_block))
        .route("/tx", post(rpc_submit_tx))
        .route("/transaction", post(rpc_submit_tx))
        .route("/protocol/transaction", post(rpc_submit_protocol_tx))
        .route("/ecash/tx", post(rpc_submit_ecash_tx))
        .layer(middleware::from_fn_with_state(metrics, track_rpc_request))
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

async fn track_rpc_request(
    State(metrics): State<Arc<RpcMetrics>>,
    request: Request<Body>,
    next: Next,
) -> axum::response::Response {
    let started = Instant::now();
    let response = next.run(request).await;
    metrics.requests_total.fetch_add(1, Ordering::Relaxed);
    metrics.latency_micros_total.fetch_add(
        started.elapsed().as_micros().min(u128::from(u64::MAX)) as u64,
        Ordering::Relaxed,
    );
    if response.status().is_client_error() || response.status().is_server_error() {
        metrics.errors_total.fetch_add(1, Ordering::Relaxed);
    }
    response
}

async fn rpc_metrics(State(state): State<RpcState>) -> impl IntoResponse {
    let (height, mempool_size, validation_failures, reorgs) = state
        .node
        .lock()
        .map(|node| {
            (
                node.tip_height().unwrap_or(Height(0)).0,
                node.mempool.len() + node.extension_mempool.len(),
                node.block_validation_failures_total(),
                node.reorgs_total(),
            )
        })
        .unwrap_or_default();
    let peer_count = state
        .peer_connections
        .lock()
        .map(|peers| peers.len())
        .unwrap_or_default()
        + state
            .inbound_connections
            .lock()
            .map(|peers| peers.len())
            .unwrap_or_default();
    let database_bytes = fs::metadata(std::path::Path::new(&state.db_path).join("data.mdb"))
        .map(|metadata| metadata.len())
        .unwrap_or_default();
    let body = format!(
        concat!(
            "# TYPE paqus_chain_height gauge\npaqus_chain_height {height}\n",
            "# TYPE paqus_peer_count gauge\npaqus_peer_count {peer_count}\n",
            "# TYPE paqus_mempool_size gauge\npaqus_mempool_size {mempool_size}\n",
            "# TYPE paqus_block_validation_failures_total counter\npaqus_block_validation_failures_total {validation_failures}\n",
            "# TYPE paqus_reorgs_total counter\npaqus_reorgs_total {reorgs}\n",
            "# TYPE paqus_rpc_requests_total counter\npaqus_rpc_requests_total {requests}\n",
            "# TYPE paqus_rpc_errors_total counter\npaqus_rpc_errors_total {errors}\n",
            "# TYPE paqus_rpc_latency_seconds summary\npaqus_rpc_latency_seconds_sum {latency_seconds:.6}\npaqus_rpc_latency_seconds_count {requests}\n",
            "# TYPE paqus_mining_hashrate_hps gauge\npaqus_mining_hashrate_hps {hashrate}\n",
            "# TYPE paqus_database_size_bytes gauge\npaqus_database_size_bytes {database_bytes}\n"
        ),
        height = height,
        peer_count = peer_count,
        mempool_size = mempool_size,
        validation_failures = validation_failures,
        reorgs = reorgs,
        requests = state.metrics.requests_total.load(Ordering::Relaxed),
        errors = state.metrics.errors_total.load(Ordering::Relaxed),
        latency_seconds =
            state.metrics.latency_micros_total.load(Ordering::Relaxed) as f64 / 1_000_000.0,
        hashrate = state.mining_stats.last_hashrate_hps.load(Ordering::Relaxed),
        database_bytes = database_bytes,
    );
    ([(header::CONTENT_TYPE, "text/plain; version=0.0.4")], body)
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
            pow_algorithm: CURRENT_CHAIN_PARAMS.pow_algorithm,
            difficulty_algorithm: CURRENT_CHAIN_PARAMS.difficulty_algorithm,
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
        pow_algorithm: CURRENT_CHAIN_PARAMS.pow_algorithm,
        difficulty_algorithm: CURRENT_CHAIN_PARAMS.difficulty_algorithm,
        asert_half_life_secs: ASERT_HALF_LIFE,
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

fn protocol_event_response(event: ProtocolEvent) -> ProtocolEventResponse {
    ProtocolEventResponse {
        id: hex::encode(event.id().0),
        event,
    }
}

fn protocol_event_kind_name(kind: &ProtocolEventKind) -> &'static str {
    match kind {
        ProtocolEventKind::Transfer { .. } => "transfer",
        ProtocolEventKind::EcashWithdrawn { .. } => "ecash_withdrawn",
        ProtocolEventKind::EcashDeposited { .. } => "ecash_deposited",
        ProtocolEventKind::GenesisAllocation { .. } => "genesis_allocation",
        ProtocolEventKind::CoinbasePaid { .. } => "coinbase_paid",
    }
}

fn is_protocol_event_kind(kind: &str) -> bool {
    matches!(
        kind,
        "transfer" | "ecash_withdrawn" | "ecash_deposited" | "genesis_allocation" | "coinbase_paid"
    )
}

fn protocol_event_list(
    events: Vec<ProtocolEvent>,
    query: EventQuery,
) -> Result<ProtocolEventListResponse, &'static str> {
    const DEFAULT_LIMIT: usize = 100;
    const MAX_LIMIT: usize = 500;

    let offset = query.offset.unwrap_or(0);
    let limit = query.limit.unwrap_or(DEFAULT_LIMIT);
    if limit == 0 || limit > MAX_LIMIT {
        return Err("event_limit_must_be_between_1_and_500");
    }
    if query
        .from_height
        .zip(query.to_height)
        .is_some_and(|(from, to)| from > to)
    {
        return Err("event_height_range_is_invalid");
    }
    let kind = query.kind.map(|kind| kind.to_ascii_lowercase());
    if kind
        .as_deref()
        .is_some_and(|kind| !is_protocol_event_kind(kind))
    {
        return Err("unknown_event_kind");
    }

    let filtered: Vec<_> = events
        .into_iter()
        .filter(|event| {
            query
                .from_height
                .is_none_or(|height| event.block_height.0 >= height)
                && query
                    .to_height
                    .is_none_or(|height| event.block_height.0 <= height)
                && kind
                    .as_deref()
                    .is_none_or(|kind| protocol_event_kind_name(&event.kind) == kind)
        })
        .collect();
    let total = filtered.len();
    let events = filtered
        .into_iter()
        .skip(offset)
        .take(limit)
        .map(protocol_event_response)
        .collect();
    Ok(ProtocolEventListResponse {
        total,
        offset,
        limit,
        events,
    })
}

fn protocol_event_involves_address(kind: &ProtocolEventKind, address: &Address) -> bool {
    match kind {
        ProtocolEventKind::Transfer { from, to, .. } => from == address || to == address,
        ProtocolEventKind::EcashWithdrawn { signer, .. } => signer == address,
        ProtocolEventKind::EcashDeposited {
            signer, recipient, ..
        } => signer == address || recipient == address,
        ProtocolEventKind::GenesisAllocation { recipient, .. } => recipient == address,
        ProtocolEventKind::CoinbasePaid { miner, .. } => miner == address,
    }
}

fn finalized_event_height(tip: Option<Height>) -> Option<u64> {
    tip.and_then(|height| height.0.checked_sub(u64::from(FINALITY_DEPTH)))
}

async fn rpc_event_stream(
    State(state): State<RpcState>,
    Query(query): Query<EventStreamQuery>,
) -> impl IntoResponse {
    let kind = query.kind.map(|kind| kind.to_ascii_lowercase());
    if kind
        .as_deref()
        .is_some_and(|kind| !is_protocol_event_kind(kind))
    {
        return rpc_error(StatusCode::BAD_REQUEST, "unknown_event_kind");
    }
    let address = match query.address {
        Some(address) => match parse_address_string(&address) {
            Ok(address) => Some(address),
            Err(error) => return rpc_error(StatusCode::BAD_REQUEST, error),
        },
        None => None,
    };
    let next_height = match query.from_height {
        Some(height) => height,
        None => match state.node.lock() {
            Ok(node) => finalized_event_height(node.tip_height())
                .map(|height| height.saturating_add(1))
                .unwrap_or(0),
            Err(_) => {
                return rpc_error(StatusCode::INTERNAL_SERVER_ERROR, "state_lock_failed");
            }
        },
    };
    let stream_state = ProtocolEventStreamState {
        rpc: state,
        next_height,
        kind,
        address,
        pending: VecDeque::new(),
        poll_immediately: true,
    };
    let events = stream::unfold(stream_state, |mut state| async move {
        loop {
            if let Some(event) = state.pending.pop_front() {
                let id = hex::encode(event.id().0);
                let event_name = protocol_event_kind_name(&event.kind);
                let data = serde_json::to_string(&protocol_event_response(event))
                    .unwrap_or_else(|_| "{\"error\":\"event_encode_failed\"}".to_string());
                let message = SseEvent::default().id(id).event(event_name).data(data);
                return Some((Ok::<_, Infallible>(message), state));
            }

            if !state.poll_immediately {
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            state.poll_immediately = false;

            let reached_tip = {
                let node = match state.rpc.node.lock() {
                    Ok(node) => node,
                    Err(_) => return None,
                };
                let finalized_height = finalized_event_height(node.tip_height());
                if finalized_height.is_none_or(|height| state.next_height > height) {
                    true
                } else {
                    let height = Height(state.next_height);
                    state.next_height = state.next_height.saturating_add(1);
                    match node.storage.load_block_by_height(height) {
                        Ok(Some(block)) => match node.storage.load_block_events(&block.hash()) {
                            Ok(events) => {
                                state.pending.extend(events.into_iter().filter(|event| {
                                    state.kind.as_deref().is_none_or(|kind| {
                                        protocol_event_kind_name(&event.kind) == kind
                                    }) && state.address.as_ref().is_none_or(|address| {
                                        protocol_event_involves_address(&event.kind, address)
                                    })
                                }));
                            }
                            Err(error) => eprintln!(
                                "failed to load protocol events at height {}: {error}",
                                height.0
                            ),
                        },
                        Ok(None) => {}
                        Err(error) => eprintln!(
                            "failed to load protocol event block at height {}: {error}",
                            height.0
                        ),
                    }
                    false
                }
            };

            if !state.pending.is_empty() {
                continue;
            }
            if !reached_tip {
                state.poll_immediately = true;
            }
        }
    });

    Sse::new(events)
        .keep_alive(
            KeepAlive::new()
                .interval(Duration::from_secs(15))
                .text("keep-alive"),
        )
        .into_response()
}

async fn rpc_event(
    State(state): State<RpcState>,
    AxumPath(id): AxumPath<String>,
) -> impl IntoResponse {
    let id = match parse_hash_hex(&id) {
        Ok(hash) => EventId(hash.0),
        Err(error) => return rpc_error(StatusCode::BAD_REQUEST, error),
    };
    match state.node.lock() {
        Ok(node) => match node.storage.load_protocol_event(&id) {
            Ok(Some(event)) => Json(protocol_event_response(event)).into_response(),
            Ok(None) => rpc_error(StatusCode::NOT_FOUND, "event_not_found"),
            Err(error) => rpc_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to load event: {error}"),
            ),
        },
        Err(_) => rpc_error(StatusCode::INTERNAL_SERVER_ERROR, "state_lock_failed"),
    }
}

async fn rpc_block_events(
    State(state): State<RpcState>,
    AxumPath(height): AxumPath<u64>,
    Query(query): Query<EventQuery>,
) -> impl IntoResponse {
    match state.node.lock() {
        Ok(node) => {
            let block = match node.storage.load_block_by_height(Height(height)) {
                Ok(Some(block)) => block,
                Ok(None) => return rpc_error(StatusCode::NOT_FOUND, "block_not_found"),
                Err(error) => {
                    return rpc_error(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("failed to load block: {error}"),
                    );
                }
            };
            match node.storage.load_block_events(&block.hash()) {
                Ok(events) => match protocol_event_list(events, query) {
                    Ok(response) => Json(response).into_response(),
                    Err(error) => rpc_error(StatusCode::BAD_REQUEST, error),
                },
                Err(error) => rpc_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("failed to load block events: {error}"),
                ),
            }
        }
        Err(_) => rpc_error(StatusCode::INTERNAL_SERVER_ERROR, "state_lock_failed"),
    }
}

async fn rpc_transaction_events(
    State(state): State<RpcState>,
    AxumPath(hash): AxumPath<String>,
    Query(query): Query<EventQuery>,
) -> impl IntoResponse {
    let hash = match parse_hash_hex(&hash) {
        Ok(hash) => TransactionHash::from(hash),
        Err(error) => return rpc_error(StatusCode::BAD_REQUEST, error),
    };
    match state.node.lock() {
        Ok(node) => match node.storage.load_transaction_events(&hash) {
            Ok(events) => match protocol_event_list(events, query) {
                Ok(response) => Json(response).into_response(),
                Err(error) => rpc_error(StatusCode::BAD_REQUEST, error),
            },
            Err(error) => rpc_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to load transaction events: {error}"),
            ),
        },
        Err(_) => rpc_error(StatusCode::INTERNAL_SERVER_ERROR, "state_lock_failed"),
    }
}

async fn rpc_address_events(
    State(state): State<RpcState>,
    AxumPath(address): AxumPath<String>,
    Query(query): Query<EventQuery>,
) -> impl IntoResponse {
    let address = match parse_address_string(&address) {
        Ok(address) => address,
        Err(error) => return rpc_error(StatusCode::BAD_REQUEST, error),
    };
    match state.node.lock() {
        Ok(node) => match node.storage.load_address_events(&address) {
            Ok(events) => match protocol_event_list(events, query) {
                Ok(response) => Json(response).into_response(),
                Err(error) => rpc_error(StatusCode::BAD_REQUEST, error),
            },
            Err(error) => rpc_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to load address events: {error}"),
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
                .accounts()
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
                .cloned()
                .map(SignedProtocolTransaction::Transfer)
                .chain(node.extension_mempool.transactions().cloned())
                .map(|transaction| protocol_tx_response(&transaction, None, None, "pending"))
                .collect::<Vec<_>>();
            Json(MempoolResponse {
                size: node.mempool.len() + node.extension_mempool.len(),
                transactions,
            })
            .into_response()
        }
        Err(_) => rpc_error(StatusCode::INTERNAL_SERVER_ERROR, "state_lock_failed"),
    }
}

async fn rpc_ecash_mempool(State(state): State<RpcState>) -> impl IntoResponse {
    match state.node.lock() {
        Ok(node) => {
            let transactions = node
                .extension_mempool
                .transactions_for_family(paqus::transaction::TransactionFamily::Ecash)
                .filter_map(|transaction| match transaction {
                    paqus::transaction::SignedProtocolTransaction::Ecash(signed) => Some(signed),
                    _ => None,
                })
                .map(|signed| {
                    serde_json::json!({
                        "hash": hex::encode(signed.hash().0),
                        "signer": address_to_string(&signed.transaction.signer),
                        "nonce": signed.transaction.nonce.0,
                        "fee": signed.transaction.fee.0,
                        "kind": match signed.transaction.kind {
                            paqus::transaction::EcashTransactionKind::WithdrawCash { .. } => "withdraw",
                            paqus::transaction::EcashTransactionKind::DepositCash { .. } => "deposit",
                        },
                    })
                })
                .collect::<Vec<_>>();
            Json(serde_json::json!({
                "size": transactions.len(),
                "transactions": transactions,
            }))
            .into_response()
        }
        Err(_) => rpc_error(StatusCode::INTERNAL_SERVER_ERROR, "state_lock_failed"),
    }
}

async fn rpc_submit_ecash_tx(
    State(state): State<RpcState>,
    Json(request): Json<SubmitTxRequest>,
) -> impl IntoResponse {
    let transaction = match signed_ecash_transaction_from_hex(&request.tx) {
        Ok(transaction) => transaction,
        Err(error) => return rpc_error(StatusCode::BAD_REQUEST, error),
    };
    let hash = transaction.hash();
    let wtxid = transaction.wtxid();
    match state.node.lock() {
        Ok(mut node) => {
            if let Err(error) = node.submit_ecash_transaction(transaction) {
                return rpc_error(
                    StatusCode::BAD_REQUEST,
                    format!("failed to submit eCash transaction: {error}"),
                );
            }
        }
        Err(_) => return rpc_error(StatusCode::INTERNAL_SERVER_ERROR, "state_lock_failed"),
    }
    Json(serde_json::json!({
        "accepted": true,
        "hash": hex::encode(hash.0),
        "wtxid": hex::encode(wtxid.0),
        "status": "pending",
    }))
    .into_response()
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
    let wtxid = transaction.wtxid();
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
        NetworkMessage::Transaction(transaction.into()),
    );
    state
        .log_counters
        .broadcast_tx_total
        .fetch_add(1, Ordering::Relaxed);
    Json(SubmitTxResponse {
        accepted: true,
        hash: hex::encode(hash.0),
        wtxid: hex::encode(wtxid.0),
    })
    .into_response()
}

async fn rpc_mining_template(
    State(state): State<RpcState>,
    Query(query): Query<MiningTemplateQuery>,
) -> impl IntoResponse {
    let miner = match address_from_string(&query.miner) {
        Ok(miner) => miner,
        Err(_) => return rpc_error(StatusCode::BAD_REQUEST, "invalid_miner_address"),
    };
    let timestamp = match unix_timestamp() {
        Ok(timestamp) => timestamp,
        Err(error) => return rpc_error(StatusCode::INTERNAL_SERVER_ERROR, error),
    };
    let candidate = match state.node.lock() {
        Ok(mut node) => {
            node.mempool.prune_expired(timestamp);
            let difficulty = match node.next_difficulty() {
                Ok(difficulty) => difficulty,
                Err(error) => {
                    return rpc_error(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("difficulty_unavailable: {error}"),
                    );
                }
            };
            match prepare_candidate_block(
                &node.mempool,
                &node.extension_mempool,
                &node.ledger,
                miner,
                timestamp,
                MAX_BLOCK_TXS,
                node.mempool.dynamic_market_fee_rate(),
                difficulty,
            ) {
                Ok(candidate) => candidate,
                Err(error) => {
                    return rpc_error(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("template_failed: {error}"),
                    );
                }
            }
        }
        Err(_) => return rpc_error(StatusCode::INTERNAL_SERVER_ERROR, "state_lock_failed"),
    };
    let job_id = hex::encode(candidate.hash().0);
    Json(MiningTemplateResponse {
        job_id,
        block: hex::encode(block_bytes(&candidate)),
        height: candidate.height().0,
        previous_hash: hex::encode(candidate.previous_hash().0),
        difficulty: candidate.difficulty(),
        algorithm: CURRENT_CHAIN_PARAMS.pow_algorithm,
    })
    .into_response()
}

async fn rpc_submit_mined_block(
    State(state): State<RpcState>,
    Json(request): Json<SubmitBlockRequest>,
) -> impl IntoResponse {
    let bytes = match hex::decode(&request.block) {
        Ok(bytes) => bytes,
        Err(_) => return rpc_error(StatusCode::BAD_REQUEST, "invalid_block_hex"),
    };
    let block = match decode_block(&bytes) {
        Ok(block) => block,
        Err(error) => {
            return rpc_error(StatusCode::BAD_REQUEST, format!("invalid_block: {error}"));
        }
    };
    let height = block.height().0;
    let hash = block.hash();
    match state.node.lock() {
        Ok(mut node) => {
            if let Err(error) = node.apply_block(block.clone()) {
                return rpc_error(StatusCode::BAD_REQUEST, format!("block_rejected: {error}"));
            }
            if let Err(error) = node.flush_to_storage() {
                return rpc_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("block_flush_failed: {error}"),
                );
            }
        }
        Err(_) => return rpc_error(StatusCode::INTERNAL_SERVER_ERROR, "state_lock_failed"),
    }
    let _report = broadcast_to_peers(
        &state.peers,
        &state.peer_connections,
        &state.inbound_connections,
        NetworkMessage::Block(block),
    );
    Json(SubmitBlockResponse {
        accepted: true,
        height,
        hash: hex::encode(hash.0),
    })
    .into_response()
}

async fn rpc_submit_protocol_tx(
    State(state): State<RpcState>,
    Json(request): Json<SubmitTxRequest>,
) -> impl IntoResponse {
    let transaction = match signed_protocol_transaction_from_hex(&request.tx) {
        Ok(transaction) => transaction,
        Err(error) => return rpc_error(StatusCode::BAD_REQUEST, error),
    };
    let hash = transaction.hash();
    let wtxid = transaction.wtxid();
    match state.node.lock() {
        Ok(mut node) => {
            if let Err(error) = node.submit_protocol_transaction(transaction.clone()) {
                return rpc_error(
                    StatusCode::BAD_REQUEST,
                    format!("failed to submit protocol transaction: {error}"),
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
    let _report = broadcast_to_peers(
        &state.peers,
        &state.peer_connections,
        &state.inbound_connections,
        NetworkMessage::Transaction(transaction),
    );
    Json(SubmitTxResponse {
        accepted: true,
        hash: hex::encode(hash.0),
        wtxid: hex::encode(wtxid.0),
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
        version: block.header.version,
        height,
        hash: hex::encode(block_hash.0),
        short_hash: short_hash(Some(block_hash)),
        previous_hash: hex::encode(block.previous_hash().0),
        merkle_root: hex::encode(block.header.merkle_root.0),
        witness_root: hex::encode(block.header.witness_root.0),
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
        value_moved: block_protocol_transactions(block)
            .iter()
            .filter_map(|transaction| protocol_transaction_summary(transaction).2)
            .sum(),
        nonce: block.header.nonce.0,
        tx_count: block.transaction_count(),
        size: block.serialized_size(),
        stripped_size: block.stripped_size(),
        witness_size: block.witness_size(),
        weight: block.weight(),
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
        transactions: block_protocol_transactions(block)
            .iter()
            .map(|transaction| {
                protocol_tx_response(
                    transaction,
                    Some(block.height()),
                    Some(block_hash.into()),
                    status.unwrap_or("confirmed"),
                )
            })
            .collect(),
    }
}

fn protocol_tx_response(
    transaction: &SignedProtocolTransaction,
    block_height: Option<Height>,
    block_hash: Option<Hash>,
    status: &'static str,
) -> ProtocolTxResponse {
    let (operation, recipient, amount, timestamp) = protocol_transaction_summary(transaction);
    let now = unix_timestamp().unwrap_or(timestamp.unwrap_or(0));
    let validity = transaction.validity();
    ProtocolTxResponse {
        family: transaction_family_name(transaction.family()),
        operation,
        txid: hex::encode(transaction.hash().0),
        wtxid: hex::encode(transaction.wtxid().0),
        signer: address_to_string(&transaction.signer()),
        witness_addresses: transaction
            .witness_addresses()
            .iter()
            .map(address_to_string)
            .collect(),
        recipient: recipient.map(|address| address_to_string(&address)),
        amount,
        fee: transaction.fee().0,
        nonce: transaction.nonce().0,
        valid_from: validity.valid_from.0,
        valid_until: validity.valid_until.0,
        timestamp,
        age_secs: timestamp.map(|timestamp| now.saturating_sub(timestamp)),
        stripped_size: transaction.stripped_size(),
        witness_size: transaction.witness_size(),
        virtual_size: transaction.virtual_size(),
        block_height: block_height.map(|height| height.0),
        block_hash: block_hash.map(|hash| hex::encode(hash.0)),
        status,
    }
}

fn transaction_family_name(family: paqus::transaction::TransactionFamily) -> &'static str {
    use paqus::transaction::TransactionFamily;
    match family {
        TransactionFamily::Transfer => "transfer",
        TransactionFamily::Ecash => "ecash",
    }
}

fn protocol_transaction_summary(
    transaction: &SignedProtocolTransaction,
) -> (&'static str, Option<Address>, Option<u64>, Option<u64>) {
    match transaction {
        SignedProtocolTransaction::Transfer(tx) => (
            "transfer",
            Some(tx.transaction.to),
            tx.transaction.total_amount().ok().map(|amount| amount.0),
            Some(tx.transaction.timestamp),
        ),
        SignedProtocolTransaction::Ecash(tx) => match &tx.transaction.kind {
            paqus::transaction::EcashTransactionKind::WithdrawCash { amount, .. } => (
                "withdraw_cash",
                None,
                Some(amount.0),
                Some(tx.transaction.timestamp),
            ),
            paqus::transaction::EcashTransactionKind::DepositCash {
                recipient,
                metadata,
            } => (
                "deposit_cash",
                Some(*recipient),
                metadata.amount().ok().map(|amount| amount.0),
                Some(tx.transaction.timestamp),
            ),
        },
    }
}

fn protocol_transaction_addresses(transaction: &SignedProtocolTransaction) -> Vec<Address> {
    let mut addresses = vec![transaction.signer()];
    if let Some(recipient) = protocol_transaction_summary(transaction).1
        && recipient != transaction.signer()
    {
        addresses.push(recipient);
    }
    addresses
}

fn block_protocol_transactions(block: &Block) -> Vec<SignedProtocolTransaction> {
    block
        .transactions
        .iter()
        .cloned()
        .map(SignedProtocolTransaction::Transfer)
        .chain(
            block
                .ecash_transactions
                .iter()
                .cloned()
                .map(SignedProtocolTransaction::Ecash),
        )
        .collect()
}

fn find_transaction(node: &Node, hash: &Hash) -> Result<Option<ProtocolTxResponse>, String> {
    for transaction in node.mempool.transactions() {
        let transaction = SignedProtocolTransaction::Transfer(transaction.clone());
        if transaction.hash() == *hash || transaction.wtxid().0 == hash.0 {
            return Ok(Some(protocol_tx_response(
                &transaction,
                None,
                None,
                "pending",
            )));
        }
    }
    for transaction in node.extension_mempool.transactions() {
        if transaction.hash() == *hash || transaction.wtxid().0 == hash.0 {
            return Ok(Some(protocol_tx_response(
                transaction,
                None,
                None,
                "pending",
            )));
        }
    }

    let txid = TransactionHash(hash.0);
    if let Some((location, transaction)) = node
        .storage
        .load_protocol_transaction(&txid)
        .map_err(|error| format!("failed to load indexed transaction: {error}"))?
    {
        return Ok(Some(protocol_tx_response(
            &transaction,
            Some(location.block_height),
            Some(location.block_hash.into()),
            "confirmed",
        )));
    }
    let wtxid = WitnessTransactionHash(hash.0);
    if let Some((location, transaction)) =
        node.storage
            .load_protocol_transaction_by_wtxid(&wtxid)
            .map_err(|error| format!("failed to load indexed witness transaction: {error}"))?
    {
        return Ok(Some(protocol_tx_response(
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
            .load_protocol_transaction(&location.tx_hash)
            .map_err(|error| format!("failed to load indexed transaction: {error}"))?
        {
            transactions.push(protocol_tx_response(
                &transaction,
                Some(location.block_height),
                Some(location.block_hash.into()),
                "confirmed",
            ));
        }
    }

    for transaction in node.mempool.transactions_for_address(address) {
        transactions.push(protocol_tx_response(
            &SignedProtocolTransaction::Transfer(transaction.clone()),
            None,
            None,
            "pending",
        ));
    }
    for transaction in node.extension_mempool.transactions() {
        if protocol_transaction_addresses(transaction).contains(address) {
            transactions.push(protocol_tx_response(transaction, None, None, "pending"));
        }
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
            mined_supply = mined_supply.saturating_add(coinbase.subsidy.0);
            total_fees_collected = total_fees_collected.saturating_add(coinbase.fees.0);
            total_coinbase_rewards = total_coinbase_rewards.saturating_add(coinbase.total().0);
        }
        total_transactions = total_transactions.saturating_add(block.transaction_count() as u64);
        total_transaction_fees = total_transaction_fees
            .saturating_add(block.checked_total_fees().map(|fees| fees.0).unwrap_or(0));
        for transaction in &block.transactions {
            total_transfer_volume = total_transfer_volume.saturating_add(
                transaction
                    .transaction
                    .total_amount()
                    .map(|amount| amount.0)
                    .unwrap_or(0),
            );
        }
    }

    let pending_transactions = (node.mempool.len() + node.extension_mempool.len()) as u64;
    let average_transfer_amount = total_transfer_volume
        .checked_div(total_transactions)
        .unwrap_or(0);
    let current_supply = GENESIS_PREMINE.saturating_add(mined_supply);
    let average_block_time_secs = total_block_time_secs.checked_div(block_time_samples);

    Ok(ChainStatsResponse {
        chain: CHAIN_NAME,
        coin: COIN_NAME,
        height: tip,
        blocks,
        average_block_time_secs,
        target_block_time_secs: BLOCK_TIME,
        genesis_premine: GENESIS_PREMINE,
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
        "Paqus Node db::{}|p2p::{}|rpc::{}|height::{}|tip::{}|difficulty::{}|peers::{}|mining::{}|min_relay_fee_rate_per_byte::{}|base_market_fee_rate_per_byte::{}|dynamic_market_fee_rate_per_byte::{}|miner_min_fee_rate_per_byte::{}|low_fee_expiry::{}s|mempool_expiry::{}s",
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
        metrics: Arc::new(RpcMetrics::default()),
        db_path: service.config.db_path.clone(),
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
        "consensus: block_time::{}s|confirmation::{}|finality::{}|reward_maturity::{}|difficulty_start::{}|asert_half_life::{}s",
        BLOCK_TIME,
        CONFIRMATION_DEPTH,
        FINALITY_DEPTH,
        BLOCK_REWARD_MATURITY,
        DIFFICULTY_START,
        ASERT_HALF_LIFE
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
    config.miner_address = load_encrypted_wallet_address(path)?;
    Ok(())
}

#[derive(Deserialize)]
struct EncryptedMiningWalletFile {
    version: u8,
    address: String,
    kdf: String,
}

fn load_encrypted_wallet_address(path: &str) -> Result<Address, String> {
    let bytes = fs::read(path)
        .map_err(|error| format!("failed to read mining wallet `{path}`: {error}"))?;
    let wallet: EncryptedMiningWalletFile = serde_json::from_slice(&bytes)
        .map_err(|error| format!("invalid encrypted mining wallet `{path}`: {error}"))?;
    if wallet.version != 1 || wallet.kdf != "argon2id" {
        return Err(format!("unsupported encrypted mining wallet `{path}`"));
    }
    address_from_string(&wallet.address)
        .map_err(|_| format!("invalid address in mining wallet `{path}`"))
}

fn load_wallet(path: &str) -> Result<Wallet, String> {
    Err(format!(
        "refusing wallet file `{path}` in full-node: legacy files contain plaintext secrets; use wallet-cli for wallet operations"
    ))
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

    let parent_hash = BlockHash::from(candidate.previous_hash().as_hash());
    let started = Instant::now();
    let mined = mine_prepared_block(candidate, &consensus, mining_config)
        .map_err(|error| format!("mining failed: {error}"))?;
    let elapsed = started.elapsed();
    let Some(result) = mined else {
        update_mining_stats(mining_stats, mining_config.max_attempts, elapsed);
        println!(
            "mining batch:: |result::exhausted|start_nonce::{}|attempts::{}|",
            mining_config.start_nonce, mining_config.max_attempts
        );
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

fn signed_ecash_transaction_from_hex(value: &str) -> Result<SignedEcashTransaction, String> {
    let bytes = hex::decode(value).map_err(|_| "invalid eCash transaction hex".to_string())?;
    SignedEcashTransaction::try_from_slice(&bytes)
        .map_err(|error| format!("invalid signed eCash transaction bytes: {error}"))
}

fn signed_protocol_transaction_from_hex(value: &str) -> Result<SignedProtocolTransaction, String> {
    let bytes = hex::decode(value).map_err(|_| "invalid protocol transaction hex".to_string())?;
    SignedProtocolTransaction::try_from_slice(&bytes)
        .map_err(|error| format!("invalid signed protocol transaction bytes: {error}"))
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
  paqus node db check [db-path]
  paqus node db backup <db-path> <backup-path>
  paqus node db restore <backup-path> <db-path>
  paqus node run [db-path] [--config path] [--listen addr] [--rpc-listen addr] [--peer addr] [--peers-file path] [--gateway host:port] [--public-addr host:port] [--min-relay-fee paqus-per-byte] [--market-fee paqus-per-byte] [--miner-min-fee-rate paqus-per-byte] [--low-fee-expiry-secs n] [--mempool-expiry-secs n] [--wallet path] [--miner address] [--miner-secret-key key-hex] [--mine]
  paqus wallet new [wallet-path] [--show-secret]
  paqus wallet address <secret-key-hex>
  paqus wallet balance <address> [db-path]
  paqus wallet pay <address> <amount> [--wallet path] [--fee units] [--rpc addr]
  paqus wallet send <address> <amount> [--wallet path] [--nonce n] [--fee units] [--rpc addr]
  paqus wallet send --wallet path --to address --amount units [--nonce n] [--fee units] [--submit] [--rpc addr]

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
  GET  /ecash/mempool
  POST /tx              JSON: {{\"tx\":\"signed-transaction-hex\"}}
  POST /ecash/tx        JSON: {{\"tx\":\"signed-ecash-transaction-hex\"}}

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
    let swarm = p2p::libp2p::build_swarm()?;
    println!("peer_id: {}", swarm.local_peer_id());
    println!("block_topic: {}", p2p::libp2p::PAQUS_BLOCK_TOPIC);
    println!("tx_topic: {}", p2p::libp2p::PAQUS_TX_TOPIC);
    println!("request_protocol: {}", p2p::libp2p::PAQUS_REQUEST_PROTOCOL);
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
    fn mining_reads_only_address_from_encrypted_wallet() {
        let address = Address([0xcd; 20]);
        let path = std::env::temp_dir().join(format!(
            "paqus-mining-wallet-address-only-{}.json",
            std::process::id()
        ));
        let contents = serde_json::json!({
            "version": 1,
            "address": address_to_string(&address),
            "public_key": "not-read-by-node",
            "kdf": "argon2id",
            "salt": "not-read-by-node",
            "nonce": "not-read-by-node",
            "ciphertext": "not-read-by-node"
        });
        fs::write(&path, serde_json::to_vec(&contents).unwrap()).unwrap();

        assert_eq!(
            load_encrypted_wallet_address(path.to_str().unwrap()),
            Ok(address)
        );
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn parse_run_config_accepts_pasted_flags_with_surrounding_spaces() {
        let config = parse_run_config(&args(&[
            "--config",
            "/tmp/full-node-missing-test-config.json",
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
        assert_eq!(config.peers.len(), 2);
        assert_eq!(config.public_addrs.len(), 1);
        assert_eq!(config.rpc_addr, "127.0.0.1:6666".parse().unwrap());
        assert!(config.mine);
        assert_eq!(config.mine_attempts, 100000);
    }

    #[test]
    fn run_config_defaults_to_local_rpc_without_bootstrap_peer() {
        let config = RunConfig::default();

        assert_eq!(config.rpc_addr, "127.0.0.1:6666".parse().unwrap());
        assert!(config.peers.is_empty());
    }

    #[test]
    fn database_backup_restore_roundtrip_and_refuse_overwrite() {
        let root = std::env::temp_dir().join(format!(
            "paqus-db-ops-{}-{}",
            std::process::id(),
            unix_timestamp().unwrap()
        ));
        let source = root.join("source");
        let backup = root.join("backup");
        let restored = root.join("restored");
        let _ = fs::remove_dir_all(&root);

        let source_node = open_node(source.to_string_lossy().as_ref(), Address([9; 20])).unwrap();
        let expected_tip = source_node.tip_hash();
        drop(source_node);
        backup_node_database(
            source.to_string_lossy().as_ref(),
            backup.to_string_lossy().as_ref(),
        )
        .unwrap();
        assert!(
            backup_node_database(
                source.to_string_lossy().as_ref(),
                backup.to_string_lossy().as_ref(),
            )
            .is_err()
        );
        restore_node_database(
            backup.to_string_lossy().as_ref(),
            restored.to_string_lossy().as_ref(),
        )
        .unwrap();
        let restored_node =
            open_node(restored.to_string_lossy().as_ref(), Address([9; 20])).unwrap();
        assert_eq!(restored_node.tip_hash(), expected_tip);
        drop(restored_node);

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn protocol_event_query_filters_height_kind_and_paginates() {
        let owner = Address([1; 20]);
        let events = vec![
            ProtocolEvent::new(
                Height(3),
                BlockHash([3; 32]),
                None,
                0,
                ProtocolEventKind::CoinbasePaid {
                    miner: owner,
                    subsidy: Amount(1),
                    fees: Amount(0),
                },
            ),
            ProtocolEvent::new(
                Height(4),
                BlockHash([4; 32]),
                None,
                0,
                ProtocolEventKind::EcashWithdrawn {
                    signer: owner,
                    amount: Amount(1),
                },
            ),
            ProtocolEvent::new(
                Height(5),
                BlockHash([5; 32]),
                None,
                0,
                ProtocolEventKind::CoinbasePaid {
                    miner: owner,
                    subsidy: Amount(1),
                    fees: Amount(0),
                },
            ),
        ];

        let response = protocol_event_list(
            events,
            EventQuery {
                offset: Some(1),
                limit: Some(1),
                kind: Some("coinbase_paid".to_string()),
                from_height: Some(3),
                to_height: Some(5),
            },
        )
        .unwrap();

        assert_eq!(response.total, 2);
        assert_eq!(response.events.len(), 1);
        assert_eq!(response.events[0].event.block_height, Height(5));
    }

    #[test]
    fn protocol_event_query_rejects_invalid_limits_and_kinds() {
        assert!(matches!(
            protocol_event_list(
                vec![],
                EventQuery {
                    limit: Some(0),
                    ..EventQuery::default()
                }
            ),
            Err("event_limit_must_be_between_1_and_500")
        ));
        assert!(matches!(
            protocol_event_list(
                vec![],
                EventQuery {
                    kind: Some("unknown".to_string()),
                    ..EventQuery::default()
                }
            ),
            Err("unknown_event_kind")
        ));
    }

    #[test]
    fn event_stream_waits_for_finality_depth() {
        assert_eq!(finalized_event_height(Some(Height(1))), None);
        assert_eq!(
            finalized_event_height(Some(Height(u64::from(FINALITY_DEPTH)))),
            Some(0)
        );
        assert_eq!(
            finalized_event_height(Some(Height(u64::from(FINALITY_DEPTH) + 7))),
            Some(7)
        );
    }

    #[test]
    fn event_stream_address_filter_covers_transfer_participants() {
        let sender = Address([7; 20]);
        let recipient = Address([8; 20]);
        let unrelated = Address([9; 20]);
        let event = ProtocolEventKind::Transfer {
            from: sender,
            to: recipient,
            amount: Amount(10),
            fee: Amount(1),
        };

        assert!(protocol_event_involves_address(&event, &sender));
        assert!(protocol_event_involves_address(&event, &recipient));
        assert!(!protocol_event_involves_address(&event, &unrelated));
    }

    #[test]
    fn generic_explorer_response_exposes_txid_wtxid_and_witness_identity() {
        let keypair = paqus::crypto::generate_keypair();
        let signer = address_from_public_key(&keypair.public_key);
        let payload = Transaction::new(signer, Address([2; 20]), Amount(10), Amount(2), Nonce(3));
        let signature = paqus::crypto::sign(&keypair.secret_key, &payload.signing_bytes());
        let transaction = SignedProtocolTransaction::Transfer(SignedTransaction::new(
            payload,
            keypair.public_key,
            signature,
        ));

        let response = protocol_tx_response(&transaction, Some(Height(7)), None, "confirmed");
        let json = serde_json::to_value(response).unwrap();

        assert_eq!(json["family"], "transfer");
        assert_eq!(json["operation"], "transfer");
        assert_eq!(json["txid"], hex::encode(transaction.hash().0));
        assert_eq!(json["wtxid"], hex::encode(transaction.wtxid().0));
        assert_eq!(json["signer"], address_to_string(&signer));
        assert_eq!(json["witness_addresses"][0], address_to_string(&signer));
        assert_eq!(json["block_height"], 7);
        assert!(
            json["virtual_size"].as_u64().unwrap()
                < json["stripped_size"].as_u64().unwrap() + json["witness_size"].as_u64().unwrap()
        );
    }
}
