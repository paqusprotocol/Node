use crate::command::parse::{address, address_string, secret_key};
use crate::runtime;
use paqus::crypto::{Address, SecretKey};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::fs;
use std::io;
use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;

const DEFAULT_NODE_DB: &str = "./data/paqus";
const DEFAULT_LISTEN_ADDR: &str = "[::]:5555";
const DEFAULT_RPC_ADDR: &str = "127.0.0.1:6666";
const DEFAULT_CONFIG_FILE: &str = "./data/paqus/node.json";
const DEFAULT_PEERS_FILE: &str = "./data/paqus/peers.json";
const DEFAULT_SHUTDOWN_FILE: &str = "./data/paqus/STOP";
const DEFAULT_MAX_PEERS: usize = 128;
const DEFAULT_GATEWAY_HEARTBEAT: Duration = Duration::from_secs(60);

#[derive(Debug)]
pub struct RunConfig {
    pub db_path: String,
    pub listen_addrs: Vec<SocketAddr>,
    pub rpc_addr: SocketAddr,
    pub peers: Vec<SocketAddr>,
    pub peers_file: Option<String>,
    pub gateway_url: Option<String>,
    pub public_addrs: Vec<SocketAddr>,
    pub gateway_heartbeat: Duration,
    pub shutdown_file: String,
    pub max_peers: usize,
    pub min_relay_fee: u64,
    pub market_fee: u64,
    pub low_fee_expiry: Duration,
    pub mempool_expiry: Duration,
    pub miner_address: Address,
    pub miner_secret_key: Option<SecretKey>,
    pub miner_min_fee_rate: Option<u64>,
    pub mine: bool,
    pub mine_interval: Duration,
    pub mine_attempts: u64,
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

impl Default for RunConfig {
    fn default() -> Self {
        Self {
            db_path: DEFAULT_NODE_DB.to_string(),
            listen_addrs: vec![
                DEFAULT_LISTEN_ADDR
                    .parse()
                    .expect("valid default listen address"),
            ],
            rpc_addr: DEFAULT_RPC_ADDR.parse().expect("valid default rpc address"),
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
            mine_interval: Duration::ZERO,
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

pub fn write_default(path: &str) -> Result<(), String> {
    if let Some(parent) = Path::new(path).parent() {
        fs::create_dir_all(parent)
            .map_err(|error| format!("failed to create config directory: {error}"))?;
    }
    let contents = serde_json::to_string_pretty(&RunConfigFile::default())
        .map_err(|error| format!("failed to encode default config: {error}"))?;
    fs::write(path, contents).map_err(|error| format!("failed to write config {path}: {error}"))
}

pub fn parse(args: &[String]) -> Result<RunConfig, String> {
    let args = args
        .iter()
        .map(|arg| arg.trim().to_string())
        .collect::<Vec<_>>();
    let mut config = RunConfig::default();
    let config_path = config_path_arg(&args).unwrap_or(DEFAULT_CONFIG_FILE);
    if let Some(file_config) = load_file(config_path)? {
        apply_file(&mut config, file_config)?;
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
                config.db_path = required(&args, index, "--db")?.clone();
            }
            "--listen" => {
                index += 1;
                if !listen_overridden {
                    config.listen_addrs.clear();
                    listen_overridden = true;
                }
                config
                    .listen_addrs
                    .push(socket(args.get(index), "--listen")?);
            }
            "--rpc-listen" => {
                index += 1;
                config.rpc_addr = socket(args.get(index), "--rpc-listen")?;
            }
            "--peer" => {
                index += 1;
                config.peers.push(socket(args.get(index), "--peer")?);
            }
            "--peers-file" => {
                index += 1;
                config.peers_file = Some(required(&args, index, "--peers-file")?.clone());
            }
            "--gateway" | "--gateway-url" => {
                index += 1;
                config.gateway_url = Some(required(&args, index, "--gateway")?.clone());
            }
            "--public-addr" => {
                index += 1;
                if !public_overridden {
                    config.public_addrs.clear();
                    public_overridden = true;
                }
                config
                    .public_addrs
                    .push(socket(args.get(index), "--public-addr")?);
            }
            "--gateway-heartbeat-secs" => {
                index += 1;
                config.gateway_heartbeat =
                    Duration::from_secs(number(&args, index, "--gateway-heartbeat-secs")?.max(1));
            }
            "--shutdown-file" => {
                index += 1;
                config.shutdown_file = required(&args, index, "--shutdown-file")?.clone();
            }
            "--max-peers" => {
                index += 1;
                config.max_peers = number(&args, index, "--max-peers")? as usize;
                config.max_peers = config.max_peers.max(1);
            }
            "--min-relay-fee" => {
                index += 1;
                config.min_relay_fee = number(&args, index, "--min-relay-fee")?
                    .max(runtime::params::MIN_RELAY_FEE_FLOOR);
            }
            "--market-fee" => {
                index += 1;
                config.market_fee = number(&args, index, "--market-fee")?;
            }
            "--low-fee-expiry-secs" => {
                index += 1;
                config.low_fee_expiry =
                    Duration::from_secs(number(&args, index, "--low-fee-expiry-secs")?.max(1));
            }
            "--mempool-expiry-secs" => {
                index += 1;
                config.mempool_expiry =
                    Duration::from_secs(number(&args, index, "--mempool-expiry-secs")?.max(1));
            }
            "--miner" => {
                index += 1;
                config.miner_address = address(args.get(index))?;
            }
            "--wallet" => {
                index += 1;
                config.miner_address =
                    encrypted_wallet_address(required(&args, index, "--wallet")?)?;
            }
            "--miner-secret-key" => {
                index += 1;
                config.miner_secret_key = Some(secret_key(args.get(index))?);
            }
            "--miner-min-fee-rate" => {
                index += 1;
                config.miner_min_fee_rate = Some(number(&args, index, "--miner-min-fee-rate")?);
            }
            "--premine" => {
                return Err(
                    "premine address is fixed by protocol and cannot be overridden".to_string(),
                );
            }
            "--mine" => config.mine = true,
            "--mine-interval-secs" => {
                index += 1;
                config.mine_interval =
                    Duration::from_secs(number(&args, index, "--mine-interval-secs")?);
            }
            "--mine-attempts" => {
                index += 1;
                config.mine_attempts = number(&args, index, "--mine-attempts")?;
            }
            value if !value.starts_with('-') && config.db_path == DEFAULT_NODE_DB => {
                config.db_path = value.to_string()
            }
            value => return Err(format!("unknown node run option `{value}`")),
        }
        index += 1;
    }
    dedupe(&mut config.listen_addrs);
    dedupe(&mut config.public_addrs);
    normalize(&mut config);
    Ok(config)
}

pub fn format_socket_addrs(addrs: &[SocketAddr]) -> String {
    addrs
        .iter()
        .map(SocketAddr::to_string)
        .collect::<Vec<_>>()
        .join(",")
}

fn required<'a>(args: &'a [String], index: usize, flag: &str) -> Result<&'a String, String> {
    args.get(index)
        .ok_or_else(|| format!("missing value for {flag}"))
}

fn number(args: &[String], index: usize, flag: &str) -> Result<u64, String> {
    required(args, index, flag)?
        .parse::<u64>()
        .map_err(|error| format!("invalid {flag}: {error}"))
}

fn socket(value: Option<&String>, flag: &str) -> Result<SocketAddr, String> {
    value
        .ok_or_else(|| format!("missing value for {flag}"))?
        .parse()
        .map_err(|error| format!("invalid socket address for {flag}: {error}"))
}

fn config_path_arg(args: &[String]) -> Option<&str> {
    args.windows(2)
        .find_map(|window| (window[0] == "--config").then_some(window[1].as_str()))
}

fn load_file(path: &str) -> Result<Option<RunConfigFile>, String> {
    let contents = match fs::read_to_string(path) {
        Ok(contents) => contents,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(format!("failed to read config {path}: {error}")),
    };
    serde_json::from_str(&contents)
        .map(Some)
        .map_err(|error| format!("failed to parse config {path}: {error}"))
}

fn apply_file(config: &mut RunConfig, file: RunConfigFile) -> Result<(), String> {
    config.db_path = file.db_path;
    config.listen_addrs = parse_sockets(file.listen_addr.into_vec(), "listen_addr")?;
    config.rpc_addr = file
        .rpc_addr
        .parse()
        .map_err(|error| format!("invalid rpc_addr in config: {error}"))?;
    config.peers = parse_sockets(file.peers, "peer")?;
    config.peers_file = file.peers_file;
    config.gateway_url = file.gateway_url;
    config.public_addrs = parse_sockets(
        file.public_addr
            .map(OneOrMany::into_vec)
            .unwrap_or_default(),
        "public_addr",
    )?;
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
    if let Some(path) = file.wallet {
        config.miner_address = encrypted_wallet_address(&path)?;
    }
    if let Some(value) = file.miner_address {
        config.miner_address = address(Some(&value))?;
    }
    if let Some(value) = file.miner_secret_key {
        config.miner_secret_key = Some(secret_key(Some(&value))?);
    }
    config.miner_min_fee_rate = file.miner_min_fee_rate;
    Ok(())
}

fn parse_sockets(values: Vec<String>, label: &str) -> Result<Vec<SocketAddr>, String> {
    values
        .into_iter()
        .map(|value| {
            value
                .parse()
                .map_err(|error| format!("invalid {label} `{value}` in config: {error}"))
        })
        .collect()
}

pub fn dedupe(addrs: &mut Vec<SocketAddr>) {
    let mut seen = HashSet::new();
    addrs.retain(|addr| seen.insert(*addr));
}

fn normalize(config: &mut RunConfig) {
    config.min_relay_fee = config
        .min_relay_fee
        .max(runtime::params::MIN_RELAY_FEE_FLOOR);
    config.market_fee = config.market_fee.max(config.min_relay_fee);
    if config.low_fee_expiry > config.mempool_expiry {
        config.low_fee_expiry = config.mempool_expiry;
    }
}

#[derive(Deserialize)]
struct EncryptedMiningWalletFile {
    version: u8,
    address: String,
    kdf: String,
}

pub fn encrypted_wallet_address(path: &str) -> Result<Address, String> {
    let bytes = fs::read(path)
        .map_err(|error| format!("failed to read mining wallet `{path}`: {error}"))?;
    let wallet: EncryptedMiningWalletFile = serde_json::from_slice(&bytes)
        .map_err(|error| format!("invalid encrypted mining wallet `{path}`: {error}"))?;
    if wallet.version != 1 || wallet.kdf != "argon2id" {
        return Err(format!("unsupported encrypted mining wallet `{path}`"));
    }
    address_string(&wallet.address)
        .map_err(|_| format!("invalid address in mining wallet `{path}`"))
}
