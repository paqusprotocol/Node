use crate::network::{configure_stream, connect_peer, request_on_stream};
use crate::paquscore::{BlockHash, Height, NetworkMessage, Node, PeerInfo, TipInfo, VersionInfo};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::fs;
use std::io;
use std::net::{SocketAddr, TcpStream};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const DEFAULT_SYNC_INTERVAL: Duration = Duration::from_secs(5);
pub const PERSISTENT_PEER_TIMEOUT: Duration = Duration::from_secs(30);
const PEER_RETRY_BASE: Duration = Duration::from_secs(2);
const PEER_RETRY_MAX: Duration = Duration::from_secs(60);
const MAX_BLOCKS_PER_SYNC: u64 = 64;

#[derive(Debug, Serialize, Deserialize)]
struct PeerCache {
    peers: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct PeerState {
    pub addr: SocketAddr,
    pub failures: u32,
    pub next_attempt: Instant,
    pub last_tip: Option<Height>,
}

impl PeerState {
    pub fn new(addr: SocketAddr) -> Self {
        Self {
            addr,
            failures: 0,
            next_attempt: Instant::now(),
            last_tip: None,
        }
    }

    pub fn mark_ok(&mut self, tip: Option<Height>) {
        self.failures = 0;
        self.last_tip = tip;
        self.next_attempt = Instant::now() + DEFAULT_SYNC_INTERVAL;
    }

    pub fn mark_failed(&mut self) {
        self.failures = self.failures.saturating_add(1);
        let shift = self.failures.saturating_sub(1).min(5);
        let secs = PEER_RETRY_BASE
            .as_secs()
            .saturating_mul(1_u64 << shift)
            .min(PEER_RETRY_MAX.as_secs());
        self.next_attempt = Instant::now() + Duration::from_secs(secs);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeerPoll {
    Idle { remote_tip: Height },
    Synced { remote_tip: Height },
}

pub struct PeerConnection {
    addr: SocketAddr,
    stream: TcpStream,
}

impl PeerConnection {
    pub fn connect(addr: SocketAddr) -> Result<Self, String> {
        let stream = connect_peer(addr)?;
        configure_stream(&stream, PERSISTENT_PEER_TIMEOUT)?;
        Ok(Self { addr, stream })
    }

    pub fn from_stream(addr: SocketAddr, stream: TcpStream) -> Result<Self, String> {
        configure_stream(&stream, PERSISTENT_PEER_TIMEOUT)?;
        Ok(Self { addr, stream })
    }

    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    fn request(&mut self, message: NetworkMessage) -> Result<NetworkMessage, String> {
        request_on_stream(&mut self.stream, message)
    }

    pub fn send(&mut self, message: NetworkMessage) -> Result<(), String> {
        crate::paquscore::write_message(&mut self.stream, &message.to_envelope())
            .map_err(|error| format!("send failed: {error}"))
    }
}

pub fn load_peers_file(path: &str) -> Result<Vec<SocketAddr>, String> {
    let contents = match fs::read_to_string(path) {
        Ok(contents) => contents,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(format!("failed to read peers file {path}: {error}")),
    };
    if contents.trim_start().starts_with('{') {
        let cache = serde_json::from_str::<PeerCache>(&contents)
            .map_err(|error| format!("failed to parse peer cache {path}: {error}"))?;
        return cache
            .peers
            .into_iter()
            .map(|peer| {
                peer.parse()
                    .map_err(|error| format!("invalid peer `{peer}` in {path}: {error}"))
            })
            .collect();
    }

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

pub fn save_peers_file(path: &str, peers: Vec<SocketAddr>) -> Result<(), String> {
    let cache = PeerCache {
        peers: peers
            .into_iter()
            .map(|peer| peer.to_string())
            .collect::<Vec<_>>(),
    };
    let contents = serde_json::to_string_pretty(&cache)
        .map_err(|error| format!("failed to encode peer cache {path}: {error}"))?;
    fs::write(path, contents).map_err(|error| format!("failed to write peers file {path}: {error}"))
}

pub fn dedupe_peers(peers: &mut Vec<SocketAddr>) {
    let mut seen = HashSet::new();
    peers.retain(|peer| seen.insert(*peer));
}

pub fn poll_peer(
    peer: SocketAddr,
    node: &Arc<Mutex<Node>>,
    public_addrs: &[SocketAddr],
) -> Result<PeerPoll, String> {
    let mut peer = PeerConnection::connect(peer)?;
    poll_peer_connection(&mut peer, node, public_addrs)
}

pub fn poll_peer_connection(
    peer: &mut PeerConnection,
    node: &Arc<Mutex<Node>>,
    public_addrs: &[SocketAddr],
) -> Result<PeerPoll, String> {
    handshake_peer(peer, node, public_addrs)?;
    let tip = request_tip(peer)?;
    let local_height = node
        .lock()
        .map_err(|_| "node state lock poisoned".to_string())?
        .tip_height()
        .unwrap_or(Height(0))
        .0;
    if tip.0 <= local_height {
        return if request_missing_parent_blocks(peer, node)? {
            Ok(PeerPoll::Synced { remote_tip: tip })
        } else {
            Ok(PeerPoll::Idle { remote_tip: tip })
        };
    }

    let target = tip.0.min(local_height.saturating_add(MAX_BLOCKS_PER_SYNC));
    for height in local_height.saturating_add(1)..=target {
        request_block(peer, node, Height(height))?;
    }
    request_missing_parent_blocks(peer, node)?;
    Ok(PeerPoll::Synced { remote_tip: tip })
}

pub fn request_peers_connection(peer: &mut PeerConnection) -> Result<Vec<PeerInfo>, String> {
    match peer.request(NetworkMessage::GetPeers)? {
        NetworkMessage::Peers(peers) => Ok(peers),
        _ => Err("peer returned unexpected peers response".to_string()),
    }
}

fn handshake_peer(
    peer: &mut PeerConnection,
    node: &Arc<Mutex<Node>>,
    public_addrs: &[SocketAddr],
) -> Result<(), String> {
    let tip = {
        let node = node
            .lock()
            .map_err(|_| "node state lock poisoned".to_string())?;
        node.tip_height()
            .zip(node.tip_hash())
            .map(|(height, hash)| TipInfo { height, hash })
    };
    let version = VersionInfo::local(tip);
    match peer.request(NetworkMessage::Version(version))? {
        NetworkMessage::VerAck(remote) => {
            remote
                .validate_compatibility()
                .map_err(|reason| format!("peer returned incompatible version: {reason:?}"))?;
            if !public_addrs.is_empty() {
                peer.send(NetworkMessage::Peers(
                    public_addrs
                        .iter()
                        .map(|addr| PeerInfo {
                            address: addr.to_string(),
                        })
                        .collect(),
                ))?;
            }
            Ok(())
        }
        NetworkMessage::Reject { reason, message } => {
            Err(format!("peer rejected handshake: {reason:?}: {message}"))
        }
        _ => Err("peer returned unexpected handshake response".to_string()),
    }
}

fn request_tip(peer: &mut PeerConnection) -> Result<Height, String> {
    match peer.request(NetworkMessage::GetTip)? {
        NetworkMessage::Tip(tip) => Ok(tip.height),
        _ => Err("peer returned unexpected tip response".to_string()),
    }
}

fn request_block(
    peer: &mut PeerConnection,
    node: &Arc<Mutex<Node>>,
    height: Height,
) -> Result<(), String> {
    let response = peer.request(NetworkMessage::GetBlockByHeight { height })?;
    let NetworkMessage::Block(block) = response else {
        return Err(format!("peer did not return block at height {}", height.0));
    };
    let mut node = node
        .lock()
        .map_err(|_| "node state lock poisoned".to_string())?;
    node.apply_block(block).map_err(|error| {
        format!(
            "failed to apply block {} from {}: {error}",
            height.0,
            peer.addr()
        )
    })?;
    node.flush_to_storage()
        .map_err(|error| format!("failed to flush synced block: {error}"))?;
    println!(
        "synced block height {} from {} |tip::{}|",
        height.0,
        peer.addr(),
        node.tip_hash()
            .map(|hash| hex::encode(hash.0))
            .unwrap_or_else(|| "none".to_string())
    );
    Ok(())
}

fn request_missing_parent_blocks(
    peer: &mut PeerConnection,
    node: &Arc<Mutex<Node>>,
) -> Result<bool, String> {
    let mut requested = false;
    let hashes = node
        .lock()
        .map_err(|_| "node state lock poisoned".to_string())?
        .drain_missing_parent_requests();
    for hash in hashes {
        requested = true;
        if let Err(error) = request_block_by_hash(peer, node, hash) {
            node.lock()
                .map_err(|_| "node state lock poisoned".to_string())?
                .retry_missing_parent_request(hash);
            return Err(error);
        }
    }
    Ok(requested)
}

fn request_block_by_hash(
    peer: &mut PeerConnection,
    node: &Arc<Mutex<Node>>,
    hash: BlockHash,
) -> Result<(), String> {
    let response = peer.request(NetworkMessage::GetBlockByHash { hash })?;
    let NetworkMessage::Block(block) = response else {
        return Err(format!(
            "peer did not return block hash {}",
            hex::encode(hash.0)
        ));
    };
    let mut node = node
        .lock()
        .map_err(|_| "node state lock poisoned".to_string())?;
    node.apply_block(block).map_err(|error| {
        format!(
            "failed to apply missing parent from {}: {error}",
            peer.addr()
        )
    })?;
    node.flush_to_storage()
        .map_err(|error| format!("failed to flush missing parent block: {error}"))?;
    println!(
        "synced missing parent {} from {} |tip::{}|",
        hex::encode(hash.0),
        peer.addr(),
        node.tip_hash()
            .map(|hash| hex::encode(hash.0))
            .unwrap_or_else(|| "none".to_string())
    );
    Ok(())
}
