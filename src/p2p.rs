use crate::network::roundtrip;
use crate::paquscore::{BlockHash, Height, NetworkMessage, Node, PeerInfo, TipInfo, VersionInfo};
use std::collections::HashSet;
use std::fs;
use std::io;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

const DEFAULT_SYNC_INTERVAL: Duration = Duration::from_secs(5);
const PEER_RETRY_BASE: Duration = Duration::from_secs(2);
const PEER_RETRY_MAX: Duration = Duration::from_secs(60);
const MAX_BLOCKS_PER_SYNC: u64 = 64;

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
    Idle,
    Synced,
}

pub fn load_peers_file(path: &str) -> Result<Vec<SocketAddr>, String> {
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

pub fn dedupe_peers(peers: &mut Vec<SocketAddr>) {
    let mut seen = HashSet::new();
    peers.retain(|peer| seen.insert(*peer));
}

pub fn poll_peer(peer: SocketAddr, node: &mut Node) -> Result<PeerPoll, String> {
    handshake_peer(peer, node)?;
    let tip = request_tip(peer)?;
    let local_height = node.tip_height().unwrap_or(Height(0)).0;
    if tip.0 <= local_height {
        return if request_missing_parent_blocks(peer, node)? {
            Ok(PeerPoll::Synced)
        } else {
            Ok(PeerPoll::Idle)
        };
    }

    let target = tip.0.min(local_height.saturating_add(MAX_BLOCKS_PER_SYNC));
    for height in local_height.saturating_add(1)..=target {
        request_block(peer, node, Height(height))?;
    }
    request_missing_parent_blocks(peer, node)?;
    Ok(PeerPoll::Synced)
}

pub fn request_peers(peer: SocketAddr) -> Result<Vec<PeerInfo>, String> {
    match roundtrip(peer, NetworkMessage::GetPeers)? {
        NetworkMessage::Peers(peers) => Ok(peers),
        _ => Err("peer returned unexpected peers response".to_string()),
    }
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
        "synced block height {} from {} |tip::{}|",
        height.0,
        peer,
        node.tip_hash()
            .map(|hash| hex::encode(hash.0))
            .unwrap_or_else(|| "none".to_string())
    );
    Ok(())
}

fn request_missing_parent_blocks(peer: SocketAddr, node: &mut Node) -> Result<bool, String> {
    let mut requested = false;
    for hash in node.drain_missing_parent_requests() {
        requested = true;
        if let Err(error) = request_block_by_hash(peer, node, hash) {
            node.retry_missing_parent_request(hash);
            return Err(error);
        }
    }
    Ok(requested)
}

fn request_block_by_hash(peer: SocketAddr, node: &mut Node, hash: BlockHash) -> Result<(), String> {
    let response = roundtrip(peer, NetworkMessage::GetBlockByHash { hash })?;
    let NetworkMessage::Block(block) = response else {
        return Err(format!(
            "peer did not return block hash {}",
            hex::encode(hash.0)
        ));
    };
    node.apply_block(block)
        .map_err(|error| format!("failed to apply missing parent from {peer}: {error}"))?;
    node.flush_to_storage()
        .map_err(|error| format!("failed to flush missing parent block: {error}"))?;
    println!(
        "synced missing parent {} from {} |tip::{}|",
        hex::encode(hash.0),
        peer,
        node.tip_hash()
            .map(|hash| hex::encode(hash.0))
            .unwrap_or_else(|| "none".to_string())
    );
    Ok(())
}
