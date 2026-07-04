use crate::network::{configure_stream, connect_peer, request_on_stream};
use crate::paquscore::{
    BlockHash, Height, InventoryItem, NetworkMessage, Node, PeerInfo, TipInfo, VersionInfo,
};
use paqus::block::{Block, BlockHeader};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashSet};
use std::fs;
use std::io;
use std::net::{SocketAddr, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

const DEFAULT_SYNC_INTERVAL: Duration = Duration::from_secs(5);
pub const PERSISTENT_PEER_TIMEOUT: Duration = Duration::from_secs(30);
const PEER_RETRY_BASE: Duration = Duration::from_secs(2);
const PEER_RETRY_MAX: Duration = Duration::from_secs(60);
const MIN_BLOCKS_PER_SYNC: u64 = 32;
const INITIAL_BLOCKS_PER_SYNC: u64 = 64;
const MAX_BLOCKS_PER_SYNC: u64 = 256;
const MAX_BLOCKS_PER_BATCH: u64 = 32;
const MAX_BLOCK_LOCATOR_HASHES: usize = 32;

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
    pub sync_window: u64,
}

impl PeerState {
    pub fn new(addr: SocketAddr) -> Self {
        Self {
            addr,
            failures: 0,
            next_attempt: Instant::now(),
            last_tip: None,
            sync_window: INITIAL_BLOCKS_PER_SYNC,
        }
    }

    pub fn mark_ok(&mut self, tip: Option<Height>) {
        self.failures = 0;
        self.last_tip = tip;
        self.next_attempt = Instant::now() + DEFAULT_SYNC_INTERVAL;
    }

    pub fn mark_synced(&mut self, tip: Height, synced_blocks: usize) {
        self.failures = 0;
        self.last_tip = Some(tip);
        if synced_blocks as u64 >= self.sync_window {
            self.sync_window = self.sync_window.saturating_mul(2).min(MAX_BLOCKS_PER_SYNC);
        }
        self.next_attempt = Instant::now() + DEFAULT_SYNC_INTERVAL;
    }

    pub fn mark_failed(&mut self) {
        self.failures = self.failures.saturating_add(1);
        self.sync_window = self.sync_window.saturating_div(2).max(MIN_BLOCKS_PER_SYNC);
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
    Idle {
        remote_tip: Height,
    },
    Synced {
        remote_tip: Height,
        synced_blocks: usize,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParallelSyncReport {
    pub remote_tip: Height,
    pub applied_blocks: usize,
    pub used_peers: usize,
    pub used_peer_addrs: Vec<SocketAddr>,
    pub failed_peer_addrs: Vec<SocketAddr>,
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

    pub fn request(&mut self, message: NetworkMessage) -> Result<NetworkMessage, String> {
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

pub fn poll_peer_connection(
    peer: &mut PeerConnection,
    node: &Arc<Mutex<Node>>,
    public_addrs: &[SocketAddr],
    sync_window: u64,
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
            Ok(PeerPoll::Synced {
                remote_tip: tip,
                synced_blocks: 0,
            })
        } else {
            Ok(PeerPoll::Idle { remote_tip: tip })
        };
    }

    let ancestor = request_common_ancestor(peer, node)?;
    let sync_window = sync_window.clamp(MIN_BLOCKS_PER_SYNC, MAX_BLOCKS_PER_SYNC);
    let target = tip.0.min(ancestor.height.0.saturating_add(sync_window));
    if target <= ancestor.height.0 {
        return Ok(PeerPoll::Idle { remote_tip: tip });
    }
    let start = Height(ancestor.height.0.saturating_add(1));
    let headers = request_headers(peer, start, target, ancestor.hash)?;
    request_blocks(peer, node, start, target, headers)?;
    request_missing_parent_blocks(peer, node)?;
    Ok(PeerPoll::Synced {
        remote_tip: tip,
        synced_blocks: target.saturating_sub(start.0).saturating_add(1) as usize,
    })
}

pub fn sync_from_peers_parallel(
    peers: Vec<SocketAddr>,
    node: &Arc<Mutex<Node>>,
    public_addrs: &[SocketAddr],
    sync_window: u64,
) -> Result<ParallelSyncReport, String> {
    let local_height = node
        .lock()
        .map_err(|_| "node state lock poisoned".to_string())?
        .tip_height()
        .unwrap_or(Height(0))
        .0;

    let mut candidates = Vec::new();
    for addr in peers {
        let mut peer = match PeerConnection::connect(addr) {
            Ok(peer) => peer,
            Err(error) => {
                eprintln!("parallel sync candidate {addr} connect failed: {error}");
                continue;
            }
        };
        if let Err(error) = handshake_peer(&mut peer, node, public_addrs) {
            eprintln!("parallel sync candidate {addr} handshake failed: {error}");
            continue;
        }
        match request_tip(&mut peer) {
            Ok(tip) if tip.0 > local_height => candidates.push((addr, tip)),
            Ok(_) => {}
            Err(error) => eprintln!("parallel sync candidate {addr} tip failed: {error}"),
        }
    }

    let Some((leader, remote_tip)) = candidates.iter().copied().max_by_key(|(_, tip)| tip.0) else {
        return Ok(ParallelSyncReport {
            remote_tip: Height(local_height),
            applied_blocks: 0,
            used_peers: 0,
            used_peer_addrs: Vec::new(),
            failed_peer_addrs: Vec::new(),
        });
    };

    let mut leader_connection = PeerConnection::connect(leader)?;
    handshake_peer(&mut leader_connection, node, public_addrs)?;
    let ancestor = request_common_ancestor(&mut leader_connection, node)?;
    let sync_window = sync_window.clamp(MIN_BLOCKS_PER_SYNC, MAX_BLOCKS_PER_SYNC);
    let target = remote_tip
        .0
        .min(ancestor.height.0.saturating_add(sync_window));
    if target <= ancestor.height.0 {
        return Ok(ParallelSyncReport {
            remote_tip,
            applied_blocks: 0,
            used_peers: 0,
            used_peer_addrs: Vec::new(),
            failed_peer_addrs: Vec::new(),
        });
    }
    let start = Height(ancestor.height.0.saturating_add(1));
    let headers = request_headers(&mut leader_connection, start, target, ancestor.hash)?;
    let ranges = plan_parallel_ranges(start, target, &headers, &candidates);
    if ranges.is_empty() {
        return Ok(ParallelSyncReport {
            remote_tip,
            applied_blocks: 0,
            used_peers: 0,
            used_peer_addrs: Vec::new(),
            failed_peer_addrs: Vec::new(),
        });
    }

    let mut handles = Vec::new();
    for range in ranges {
        let node = Arc::clone(node);
        let public_addrs = public_addrs.to_vec();
        let candidates = candidates.clone();
        handles.push(thread::spawn(move || {
            fetch_range_with_retries(range, candidates, &node, &public_addrs)
        }));
    }

    let mut downloaded = BTreeMap::new();
    let mut used_peers = HashSet::new();
    let mut failed_peers = HashSet::new();
    for handle in handles {
        let (start, peer, blocks, worker_failed_peers) = handle
            .join()
            .map_err(|_| "parallel sync worker panicked".to_string())??;
        used_peers.insert(peer);
        for failed_peer in worker_failed_peers {
            failed_peers.insert(failed_peer);
        }
        downloaded.insert(start, blocks);
    }
    let used_peer_addrs = used_peers.iter().copied().collect::<Vec<_>>();
    for peer in &used_peer_addrs {
        failed_peers.remove(peer);
    }
    let failed_peer_addrs = failed_peers.iter().copied().collect::<Vec<_>>();

    let mut node = node
        .lock()
        .map_err(|_| "node state lock poisoned".to_string())?;
    let mut expected_height = start.0;
    let mut applied_blocks = 0;
    for blocks in downloaded.into_values() {
        for block in blocks {
            let height = block.height();
            if height.0 != expected_height {
                return Err(format!(
                    "parallel sync downloaded height {} while applying height {}",
                    height.0, expected_height
                ));
            }
            node.apply_block(block).map_err(|error| {
                format!(
                    "failed to apply parallel synced block {}: {error}",
                    height.0
                )
            })?;
            expected_height = expected_height.saturating_add(1);
            applied_blocks += 1;
        }
    }

    println!(
        "parallel synced blocks through height {} |peers::{}|tip::{}|",
        expected_height.saturating_sub(1),
        used_peers.len(),
        node.tip_hash()
            .map(|hash| hex::encode(hash.0))
            .unwrap_or_else(|| "none".to_string())
    );

    Ok(ParallelSyncReport {
        remote_tip,
        applied_blocks,
        used_peers: used_peers.len(),
        used_peer_addrs,
        failed_peer_addrs,
    })
}

pub fn request_peers_connection(peer: &mut PeerConnection) -> Result<Vec<PeerInfo>, String> {
    match peer.request(NetworkMessage::GetPeers)? {
        NetworkMessage::Peers(peers) => Ok(peers),
        _ => Err("peer returned unexpected peers response".to_string()),
    }
}

pub fn sync_mempool_connection(
    peer: &mut PeerConnection,
    node: &Arc<Mutex<Node>>,
) -> Result<usize, String> {
    let response = peer.request(NetworkMessage::GetMempoolInventory)?;
    let NetworkMessage::Inventory(items) = response else {
        return Err("peer returned unexpected mempool inventory response".to_string());
    };
    let missing = {
        let node = node
            .lock()
            .map_err(|_| "node state lock poisoned".to_string())?;
        items
            .into_iter()
            .filter_map(|item| match item {
                InventoryItem::Transaction(hash) if !node.mempool.contains(&hash) => {
                    Some(InventoryItem::Transaction(hash))
                }
                _ => None,
            })
            .collect::<Vec<_>>()
    };
    if missing.is_empty() {
        return Ok(0);
    }

    let response = peer.request(NetworkMessage::GetData(missing))?;
    let NetworkMessage::Transactions(transactions) = response else {
        return Err("peer returned unexpected mempool data response".to_string());
    };
    let mut accepted = 0;
    let mut node = node
        .lock()
        .map_err(|_| "node state lock poisoned".to_string())?;
    for transaction in transactions {
        match node.submit_transaction(transaction) {
            Ok(_) => accepted += 1,
            Err(error) => eprintln!(
                "mempool sync rejected transaction from {}: {error}",
                peer.addr()
            ),
        }
    }
    Ok(accepted)
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

fn request_blocks(
    peer: &mut PeerConnection,
    node: &Arc<Mutex<Node>>,
    start: Height,
    target: u64,
    headers: Vec<BlockHeader>,
) -> Result<(), String> {
    let blocks = fetch_blocks(peer, start, target, headers)?;
    let mut node = node
        .lock()
        .map_err(|_| "node state lock poisoned".to_string())?;
    for block in blocks {
        let height = block.height();
        node.apply_block(block).map_err(|error| {
            format!(
                "failed to apply block {} from {}: {error}",
                height.0,
                peer.addr()
            )
        })?;
    }
    println!(
        "synced blocks through height {} from {} |tip::{}|",
        target,
        peer.addr(),
        node.tip_hash()
            .map(|hash| hex::encode(hash.0))
            .unwrap_or_else(|| "none".to_string())
    );
    Ok(())
}

fn fetch_blocks(
    peer: &mut PeerConnection,
    start: Height,
    target: u64,
    headers: Vec<BlockHeader>,
) -> Result<Vec<Block>, String> {
    let mut downloaded = Vec::new();
    let mut next_height = start.0;
    let mut expected_headers = headers.into_iter().peekable();
    while next_height <= target {
        let remaining = target.saturating_sub(next_height).saturating_add(1);
        let limit = remaining.min(MAX_BLOCKS_PER_BATCH) as u32;
        let response = peer.request(NetworkMessage::GetBlocksByHeightRange {
            start: Height(next_height),
            limit,
        })?;
        let NetworkMessage::Blocks(blocks) = response else {
            return Err(format!(
                "peer did not return block range starting at height {}",
                next_height
            ));
        };
        if blocks.is_empty() {
            return Err(format!(
                "peer returned empty block range starting at height {}",
                next_height
            ));
        }

        for block in blocks {
            let height = block.height();
            if height.0 != next_height {
                return Err(format!(
                    "peer returned height {} while syncing height {}",
                    height.0, next_height
                ));
            }
            let Some(expected_header) = expected_headers.next() else {
                return Err(format!(
                    "peer returned block {} without a prevalidated header",
                    height.0
                ));
            };
            if block.header != expected_header {
                return Err(format!(
                    "peer returned block {} that does not match its prevalidated header",
                    height.0
                ));
            }
            downloaded.push(block);
            next_height = next_height.saturating_add(1);
        }
    }
    Ok(downloaded)
}

#[derive(Debug, Clone)]
struct SyncRange {
    peer: SocketAddr,
    start: Height,
    target: u64,
    headers: Vec<BlockHeader>,
}

fn fetch_range_with_retries(
    range: SyncRange,
    candidates: Vec<(SocketAddr, Height)>,
    node: &Arc<Mutex<Node>>,
    public_addrs: &[SocketAddr],
) -> Result<(u64, SocketAddr, Vec<Block>, Vec<SocketAddr>), String> {
    let mut peers = Vec::new();
    peers.push(range.peer);
    peers.extend(
        candidates
            .into_iter()
            .filter(|(addr, tip)| *addr != range.peer && tip.0 >= range.target)
            .map(|(addr, _)| addr),
    );

    let mut last_error = None;
    let mut failed_peers = Vec::new();
    for peer_addr in peers {
        let mut peer = match PeerConnection::connect(peer_addr) {
            Ok(peer) => peer,
            Err(error) => {
                failed_peers.push(peer_addr);
                last_error = Some(format!("connect {peer_addr} failed: {error}"));
                continue;
            }
        };
        if let Err(error) = handshake_peer(&mut peer, node, public_addrs) {
            failed_peers.push(peer_addr);
            last_error = Some(format!("handshake {peer_addr} failed: {error}"));
            continue;
        }
        match fetch_blocks(&mut peer, range.start, range.target, range.headers.clone()) {
            Ok(blocks) => {
                if peer_addr != range.peer {
                    println!(
                        "parallel sync reassigned range {}..{} from {} to {}",
                        range.start.0, range.target, range.peer, peer_addr
                    );
                }
                return Ok((range.start.0, peer_addr, blocks, failed_peers));
            }
            Err(error) => {
                failed_peers.push(peer_addr);
                last_error = Some(format!(
                    "download {}..{} from {} failed: {error}",
                    range.start.0, range.target, peer_addr
                ));
            }
        }
    }

    Err(last_error.unwrap_or_else(|| {
        format!(
            "no peer could download range {}..{}",
            range.start.0, range.target
        )
    }))
}

fn plan_parallel_ranges(
    start: Height,
    target: u64,
    headers: &[BlockHeader],
    candidates: &[(SocketAddr, Height)],
) -> Vec<SyncRange> {
    let available = candidates
        .iter()
        .copied()
        .filter(|(_, tip)| tip.0 >= target)
        .map(|(addr, _)| addr)
        .collect::<Vec<_>>();
    if available.is_empty() {
        return Vec::new();
    }

    let mut ranges = Vec::new();
    let mut next_height = start.0;
    let mut header_index = 0;
    while next_height <= target {
        let remaining = target.saturating_sub(next_height).saturating_add(1);
        let count = remaining.min(MAX_BLOCKS_PER_BATCH) as usize;
        let peer = available[ranges.len() % available.len()];
        ranges.push(SyncRange {
            peer,
            start: Height(next_height),
            target: next_height.saturating_add(count as u64).saturating_sub(1),
            headers: headers[header_index..header_index + count].to_vec(),
        });
        next_height = next_height.saturating_add(count as u64);
        header_index += count;
    }
    ranges
}

fn request_headers(
    peer: &mut PeerConnection,
    start: Height,
    target: u64,
    anchor_hash: BlockHash,
) -> Result<Vec<BlockHeader>, String> {
    let mut next_height = start.0;
    let mut previous_hash = anchor_hash;
    let mut headers = Vec::new();

    while next_height <= target {
        let remaining = target.saturating_sub(next_height).saturating_add(1);
        let limit = remaining.min(MAX_BLOCKS_PER_BATCH) as u32;
        let response = peer.request(NetworkMessage::GetBlockHeadersByHeightRange {
            start: Height(next_height),
            limit,
        })?;
        let NetworkMessage::BlockHeaders(batch) = response else {
            return Err(format!(
                "peer did not return header range starting at height {}",
                next_height
            ));
        };
        if batch.is_empty() {
            return Err(format!(
                "peer returned empty header range starting at height {}",
                next_height
            ));
        }

        for header in batch {
            if header.height.0 != next_height {
                return Err(format!(
                    "peer returned header height {} while syncing height {}",
                    header.height.0, next_height
                ));
            }
            if header.previous_hash.0 != previous_hash.0 {
                return Err(format!(
                    "peer returned header {} with unexpected parent {}",
                    header.height.0,
                    hex::encode(header.previous_hash.0)
                ));
            }
            previous_hash = header.hash();
            headers.push(header);
            next_height = next_height.saturating_add(1);
        }
    }

    Ok(headers)
}

fn request_common_ancestor(
    peer: &mut PeerConnection,
    node: &Arc<Mutex<Node>>,
) -> Result<TipInfo, String> {
    let locator = block_locator(node)?;
    match peer.request(NetworkMessage::GetCommonAncestor { locator })? {
        NetworkMessage::CommonAncestor(Some(ancestor)) => Ok(ancestor),
        NetworkMessage::CommonAncestor(None) => {
            Err("peer did not find a common ancestor from local locator".to_string())
        }
        _ => Err("peer returned unexpected common ancestor response".to_string()),
    }
}

fn block_locator(node: &Arc<Mutex<Node>>) -> Result<Vec<BlockHash>, String> {
    let node = node
        .lock()
        .map_err(|_| "node state lock poisoned".to_string())?;
    let Some(tip_height) = node.tip_height() else {
        return Ok(Vec::new());
    };

    let mut locator = Vec::new();
    let mut height = tip_height.0;
    let mut step = 1_u64;
    loop {
        if let Some(block) = node.ledger.block(&Height(height)) {
            locator.push(block.hash());
        }
        if height == 0 || locator.len() >= MAX_BLOCK_LOCATOR_HASHES {
            break;
        }
        height = height.saturating_sub(step);
        if locator.len() >= 10 {
            step = step.saturating_mul(2);
        }
    }

    if locator.last().is_some_and(|hash| {
        node.ledger
            .block(&Height(0))
            .is_some_and(|block| block.hash() != *hash)
    }) {
        if let Some(genesis) = node.ledger.block(&Height(0)) {
            locator.push(genesis.hash());
        }
    }

    Ok(locator)
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
