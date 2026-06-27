use crate::network::send_message;
use crate::p2p::PeerState;
use crate::paquscore::{BlockHash, NetworkMessage, TransactionHash};
use std::collections::{HashMap, HashSet, VecDeque};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone, Copy, Default)]
pub struct BroadcastReport {
    pub attempted: usize,
    pub sent: usize,
    pub failed: usize,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct GossipDedupe {
    capacity: usize,
    blocks: HashSet<BlockHash>,
    block_order: VecDeque<BlockHash>,
    transactions: HashSet<TransactionHash>,
    transaction_order: VecDeque<TransactionHash>,
}

impl GossipDedupe {
    #[allow(dead_code)]
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            blocks: HashSet::new(),
            block_order: VecDeque::new(),
            transactions: HashSet::new(),
            transaction_order: VecDeque::new(),
        }
    }

    #[allow(dead_code)]
    pub fn mark_block_seen(&mut self, hash: BlockHash) -> bool {
        mark_seen(self.capacity, &mut self.blocks, &mut self.block_order, hash)
    }

    #[allow(dead_code)]
    pub fn mark_transaction_seen(&mut self, hash: TransactionHash) -> bool {
        mark_seen(
            self.capacity,
            &mut self.transactions,
            &mut self.transaction_order,
            hash,
        )
    }
}

#[allow(dead_code)]
fn mark_seen<T: Copy + Eq + std::hash::Hash>(
    capacity: usize,
    seen: &mut HashSet<T>,
    order: &mut VecDeque<T>,
    value: T,
) -> bool {
    if seen.contains(&value) {
        return false;
    }
    if capacity == 0 {
        return true;
    }
    seen.insert(value);
    order.push_back(value);
    while seen.len() > capacity {
        if let Some(evicted) = order.pop_front() {
            seen.remove(&evicted);
        }
    }
    true
}

pub fn broadcast_to_peers(
    peers: &Arc<Mutex<HashMap<SocketAddr, PeerState>>>,
    message: NetworkMessage,
) -> BroadcastReport {
    let peers = match peers.lock() {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dedupe_tracks_seen_blocks_and_transactions() {
        let mut dedupe = GossipDedupe::new(2);
        let first_block = BlockHash([1; 64]);
        let second_block = BlockHash([2; 64]);
        let third_block = BlockHash([3; 64]);
        let transaction = TransactionHash([4; 64]);

        assert!(dedupe.mark_block_seen(first_block));
        assert!(!dedupe.mark_block_seen(first_block));
        assert!(dedupe.mark_block_seen(second_block));
        assert!(dedupe.mark_block_seen(third_block));
        assert!(dedupe.mark_block_seen(first_block));

        assert!(dedupe.mark_transaction_seen(transaction));
        assert!(!dedupe.mark_transaction_seen(transaction));
    }
}
