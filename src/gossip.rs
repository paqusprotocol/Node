use crate::network::send_message;
use crate::p2p::PeerState;
use crate::paquscore::NetworkMessage;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone, Copy, Default)]
pub struct BroadcastReport {
    pub attempted: usize,
    pub sent: usize,
    pub failed: usize,
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
