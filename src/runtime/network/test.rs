use super::{
    InventoryItem, NetworkEnvelope, NetworkError, NetworkMessage, Peer, PeerInfo, RejectReason,
    TipInfo, VersionInfo, handle_message, read_message, write_message,
};
use crate::runtime::node::Node;
use crate::runtime::params::BASE_FEE;
use crate::runtime::params::MAX_NETWORK_MESSAGE_SIZE;
use paqus::block::{Block, Height, Nonce};
use paqus::consensus::supply::Amount;
use paqus::consensus::{Consensus, ConsensusConfig};
use paqus::crypto::{
    Address, BlockHash, HASH_SIZE, Hash, TransactionHash, address_from_public_key,
    generate_keypair, sign,
};
use paqus::ledger::Ledger;
use paqus::transaction::{SignedTransaction, Transaction};
use std::io::{Cursor, Read, Write};
use std::time::{SystemTime, UNIX_EPOCH};

fn address(byte: u8) -> Address {
    Address([byte; 20])
}

fn block() -> Block {
    Block::new(
        Height(0),
        Hash([0; HASH_SIZE]),
        Address([9; 20]),
        1_700_000_000,
        Nonce(0),
        vec![],
    )
}

#[test]
fn roundtrips_ping_message() {
    let envelope = NetworkMessage::Ping { nonce: 7 }.to_envelope();
    let bytes = envelope.to_bytes().unwrap();

    assert_eq!(NetworkEnvelope::from_bytes(&bytes).unwrap(), envelope);
}

#[test]
fn roundtrips_tip_and_block_messages() {
    let block = block();
    let tip = NetworkMessage::Tip(TipInfo {
        height: block.height(),
        hash: block.hash(),
    })
    .to_envelope();
    let block_message = NetworkMessage::Block(block).to_envelope();

    assert_eq!(
        NetworkEnvelope::from_bytes(&tip.to_bytes().unwrap()).unwrap(),
        tip
    );
    assert_eq!(
        NetworkEnvelope::from_bytes(&block_message.to_bytes().unwrap()).unwrap(),
        block_message
    );
}

#[test]
fn roundtrips_peer_list() {
    let envelope = NetworkMessage::Peers(vec![PeerInfo {
        address: "127.0.0.1:5555".to_string(),
    }])
    .to_envelope();

    assert_eq!(
        NetworkEnvelope::from_bytes(&envelope.to_bytes().unwrap()).unwrap(),
        envelope
    );
}

#[test]
fn roundtrips_version_handshake_messages() {
    let version = VersionInfo::local(Some(TipInfo {
        height: Height(7),
        hash: Hash([7; HASH_SIZE]).into(),
    }));
    let messages = [
        NetworkMessage::Version(version.clone()),
        NetworkMessage::VerAck(version),
        NetworkMessage::Reject {
            reason: RejectReason::ProtocolVersionMismatch,
            message: "bad version".to_string(),
        },
    ];

    for message in messages {
        let envelope = message.to_envelope();
        assert_eq!(
            NetworkEnvelope::from_bytes(&envelope.to_bytes().unwrap()).unwrap(),
            envelope
        );
    }
}

#[test]
fn rejects_oversized_message_bytes() {
    let bytes = vec![0_u8; MAX_NETWORK_MESSAGE_SIZE + 1];

    assert!(matches!(
        NetworkEnvelope::from_bytes(&bytes),
        Err(NetworkError::MessageTooLarge)
    ));
}

#[test]
fn rejects_wrong_network_magic() {
    let mut envelope = NetworkMessage::GetTip.to_envelope();
    envelope.magic = [0, 0, 0, 0];
    let bytes = borsh::to_vec(&envelope).unwrap();

    assert!(matches!(
        NetworkEnvelope::from_bytes(&bytes),
        Err(NetworkError::Serialization(_))
    ));
}

#[test]
fn writes_and_reads_framed_message() {
    let envelope = NetworkMessage::Ping { nonce: 42 }.to_envelope();
    let mut bytes = Vec::new();

    write_message(&mut bytes, &envelope).unwrap();

    let mut cursor = Cursor::new(bytes);
    assert_eq!(read_message(&mut cursor).unwrap(), envelope);
}

#[test]
fn rejects_oversized_framed_message_length() {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&((MAX_NETWORK_MESSAGE_SIZE as u32) + 1).to_be_bytes());

    let mut cursor = Cursor::new(bytes);

    assert!(matches!(
        read_message(&mut cursor),
        Err(NetworkError::MessageTooLarge)
    ));
}

#[test]
fn rejects_partial_framed_message() {
    let envelope = NetworkMessage::Pong { nonce: 7 }.to_envelope();
    let mut bytes = Vec::new();
    write_message(&mut bytes, &envelope).unwrap();
    bytes.pop();

    let mut cursor = Cursor::new(bytes);

    assert!(matches!(
        read_message(&mut cursor),
        Err(NetworkError::Io(_))
    ));
}

#[test]
fn handler_responds_to_ping_and_tip_requests() {
    let mut node = test_node_with_genesis();

    assert_eq!(
        handle_message(&mut node, NetworkMessage::Ping { nonce: 7 }).unwrap(),
        Some(NetworkMessage::Pong { nonce: 7 })
    );

    assert_eq!(
        handle_message(&mut node, NetworkMessage::GetTip).unwrap(),
        Some(NetworkMessage::Tip(TipInfo {
            height: Height(0),
            hash: node.tip_hash().unwrap()
        }))
    );
}

#[test]
fn handler_accepts_compatible_version_and_rejects_incompatible_version() {
    let mut node = test_node_with_genesis();
    let compatible = VersionInfo::local(None);

    assert!(matches!(
        handle_message(&mut node, NetworkMessage::Version(compatible)).unwrap(),
        Some(NetworkMessage::VerAck(_))
    ));

    let mut incompatible = VersionInfo::local(None);
    incompatible.protocol_version = incompatible.protocol_version.saturating_add(1);

    assert_eq!(
        handle_message(&mut node, NetworkMessage::Version(incompatible)).unwrap(),
        Some(NetworkMessage::Reject {
            reason: RejectReason::ProtocolVersionMismatch,
            message: "incompatible peer version".to_string()
        })
    );

    let mut incompatible_pow = VersionInfo::local(None);
    incompatible_pow.pow_algorithm = "argon2id".to_string();
    assert_eq!(
        handle_message(&mut node, NetworkMessage::Version(incompatible_pow)).unwrap(),
        Some(NetworkMessage::Reject {
            reason: RejectReason::ConsensusMismatch,
            message: "incompatible peer version".to_string()
        })
    );
}

#[test]
fn handler_returns_blocks_by_height_and_hash() {
    let mut node = test_node_with_genesis();
    let block = node.ledger.block(&Height(0)).unwrap().clone();

    assert_eq!(
        handle_message(
            &mut node,
            NetworkMessage::GetBlockByHeight { height: Height(0) }
        )
        .unwrap(),
        Some(NetworkMessage::Block(block.clone()))
    );
    assert_eq!(
        handle_message(
            &mut node,
            NetworkMessage::GetBlockByHash { hash: block.hash() }
        )
        .unwrap(),
        Some(NetworkMessage::Block(block))
    );
}

#[test]
fn handler_returns_block_ranges_by_height() {
    let mut node = test_node_with_genesis();
    let block = node.ledger.block(&Height(0)).unwrap().clone();

    assert_eq!(
        handle_message(
            &mut node,
            NetworkMessage::GetBlocksByHeightRange {
                start: Height(0),
                limit: 32
            }
        )
        .unwrap(),
        Some(NetworkMessage::Blocks(vec![block]))
    );

    assert_eq!(
        handle_message(
            &mut node,
            NetworkMessage::GetBlocksByHeightRange {
                start: Height(1),
                limit: 32
            }
        )
        .unwrap(),
        Some(NetworkMessage::Blocks(vec![]))
    );
}

#[test]
fn handler_returns_block_header_ranges_by_height() {
    let mut node = test_node_with_genesis();
    let block = node.ledger.block(&Height(0)).unwrap().clone();

    assert_eq!(
        handle_message(
            &mut node,
            NetworkMessage::GetBlockHeadersByHeightRange {
                start: Height(0),
                limit: 32
            }
        )
        .unwrap(),
        Some(NetworkMessage::BlockHeaders(vec![block.header]))
    );
}

#[test]
fn handler_returns_common_ancestor_from_locator() {
    let mut node = test_node_with_genesis();
    let block = node.ledger.block(&Height(0)).unwrap().clone();

    assert_eq!(
        handle_message(
            &mut node,
            NetworkMessage::GetCommonAncestor {
                locator: vec![BlockHash([9; HASH_SIZE]), block.hash()]
            }
        )
        .unwrap(),
        Some(NetworkMessage::CommonAncestor(Some(TipInfo {
            height: Height(0),
            hash: block.hash()
        })))
    );

    assert_eq!(
        handle_message(
            &mut node,
            NetworkMessage::GetCommonAncestor {
                locator: vec![BlockHash([9; HASH_SIZE])]
            }
        )
        .unwrap(),
        Some(NetworkMessage::CommonAncestor(None))
    );
}

#[test]
fn handler_requests_missing_inventory_data() {
    let mut node = test_node_with_genesis();
    let block = node.ledger.block(&Height(0)).unwrap().clone();

    assert_eq!(
        handle_message(
            &mut node,
            NetworkMessage::Inventory(vec![
                InventoryItem::Block(block.hash()),
                InventoryItem::Transaction(TransactionHash([9; HASH_SIZE]))
            ])
        )
        .unwrap(),
        Some(NetworkMessage::GetData(vec![InventoryItem::Transaction(
            TransactionHash([9; HASH_SIZE])
        )]))
    );

    assert_eq!(
        handle_message(
            &mut node,
            NetworkMessage::Inventory(vec![InventoryItem::Block(BlockHash([9; HASH_SIZE]))])
        )
        .unwrap(),
        Some(NetworkMessage::GetData(vec![InventoryItem::Block(
            BlockHash([9; HASH_SIZE])
        )]))
    );
}

#[test]
fn handler_returns_mempool_inventory_and_data_by_hash() {
    let transaction = signed_transaction_to(address(2), 10, 0);
    let hash = transaction.hash();
    let sender = transaction.transaction.from;
    let mut ledger = Ledger::new();
    ledger.create_account(sender, Amount(100)).unwrap();
    ledger.create_account(address(2), Amount(0)).unwrap();
    let mut node = Node::temporary(
        ledger,
        Consensus {
            config: ConsensusConfig { difficulty: 0 },
        },
    )
    .unwrap();
    node.submit_transaction(transaction.clone()).unwrap();

    assert_eq!(
        handle_message(&mut node, NetworkMessage::GetMempoolInventory).unwrap(),
        Some(NetworkMessage::Inventory(vec![InventoryItem::Transaction(
            hash
        )]))
    );
    assert_eq!(
        handle_message(
            &mut node,
            NetworkMessage::GetData(vec![InventoryItem::Transaction(hash)])
        )
        .unwrap(),
        Some(NetworkMessage::Transactions(vec![transaction.into()]))
    );
}

#[test]
fn handler_submits_transaction_to_node_mempool() {
    let transaction = signed_transaction_to(address(2), 10, 0);
    let hash = transaction.hash();
    let sender = transaction.transaction.from;
    let mut ledger = Ledger::new();
    ledger.create_account(sender, Amount(100)).unwrap();
    ledger.create_account(address(2), Amount(0)).unwrap();
    let mut node = Node::temporary(
        ledger,
        Consensus {
            config: ConsensusConfig { difficulty: 0 },
        },
    )
    .unwrap();

    assert_eq!(
        handle_message(&mut node, NetworkMessage::Transaction(transaction.into())).unwrap(),
        None
    );
    assert!(node.mempool.contains(&hash));
}

#[test]
fn peer_sends_and_receives_messages() {
    let stream = MemoryStream::default();
    let info = PeerInfo {
        address: "127.0.0.1:5555".to_string(),
    };
    let mut peer = Peer::new(stream, info.clone());

    assert_eq!(peer.info(), &info);

    peer.send(NetworkMessage::Ping { nonce: 99 }).unwrap();
    peer.stream_mut().rewind();

    assert_eq!(
        peer.recv().unwrap(),
        NetworkMessage::Ping { nonce: 99 }.to_envelope()
    );
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct MemoryStream {
    bytes: Vec<u8>,
    read_position: usize,
}

fn test_node_with_genesis() -> Node {
    let mut ledger = Ledger::new();
    let block = block();
    ledger.chain.insert_block(block).unwrap();
    Node::temporary(
        ledger,
        Consensus {
            config: ConsensusConfig { difficulty: 0 },
        },
    )
    .unwrap()
}

fn signed_transaction_to(to: Address, amount: u64, nonce: u64) -> SignedTransaction {
    let keypair = generate_keypair();
    let from = address_from_public_key(&keypair.public_key);
    let payload = Transaction::new_at(
        from,
        to,
        Amount(amount),
        Amount(BASE_FEE),
        Nonce(nonce),
        current_unix_timestamp(),
    );
    let signature = sign(&keypair.secret_key, &payload.signing_bytes());
    SignedTransaction::new(payload, keypair.public_key, signature)
}

fn current_unix_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}

impl MemoryStream {
    fn rewind(&mut self) {
        self.read_position = 0;
    }
}

impl Read for MemoryStream {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        let remaining = self.bytes.len().saturating_sub(self.read_position);
        let count = remaining.min(buffer.len());
        buffer[..count]
            .copy_from_slice(&self.bytes[self.read_position..self.read_position + count]);
        self.read_position += count;
        Ok(count)
    }
}

impl Write for MemoryStream {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        self.bytes.extend_from_slice(buffer);
        Ok(buffer.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
