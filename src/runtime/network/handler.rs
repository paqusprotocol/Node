use crate::runtime::network::error::NetworkError;
use crate::runtime::network::message::{InventoryItem, NetworkMessage, TipInfo, VersionInfo};
use crate::runtime::node::Node;
use paqus::block::Height;

const MAX_RANGE_RESPONSE_ITEMS: u32 = 64;

pub fn handle_message(
    node: &mut Node,
    message: NetworkMessage,
) -> Result<Option<NetworkMessage>, NetworkError> {
    match message {
        NetworkMessage::Version(version) => match version.validate_compatibility() {
            Ok(()) => Ok(Some(NetworkMessage::VerAck(local_version(node)))),
            Err(reason) => Ok(Some(NetworkMessage::Reject {
                reason,
                message: "incompatible peer version".to_string(),
            })),
        },
        NetworkMessage::VerAck(_) => Ok(None),
        NetworkMessage::Reject { .. } => Ok(None),
        NetworkMessage::Ping { nonce } => Ok(Some(NetworkMessage::Pong { nonce })),
        NetworkMessage::Pong { .. } => Ok(None),
        NetworkMessage::GetTip => Ok(node
            .tip_height()
            .zip(node.tip_hash())
            .map(|(height, hash)| NetworkMessage::Tip(TipInfo { height, hash }))),
        NetworkMessage::Tip(_) => Ok(None),
        NetworkMessage::GetBlockByHeight { height } => Ok(node
            .ledger
            .block(&height)
            .cloned()
            .map(NetworkMessage::Block)),
        NetworkMessage::GetBlocksByHeightRange { start, limit } => {
            let limit = limit.min(MAX_RANGE_RESPONSE_ITEMS);
            let blocks = (start.0..start.0.saturating_add(limit as u64))
                .map(Height)
                .map_while(|height| node.ledger.block(&height).cloned())
                .collect::<Vec<_>>();
            Ok(Some(NetworkMessage::Blocks(blocks)))
        }
        NetworkMessage::GetBlockHeadersByHeightRange { start, limit } => {
            let limit = limit.min(MAX_RANGE_RESPONSE_ITEMS);
            let headers = (start.0..start.0.saturating_add(limit as u64))
                .map(Height)
                .map_while(|height| node.ledger.block(&height).map(|block| block.header.clone()))
                .collect::<Vec<_>>();
            Ok(Some(NetworkMessage::BlockHeaders(headers)))
        }
        NetworkMessage::GetCommonAncestor { locator } => {
            let ancestor = locator.into_iter().find_map(|hash| {
                let block = node.cache.block_by_hash(&hash)?;
                Some(TipInfo {
                    height: block.height(),
                    hash,
                })
            });
            Ok(Some(NetworkMessage::CommonAncestor(ancestor)))
        }
        NetworkMessage::CommonAncestor(_) => Ok(None),
        NetworkMessage::GetBlockByHash { hash } => Ok(node
            .cache
            .block_by_hash(&hash)
            .cloned()
            .map(NetworkMessage::Block)),
        NetworkMessage::Block(block) => {
            node.apply_block(block)?;
            Ok(None)
        }
        NetworkMessage::Blocks(blocks) => {
            for block in blocks {
                node.apply_block(block)?;
            }
            Ok(None)
        }
        NetworkMessage::BlockHeaders(_) => Ok(None),
        NetworkMessage::Inventory(items) => {
            let missing = items
                .into_iter()
                .filter(|item| match item {
                    InventoryItem::Block(hash) => node.cache.block_by_hash(hash).is_none(),
                    InventoryItem::Transaction(hash) => !node.mempool.contains(hash),
                })
                .collect::<Vec<_>>();
            if missing.is_empty() {
                Ok(None)
            } else {
                Ok(Some(NetworkMessage::GetData(missing)))
            }
        }
        NetworkMessage::GetData(items) => {
            let mut blocks = Vec::new();
            let mut transactions = Vec::new();
            for item in items {
                match item {
                    InventoryItem::Block(hash) => {
                        if let Some(block) = node.cache.block_by_hash(&hash).cloned() {
                            blocks.push(block);
                        }
                    }
                    InventoryItem::Transaction(hash) => {
                        if let Some(transaction) = node.mempool.get(&hash).cloned() {
                            transactions.push(transaction);
                        }
                    }
                }
            }
            if !blocks.is_empty() {
                return Ok(Some(NetworkMessage::Blocks(blocks)));
            }
            if !transactions.is_empty() {
                return Ok(Some(NetworkMessage::Transactions(transactions)));
            }
            Ok(None)
        }
        NetworkMessage::Transaction(transaction) => {
            node.submit_transaction(transaction)?;
            Ok(None)
        }
        NetworkMessage::Transactions(transactions) => {
            for transaction in transactions {
                node.submit_transaction(transaction)?;
            }
            Ok(None)
        }
        NetworkMessage::GetMempoolInventory => Ok(Some(NetworkMessage::Inventory(
            node.mempool
                .transactions()
                .map(|transaction| InventoryItem::Transaction(transaction.hash()))
                .collect(),
        ))),
        NetworkMessage::GetPeers => Ok(Some(NetworkMessage::Peers(vec![]))),
        NetworkMessage::Peers(_) => Ok(None),
    }
}

fn local_version(node: &Node) -> VersionInfo {
    VersionInfo::local(
        node.tip_height()
            .zip(node.tip_hash())
            .map(|(height, hash)| TipInfo { height, hash }),
    )
}
