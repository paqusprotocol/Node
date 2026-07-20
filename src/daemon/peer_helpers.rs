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

