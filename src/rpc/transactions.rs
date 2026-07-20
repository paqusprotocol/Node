async fn rpc_submit_qcash_tx(
    State(state): State<RpcState>,
    Json(request): Json<SubmitTxRequest>,
) -> impl IntoResponse {
    let transaction = match signed_qcash_transaction_from_hex(&request.tx) {
        Ok(transaction) => transaction,
        Err(error) => return rpc_error(StatusCode::BAD_REQUEST, error),
    };
    let hash = transaction.hash();
    let wtxid = transaction.wtxid();
    match state.node.lock() {
        Ok(mut node) => {
            if let Err(error) = node.submit_qcash_transaction(transaction) {
                return rpc_error(
                    StatusCode::BAD_REQUEST,
                    format!("failed to submit QCash transaction: {error}"),
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

