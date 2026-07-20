async fn rpc_tx(
    State(state): State<RpcState>,
    AxumPath(hash): AxumPath<String>,
) -> impl IntoResponse {
    let hash = match parse_hash_hex(&hash) {
        Ok(hash) => hash,
        Err(error) => return rpc_error(StatusCode::BAD_REQUEST, error),
    };
    match state.node.lock() {
        Ok(node) => match find_transaction(&node, &hash) {
            Ok(Some(transaction)) => Json(transaction).into_response(),
            Ok(None) => rpc_error(StatusCode::NOT_FOUND, "transaction_not_found"),
            Err(error) => rpc_error(StatusCode::INTERNAL_SERVER_ERROR, error),
        },
        Err(_) => rpc_error(StatusCode::INTERNAL_SERVER_ERROR, "state_lock_failed"),
    }
}
fn balance_json(node: &Node, address: &Address) -> String {
    let address_str = address_to_string(address);
    let height = node.tip_height().unwrap_or(Height(0)).0;
    let Some(summary) = node.balance_summary(address) else {
        return format!(
            "{{\"address\":\"{address_str}\",\"height\":{height},\"exists\":false,\"confirmed\":0,\"available\":0,\"pending_incoming\":0,\"pending_outgoing\":0,\"nonce\":null,\"unspendable\":0}}"
        );
    };
    let account = node.account_view(address);
    let nonce = account
        .map(|account| account.nonce.0.to_string())
        .unwrap_or_else(|| "null".to_string());
    let unspendable = account.map(|account| account.unspendable.0).unwrap_or(0);

    format!(
        "{{\"address\":\"{address_str}\",\"height\":{height},\"exists\":true,\"confirmed\":{},\"available\":{},\"pending_incoming\":{},\"pending_outgoing\":{},\"nonce\":{nonce},\"unspendable\":{unspendable}}}",
        summary.confirmed.0,
        summary.available.0,
        summary.pending.incoming.0,
        summary.pending.outgoing.0
    )
}



async fn rpc_address(
    State(state): State<RpcState>,
    AxumPath(address): AxumPath<String>,
) -> impl IntoResponse {
    let address = match parse_address_string(&address) {
        Ok(address) => address,
        Err(error) => return rpc_error(StatusCode::BAD_REQUEST, error),
    };
    match state.node.lock() {
        Ok(node) => match address_activity(&node, &address) {
            Ok(activity) => {
                let balance: serde_json::Value = serde_json::from_str(&balance_json(
                    &node, &address,
                ))
                .unwrap_or_else(|_| serde_json::json!({ "error": "balance_encode_failed" }));
                Json(AddressResponse {
                    address: address_to_string(&address),
                    balance,
                    mined_blocks: activity.mined_blocks,
                    transactions: activity.transactions,
                })
                .into_response()
            }
            Err(error) => rpc_error(StatusCode::INTERNAL_SERVER_ERROR, error),
        },
        Err(_) => rpc_error(StatusCode::INTERNAL_SERVER_ERROR, "state_lock_failed"),
    }
}

async fn rpc_accounts(State(state): State<RpcState>) -> impl IntoResponse {
    match state.node.lock() {
        Ok(node) => {
            let height = node.tip_height().unwrap_or(Height(0));
            let accounts = node
                .ledger
                .accounts()
                .values()
                .map(|account| {
                    let pending = node.pending_balance(&account.address);
                    AccountResponse {
                        address: address_to_string(&account.address),
                        confirmed: account.balance.0,
                        available: account.available_balance_at(height).0,
                        unspendable: account.unspendable_balance_at(height).0,
                        pending_incoming: pending.incoming.0,
                        pending_outgoing: pending.outgoing.0,
                        nonce: account.nonce.0,
                        credits: account.credits.len(),
                    }
                })
                .collect::<Vec<_>>();
            Json(accounts).into_response()
        }
        Err(_) => rpc_error(StatusCode::INTERNAL_SERVER_ERROR, "state_lock_failed"),
    }
}

async fn rpc_mempool(State(state): State<RpcState>) -> impl IntoResponse {
    match state.node.lock() {
        Ok(node) => {
            let transactions = node
                .mempool
                .transactions()
                .cloned()
                .map(SignedProtocolTransaction::Transfer)
                .chain(node.extension_mempool.transactions().cloned())
                .map(|transaction| protocol_tx_response(&transaction, None, None, "pending"))
                .collect::<Vec<_>>();
            Json(MempoolResponse {
                size: node.mempool.len() + node.extension_mempool.len(),
                transactions,
            })
            .into_response()
        }
        Err(_) => rpc_error(StatusCode::INTERNAL_SERVER_ERROR, "state_lock_failed"),
    }
}

async fn rpc_qcash_mempool(State(state): State<RpcState>) -> impl IntoResponse {
    match state.node.lock() {
        Ok(node) => {
            let transactions = node
                .extension_mempool
                .transactions_for_family(paqus::transaction::TransactionFamily::QCash)
                .filter_map(|transaction| match transaction {
                    paqus::transaction::SignedProtocolTransaction::QCash(signed) => Some(signed),
                    _ => None,
                })
                .map(|signed| {
                    serde_json::json!({
                        "hash": hex::encode(signed.hash().0),
                        "signer": address_to_string(&signed.transaction.signer),
                        "nonce": signed.transaction.nonce.0,
                        "fee": signed.transaction.fee.0,
                        "kind": match signed.transaction.kind {
                            paqus::transaction::QCashTransactionKind::WithdrawCash { .. } => "withdraw",
                            paqus::transaction::QCashTransactionKind::DepositCash { .. } => "deposit",
                        },
                    })
                })
                .collect::<Vec<_>>();
            Json(serde_json::json!({
                "size": transactions.len(),
                "transactions": transactions,
            }))
            .into_response()
        }
        Err(_) => rpc_error(StatusCode::INTERNAL_SERVER_ERROR, "state_lock_failed"),
    }
}


fn block_response(node: &Node, block: &Block, status: Option<&'static str>) -> BlockResponse {
    let block_hash = block.hash();
    let tip_height = node.tip_height().unwrap_or(Height(0)).0;
    let height = block.height().0;
    let now = unix_timestamp().unwrap_or(block.timestamp());
    let previous_timestamp = height
        .checked_sub(1)
        .and_then(|previous_height| {
            node.storage
                .load_block_by_height(Height(previous_height))
                .ok()
        })
        .flatten()
        .map(|previous_block| previous_block.timestamp());
    BlockResponse {
        version: block.header.version,
        height,
        hash: hex::encode(block_hash.0),
        short_hash: short_hash(Some(block_hash)),
        previous_hash: hex::encode(block.previous_hash().0),
        merkle_root: hex::encode(block.header.merkle_root.0),
        witness_root: hex::encode(block.header.witness_root.0),
        state_root: hex::encode(block.state_root().0),
        miner_address: address_to_string(&block.miner_address()),
        difficulty: block.difficulty(),
        timestamp: block.timestamp(),
        age_secs: now.saturating_sub(block.timestamp()),
        confirmations: tip_height.saturating_sub(height).saturating_add(1),
        block_time_secs: previous_timestamp
            .map(|timestamp| block.timestamp().saturating_sub(timestamp)),
        target_block_time_secs: BLOCK_TIME,
        block_time_delta_secs: previous_timestamp.map(|timestamp| {
            block.timestamp().saturating_sub(timestamp) as i64 - BLOCK_TIME as i64
        }),
        value_moved: block_protocol_transactions(block)
            .iter()
            .filter_map(|transaction| protocol_transaction_summary(transaction).2)
            .sum(),
        nonce: block.header.nonce.0,
        tx_count: block.transaction_count(),
        size: block.serialized_size(),
        stripped_size: block.stripped_size(),
        witness_size: block.witness_size(),
        weight: block.weight(),
        coinbase: block.coinbase.as_ref().map(|coinbase| CoinbaseResponse {
            to: address_to_string(&coinbase.to),
            subsidy: coinbase.subsidy.0,
            fees: coinbase.fees.0,
            total: coinbase.total().0,
        }),
        genesis_allocations: block
            .genesis_allocations
            .iter()
            .map(|allocation| GenesisAllocationResponse {
                to: address_to_string(&allocation.to),
                amount: allocation.amount.0,
            })
            .collect(),
        transactions: block_protocol_transactions(block)
            .iter()
            .map(|transaction| {
                protocol_tx_response(
                    transaction,
                    Some(block.height()),
                    Some(block_hash.into()),
                    status.unwrap_or("confirmed"),
                )
            })
            .collect(),
    }
}

pub(crate) fn protocol_tx_response(
    transaction: &SignedProtocolTransaction,
    block_height: Option<Height>,
    block_hash: Option<Hash>,
    status: &'static str,
) -> ProtocolTxResponse {
    let (operation, recipient, amount, timestamp) = protocol_transaction_summary(transaction);
    let now = unix_timestamp().unwrap_or(timestamp.unwrap_or(0));
    let validity = transaction.validity();
    ProtocolTxResponse {
        family: transaction_family_name(transaction.family()),
        operation,
        txid: hex::encode(transaction.hash().0),
        wtxid: hex::encode(transaction.wtxid().0),
        signer: address_to_string(&transaction.signer()),
        witness_addresses: transaction
            .witness_addresses()
            .iter()
            .map(address_to_string)
            .collect(),
        recipient: recipient.map(|address| address_to_string(&address)),
        amount,
        fee: transaction.fee().0,
        nonce: transaction.nonce().0,
        valid_from: validity.valid_from.0,
        valid_until: validity.valid_until.0,
        timestamp,
        age_secs: timestamp.map(|timestamp| now.saturating_sub(timestamp)),
        stripped_size: transaction.stripped_size(),
        witness_size: transaction.witness_size(),
        virtual_size: transaction.virtual_size(),
        block_height: block_height.map(|height| height.0),
        block_hash: block_hash.map(|hash| hex::encode(hash.0)),
        status,
    }
}

fn transaction_family_name(family: paqus::transaction::TransactionFamily) -> &'static str {
    use paqus::transaction::TransactionFamily;
    match family {
        TransactionFamily::Transfer => "transfer",
        TransactionFamily::QCash => "qcash",
    }
}

fn protocol_transaction_summary(
    transaction: &SignedProtocolTransaction,
) -> (&'static str, Option<Address>, Option<u64>, Option<u64>) {
    match transaction {
        SignedProtocolTransaction::Transfer(tx) => (
            "transfer",
            Some(tx.transaction.to),
            tx.transaction.total_amount().ok().map(|amount| amount.0),
            Some(tx.transaction.timestamp),
        ),
        SignedProtocolTransaction::QCash(tx) => match &tx.transaction.kind {
            paqus::transaction::QCashTransactionKind::WithdrawCash { amount, .. } => (
                "withdraw_cash",
                None,
                Some(amount.0),
                Some(tx.transaction.timestamp),
            ),
            paqus::transaction::QCashTransactionKind::DepositCash {
                recipient,
                metadata,
            } => (
                "deposit_cash",
                Some(*recipient),
                metadata.amount().ok().map(|amount| amount.0),
                Some(tx.transaction.timestamp),
            ),
        },
    }
}

fn protocol_transaction_addresses(transaction: &SignedProtocolTransaction) -> Vec<Address> {
    let mut addresses = vec![transaction.signer()];
    if let Some(recipient) = protocol_transaction_summary(transaction).1
        && recipient != transaction.signer()
    {
        addresses.push(recipient);
    }
    addresses
}

fn block_protocol_transactions(block: &Block) -> Vec<SignedProtocolTransaction> {
    block
        .transactions
        .iter()
        .cloned()
        .map(SignedProtocolTransaction::Transfer)
        .chain(
            block
                .qcash_transactions
                .iter()
                .cloned()
                .map(SignedProtocolTransaction::QCash),
        )
        .collect()
}

fn find_transaction(node: &Node, hash: &Hash) -> Result<Option<ProtocolTxResponse>, String> {
    for transaction in node.mempool.transactions() {
        let transaction = SignedProtocolTransaction::Transfer(transaction.clone());
        if transaction.hash() == *hash || transaction.wtxid().0 == hash.0 {
            return Ok(Some(protocol_tx_response(
                &transaction,
                None,
                None,
                "pending",
            )));
        }
    }
    for transaction in node.extension_mempool.transactions() {
        if transaction.hash() == *hash || transaction.wtxid().0 == hash.0 {
            return Ok(Some(protocol_tx_response(
                transaction,
                None,
                None,
                "pending",
            )));
        }
    }

    let txid = TransactionHash(hash.0);
    if let Some((location, transaction)) = node
        .storage
        .load_protocol_transaction(&txid)
        .map_err(|error| format!("failed to load indexed transaction: {error}"))?
    {
        return Ok(Some(protocol_tx_response(
            &transaction,
            Some(location.block_height),
            Some(location.block_hash.into()),
            "confirmed",
        )));
    }
    let wtxid = WitnessTransactionHash(hash.0);
    if let Some((location, transaction)) =
        node.storage
            .load_protocol_transaction_by_wtxid(&wtxid)
            .map_err(|error| format!("failed to load indexed witness transaction: {error}"))?
    {
        return Ok(Some(protocol_tx_response(
            &transaction,
            Some(location.block_height),
            Some(location.block_hash.into()),
            "confirmed",
        )));
    }
    Ok(None)
}

fn address_activity(node: &Node, address: &Address) -> Result<AddressActivity, String> {
    let mut mined_blocks = Vec::new();
    let mut transactions = Vec::new();
    let tip = node.tip_height().unwrap_or(Height(0)).0;
    let mined_locations = node
        .storage
        .load_miner_block_locations(address)
        .map_err(|error| format!("failed to load miner block index: {error}"))?;
    for location in mined_locations {
        let height = location.block_height.0;
        let block = node
            .storage
            .load_block_by_height(location.block_height)
            .map_err(|error| format!("failed to load block: {error}"))?;
        let Some(block) = block else {
            continue;
        };
        if block.hash() != location.block_hash || block.miner_address() != *address {
            continue;
        }
        if let Some(coinbase) = block.coinbase.as_ref() {
            let maturity_height = height.saturating_add(BLOCK_REWARD_MATURITY as u64);
            mined_blocks.push(MinedBlockResponse {
                height,
                hash: hex::encode(block.hash().0),
                confirmations: tip.saturating_sub(height).saturating_add(1),
                maturity_height,
                matured: tip >= maturity_height,
                subsidy: coinbase.subsidy.0,
                fees: coinbase.fees.0,
                total: coinbase.total().0,
                tx_count: block.transaction_count(),
                timestamp: block.timestamp(),
            });
        }
    }

    let locations = node
        .storage
        .load_address_transaction_locations(address)
        .map_err(|error| format!("failed to load address transaction index: {error}"))?;
    for location in locations {
        if let Some((_, transaction)) = node
            .storage
            .load_protocol_transaction(&location.tx_hash)
            .map_err(|error| format!("failed to load indexed transaction: {error}"))?
        {
            transactions.push(protocol_tx_response(
                &transaction,
                Some(location.block_height),
                Some(location.block_hash.into()),
                "confirmed",
            ));
        }
    }

    for transaction in node.mempool.transactions_for_address(address) {
        transactions.push(protocol_tx_response(
            &SignedProtocolTransaction::Transfer(transaction.clone()),
            None,
            None,
            "pending",
        ));
    }
    for transaction in node.extension_mempool.transactions() {
        if protocol_transaction_addresses(transaction).contains(address) {
            transactions.push(protocol_tx_response(transaction, None, None, "pending"));
        }
    }

    mined_blocks.reverse();
    transactions.reverse();
    Ok(AddressActivity {
        mined_blocks,
        transactions,
    })
}

fn chain_stats(node: &Node) -> Result<ChainStatsResponse, String> {
    let tip = node.tip_height().unwrap_or(Height(0)).0;
    let mut blocks = 0u64;
    let mut mined_supply = 0u64;
    let mut total_coinbase_rewards = 0u64;
    let mut total_fees_collected = 0u64;
    let mut total_transactions = 0u64;
    let mut total_transfer_volume = 0u64;
    let mut total_transaction_fees = 0u64;
    let mut previous_timestamp = None;
    let mut total_block_time_secs = 0u64;
    let mut block_time_samples = 0u64;

    for height in 0..=tip {
        let block = node
            .storage
            .load_block_by_height(Height(height))
            .map_err(|error| format!("failed to load block: {error}"))?;
        let Some(block) = block else {
            continue;
        };
        blocks = blocks.saturating_add(1);
        if let Some(previous_timestamp) = previous_timestamp {
            total_block_time_secs = total_block_time_secs
                .saturating_add(block.timestamp().saturating_sub(previous_timestamp));
            block_time_samples = block_time_samples.saturating_add(1);
        }
        previous_timestamp = Some(block.timestamp());
        if let Some(coinbase) = block.coinbase.as_ref() {
            mined_supply = mined_supply.saturating_add(coinbase.subsidy.0);
            total_fees_collected = total_fees_collected.saturating_add(coinbase.fees.0);
            total_coinbase_rewards = total_coinbase_rewards.saturating_add(coinbase.total().0);
        }
        total_transactions = total_transactions.saturating_add(block.transaction_count() as u64);
        total_transaction_fees = total_transaction_fees
            .saturating_add(block.checked_total_fees().map(|fees| fees.0).unwrap_or(0));
        for transaction in &block.transactions {
            total_transfer_volume = total_transfer_volume.saturating_add(
                transaction
                    .transaction
                    .total_amount()
                    .map(|amount| amount.0)
                    .unwrap_or(0),
            );
        }
    }

    let pending_transactions = (node.mempool.len() + node.extension_mempool.len()) as u64;
    let average_transfer_amount = total_transfer_volume
        .checked_div(total_transactions)
        .unwrap_or(0);
    let current_supply = GENESIS_PREMINE.saturating_add(mined_supply);
    let average_block_time_secs = total_block_time_secs.checked_div(block_time_samples);

    Ok(ChainStatsResponse {
        chain: CHAIN_NAME,
        coin: COIN_NAME,
        height: tip,
        blocks,
        average_block_time_secs,
        target_block_time_secs: BLOCK_TIME,
        genesis_premine: GENESIS_PREMINE,
        mined_supply,
        current_supply,
        total_coinbase_rewards,
        total_fees_collected,
        total_transactions,
        pending_transactions,
        total_transfer_volume,
        total_transaction_fees,
        average_transfer_amount,
    })
}
