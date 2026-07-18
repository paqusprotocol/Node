use crate::rpc::transport::http_get;
use paqus::block::Nonce;
use paqus::crypto::{Address, address_to_string};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct BalanceRpcResponse {
    nonce: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct MempoolRpcResponse {
    transactions: Vec<MempoolTxRpcResponse>,
}

#[derive(Debug, Deserialize)]
struct MempoolTxRpcResponse {
    from: String,
    nonce: u64,
}

pub fn resolve_wallet_nonce(address: &Address, rpc_addr: &str) -> Result<Nonce, String> {
    let address_hex = address_to_string(address);
    let balance_body = http_get(rpc_addr, &format!("/balance/{address_hex}"))?;
    let balance: BalanceRpcResponse = serde_json::from_str(&balance_body)
        .map_err(|error| format!("failed to parse balance rpc response: {error}"))?;
    let mut next_nonce = balance.nonce.unwrap_or(0);

    let mempool_body = http_get(rpc_addr, "/mempool")?;
    let mempool: MempoolRpcResponse = serde_json::from_str(&mempool_body)
        .map_err(|error| format!("failed to parse mempool rpc response: {error}"))?;
    let mut pending_nonces = mempool
        .transactions
        .into_iter()
        .filter_map(|transaction| (transaction.from == address_hex).then_some(transaction.nonce))
        .collect::<Vec<_>>();
    pending_nonces.sort_unstable();
    pending_nonces.dedup();

    for nonce in pending_nonces {
        if nonce == next_nonce {
            next_nonce = next_nonce.saturating_add(1);
        } else if nonce > next_nonce {
            break;
        }
    }

    Ok(Nonce(next_nonce))
}
