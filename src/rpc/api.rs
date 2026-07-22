use crate::*;

#[derive(Debug, Deserialize)]
struct SubmitTxRequest {
    tx: String,
}

#[derive(Debug, Deserialize)]
struct MiningTemplateQuery {
    miner: String,
}

#[derive(Debug, Deserialize)]
struct SubmitBlockRequest {
    block: String,
}

#[derive(Serialize)]
struct MiningTemplateResponse {
    job_id: String,
    block: String,
    height: u64,
    previous_hash: String,
    difficulty: u32,
    algorithm: &'static str,
}

#[derive(Serialize)]
struct SubmitBlockResponse {
    accepted: bool,
    height: u64,
    hash: String,
}

#[derive(Clone)]
pub(crate) struct RpcState {
    pub(crate) node: Arc<Mutex<Node>>,
    pub(crate) peers: Arc<Mutex<HashMap<SocketAddr, PeerState>>>,
    pub(crate) peer_connections: Arc<Mutex<HashMap<SocketAddr, PeerConnection>>>,
    pub(crate) inbound_connections: Arc<Mutex<HashMap<SocketAddr, PeerConnection>>>,
    pub(crate) mining: bool,
    pub(crate) log_counters: Arc<LogCounters>,
    pub(crate) mining_stats: Arc<MiningStats>,
    pub(crate) metrics: Arc<RpcMetrics>,
    pub(crate) db_path: String,
}

#[derive(Default)]
pub(crate) struct RpcMetrics {
    pub(crate) requests_total: AtomicU64,
    pub(crate) errors_total: AtomicU64,
    pub(crate) latency_micros_total: AtomicU64,
}

#[derive(Default)]
pub(crate) struct LogCounters {
    pub(crate) accepted_tx_total: AtomicU64,
    pub(crate) broadcast_tx_total: AtomicU64,
}

#[derive(Serialize)]
struct StatusResponse {
    chain: &'static str,
    stage: &'static str,
    protocol_version: u8,
    pow_algorithm: &'static str,
    difficulty_algorithm: &'static str,
    height: u64,
    tip_hash: String,
    peers: usize,
    known_peers: usize,
    outbound_peers: usize,
    inbound_peers: usize,
    mining: bool,
    hashrate_hps: u64,
    last_mine_attempts: u64,
}

#[derive(Serialize)]
struct HealthResponse {
    ok: bool,
}

#[derive(Serialize)]
struct PeerResponse {
    addr: String,
    failures: u32,
    last_tip: Option<u64>,
}

#[derive(Serialize)]
struct SubmitTxResponse {
    accepted: bool,
    hash: String,
    wtxid: String,
}

#[derive(Serialize)]
struct ErrorResponse {
    error: String,
}

#[derive(Serialize)]
struct ChainResponse {
    chain: &'static str,
    coin: &'static str,
    stage: &'static str,
    protocol_version: u8,
    pow_algorithm: &'static str,
    difficulty_algorithm: &'static str,
    asert_half_life_secs: u64,
    block_time_secs: u32,
    confirmation_depth: u32,
    finality_depth: u32,
    block_reward_maturity: u32,
    difficulty_start: u32,
}

#[derive(Serialize)]
struct ChainStatsResponse {
    chain: &'static str,
    coin: &'static str,
    height: u64,
    blocks: u64,
    average_block_time_secs: Option<u64>,
    target_block_time_secs: u32,
    genesis_premine: u64,
    mined_supply: u64,
    onchain_supply: u64,
    qcash_offchain_supply: u64,
    qcash_spendable_supply: u64,
    qcash_pending_supply: u64,
    total_known_supply: u64,
    current_supply: u64,
    total_coinbase_rewards: u64,
    total_fees_collected: u64,
    total_transactions: u64,
    pending_transactions: u64,
    total_transfer_volume: u64,
    total_transaction_fees: u64,
    average_transfer_amount: u64,
}

#[derive(Serialize)]
pub(crate) struct ProtocolTxResponse {
    family: &'static str,
    operation: &'static str,
    txid: String,
    wtxid: String,
    signer: String,
    witness_addresses: Vec<String>,
    recipient: Option<String>,
    amount: Option<u64>,
    fee: u64,
    nonce: u64,
    valid_from: u64,
    valid_until: u64,
    timestamp: Option<u64>,
    age_secs: Option<u64>,
    stripped_size: usize,
    witness_size: usize,
    virtual_size: usize,
    block_height: Option<u64>,
    block_hash: Option<String>,
    status: &'static str,
}

#[derive(Serialize)]
struct CoinbaseResponse {
    to: String,
    subsidy: u64,
    fees: u64,
    total: u64,
}

#[derive(Serialize)]
struct GenesisAllocationResponse {
    to: String,
    amount: u64,
}

#[derive(Serialize)]
struct BlockResponse {
    version: u8,
    height: u64,
    hash: String,
    short_hash: String,
    previous_hash: String,
    merkle_root: String,
    witness_root: String,
    state_root: String,
    miner_address: String,
    difficulty: u32,
    timestamp: u64,
    age_secs: u64,
    confirmations: u64,
    block_time_secs: Option<u64>,
    target_block_time_secs: u32,
    block_time_delta_secs: Option<i64>,
    value_moved: u64,
    nonce: u64,
    tx_count: usize,
    size: usize,
    stripped_size: usize,
    witness_size: usize,
    weight: usize,
    coinbase: Option<CoinbaseResponse>,
    genesis_allocations: Vec<GenesisAllocationResponse>,
    transactions: Vec<ProtocolTxResponse>,
}

#[derive(Serialize)]
struct MinedBlockResponse {
    height: u64,
    hash: String,
    confirmations: u64,
    maturity_height: u64,
    matured: bool,
    subsidy: u64,
    fees: u64,
    total: u64,
    tx_count: usize,
    timestamp: u64,
}

struct AddressActivity {
    mined_blocks: Vec<MinedBlockResponse>,
    transactions: Vec<ProtocolTxResponse>,
}

#[derive(Serialize)]
struct AddressResponse {
    address: String,
    balance: serde_json::Value,
    mined_blocks: Vec<MinedBlockResponse>,
    transactions: Vec<ProtocolTxResponse>,
}

#[derive(Serialize)]
struct AccountResponse {
    address: String,
    confirmed: u64,
    available: u64,
    unspendable: u64,
    pending_incoming: u64,
    pending_outgoing: u64,
    nonce: u64,
    credits: usize,
}

#[derive(Serialize)]
struct MempoolResponse {
    size: usize,
    transactions: Vec<ProtocolTxResponse>,
}

#[derive(Serialize)]
pub(crate) struct ProtocolEventResponse {
    id: String,
    pub(crate) event: ProtocolEvent,
}

#[derive(Debug, Default, Deserialize)]
pub(crate) struct EventQuery {
    pub(crate) offset: Option<usize>,
    pub(crate) limit: Option<usize>,
    pub(crate) kind: Option<String>,
    pub(crate) from_height: Option<u64>,
    pub(crate) to_height: Option<u64>,
}

#[derive(Debug, Default, Deserialize)]
struct EventStreamQuery {
    from_height: Option<u64>,
    kind: Option<String>,
    address: Option<String>,
}

struct ProtocolEventStreamState {
    rpc: RpcState,
    next_height: u64,
    kind: Option<String>,
    address: Option<Address>,
    pending: VecDeque<ProtocolEvent>,
    poll_immediately: bool,
}

#[derive(Serialize)]
pub(crate) struct ProtocolEventListResponse {
    pub(crate) total: usize,
    offset: usize,
    limit: usize,
    pub(crate) events: Vec<ProtocolEventResponse>,
}

pub(crate) fn start_rpc_server(
    state: RpcState,
    addr: SocketAddr,
) -> Result<thread::JoinHandle<()>, String> {
    let metrics = state.metrics.clone();
    let app = Router::new()
        .route("/", get(rpc_status))
        .route("/status", get(rpc_status))
        .route("/health", get(rpc_health))
        .route("/metrics", get(rpc_metrics))
        .route("/chain", get(rpc_chain))
        .route("/stats", get(rpc_stats))
        .route("/chain/stats", get(rpc_stats))
        .route("/peers", get(rpc_peers))
        .route("/balance/{address}", get(rpc_balance))
        .route("/blocks/latest", get(rpc_latest_blocks))
        .route("/blocks/{height}", get(rpc_block_by_height))
        .route("/blocks/hash/{hash}", get(rpc_block_by_hash))
        .route("/blocks/{height}/events", get(rpc_block_events))
        .route("/tx/{hash}", get(rpc_tx))
        .route("/tx/{hash}/events", get(rpc_transaction_events))
        .route("/address/{address}", get(rpc_address))
        .route("/address/{address}/events", get(rpc_address_events))
        .route("/events/stream", get(rpc_event_stream))
        .route("/events/{id}", get(rpc_event))
        .route("/accounts", get(rpc_accounts))
        .route("/mempool", get(rpc_mempool))
        .route("/qcash/mempool", get(rpc_qcash_mempool))
        .route("/qcash/coin/{coin_id}", get(rpc_qcash_coin))
        .route("/mining/template", get(rpc_mining_template))
        .route("/mining/submit", post(rpc_submit_mined_block))
        .route("/tx", post(rpc_submit_tx))
        .route("/transaction", post(rpc_submit_tx))
        .route("/protocol/transaction", post(rpc_submit_protocol_tx))
        .route("/qcash/tx", post(rpc_submit_qcash_tx))
        .layer(middleware::from_fn_with_state(metrics, track_rpc_request))
        .with_state(state);

    thread::Builder::new()
        .name("paqus-rpc".to_string())
        .spawn(move || {
            let runtime = match tokio::runtime::Runtime::new() {
                Ok(runtime) => runtime,
                Err(error) => {
                    eprintln!("[RPC] runtime_failed error=\"{error}\"");
                    return;
                }
            };
            runtime.block_on(async move {
                let listener = match tokio::net::TcpListener::bind(addr).await {
                    Ok(listener) => listener,
                    Err(error) => {
                        eprintln!("[RPC] bind_failed addr={addr} error=\"{error}\"");
                        return;
                    }
                };
                println!("[RPC] listening addr={addr}");
                if let Err(error) = axum::serve(listener, app).await {
                    eprintln!("[RPC] server_failed error=\"{error}\"");
                }
            });
        })
        .map_err(|error| format!("failed to spawn rpc server: {error}"))
}

async fn track_rpc_request(
    State(metrics): State<Arc<RpcMetrics>>,
    request: Request<Body>,
    next: Next,
) -> axum::response::Response {
    let started = Instant::now();
    let response = next.run(request).await;
    metrics.requests_total.fetch_add(1, Ordering::Relaxed);
    metrics.latency_micros_total.fetch_add(
        started.elapsed().as_micros().min(u128::from(u64::MAX)) as u64,
        Ordering::Relaxed,
    );
    if response.status().is_client_error() || response.status().is_server_error() {
        metrics.errors_total.fetch_add(1, Ordering::Relaxed);
    }
    response
}

async fn rpc_metrics(State(state): State<RpcState>) -> impl IntoResponse {
    let (height, mempool_size, validation_failures, reorgs) = state
        .node
        .lock()
        .map(|node| {
            (
                node.tip_height().unwrap_or(Height(0)).0,
                node.mempool.len() + node.extension_mempool.len(),
                node.block_validation_failures_total(),
                node.reorgs_total(),
            )
        })
        .unwrap_or_default();
    let peer_count = state
        .peer_connections
        .lock()
        .map(|peers| peers.len())
        .unwrap_or_default()
        + state
            .inbound_connections
            .lock()
            .map(|peers| peers.len())
            .unwrap_or_default();
    let database_bytes = fs::metadata(std::path::Path::new(&state.db_path).join("data.mdb"))
        .map(|metadata| metadata.len())
        .unwrap_or_default();
    let body = format!(
        concat!(
            "# TYPE paqus_chain_height gauge\npaqus_chain_height {height}\n",
            "# TYPE paqus_peer_count gauge\npaqus_peer_count {peer_count}\n",
            "# TYPE paqus_mempool_size gauge\npaqus_mempool_size {mempool_size}\n",
            "# TYPE paqus_block_validation_failures_total counter\npaqus_block_validation_failures_total {validation_failures}\n",
            "# TYPE paqus_reorgs_total counter\npaqus_reorgs_total {reorgs}\n",
            "# TYPE paqus_rpc_requests_total counter\npaqus_rpc_requests_total {requests}\n",
            "# TYPE paqus_rpc_errors_total counter\npaqus_rpc_errors_total {errors}\n",
            "# TYPE paqus_rpc_latency_seconds summary\npaqus_rpc_latency_seconds_sum {latency_seconds:.6}\npaqus_rpc_latency_seconds_count {requests}\n",
            "# TYPE paqus_mining_hashrate_hps gauge\npaqus_mining_hashrate_hps {hashrate}\n",
            "# TYPE paqus_database_size_bytes gauge\npaqus_database_size_bytes {database_bytes}\n"
        ),
        height = height,
        peer_count = peer_count,
        mempool_size = mempool_size,
        validation_failures = validation_failures,
        reorgs = reorgs,
        requests = state.metrics.requests_total.load(Ordering::Relaxed),
        errors = state.metrics.errors_total.load(Ordering::Relaxed),
        latency_seconds =
            state.metrics.latency_micros_total.load(Ordering::Relaxed) as f64 / 1_000_000.0,
        hashrate = state.mining_stats.last_hashrate_hps.load(Ordering::Relaxed),
        database_bytes = database_bytes,
    );
    ([(header::CONTENT_TYPE, "text/plain; version=0.0.4")], body)
}

async fn rpc_status(State(state): State<RpcState>) -> impl IntoResponse {
    match (
        state.node.lock(),
        state.peers.lock(),
        state.peer_connections.lock(),
        state.inbound_connections.lock(),
    ) {
        (Ok(node), Ok(peers), Ok(outbound), Ok(inbound)) => Json(StatusResponse {
            chain: CHAIN_NAME,
            stage: PROTOCOL_STAGE,
            protocol_version: PROTOCOL_VERSION,
            pow_algorithm: CURRENT_CHAIN_PARAMS.pow_algorithm,
            difficulty_algorithm: CURRENT_CHAIN_PARAMS.difficulty_algorithm,
            height: node.tip_height().unwrap_or(Height(0)).0,
            tip_hash: format_hash(node.tip_hash()),
            peers: peers.len(),
            known_peers: peers.len(),
            outbound_peers: outbound.len(),
            inbound_peers: inbound.len(),
            mining: state.mining,
            hashrate_hps: state.mining_stats.last_hashrate_hps.load(Ordering::Relaxed),
            last_mine_attempts: state.mining_stats.last_attempts.load(Ordering::Relaxed),
        })
        .into_response(),
        _ => rpc_error(StatusCode::INTERNAL_SERVER_ERROR, "state_lock_failed"),
    }
}

async fn rpc_health() -> impl IntoResponse {
    Json(HealthResponse { ok: true })
}

async fn rpc_chain() -> impl IntoResponse {
    Json(ChainResponse {
        chain: CHAIN_NAME,
        coin: COIN_NAME,
        stage: PROTOCOL_STAGE,
        protocol_version: PROTOCOL_VERSION,
        pow_algorithm: CURRENT_CHAIN_PARAMS.pow_algorithm,
        difficulty_algorithm: CURRENT_CHAIN_PARAMS.difficulty_algorithm,
        asert_half_life_secs: ASERT_HALF_LIFE,
        block_time_secs: BLOCK_TIME,
        confirmation_depth: CONFIRMATION_DEPTH,
        finality_depth: FINALITY_DEPTH,
        block_reward_maturity: BLOCK_REWARD_MATURITY,
        difficulty_start: DIFFICULTY_START,
    })
}

async fn rpc_stats(State(state): State<RpcState>) -> impl IntoResponse {
    match state.node.lock() {
        Ok(node) => match chain_stats(&node) {
            Ok(stats) => Json(stats).into_response(),
            Err(error) => rpc_error(StatusCode::INTERNAL_SERVER_ERROR, error),
        },
        Err(_) => rpc_error(StatusCode::INTERNAL_SERVER_ERROR, "state_lock_failed"),
    }
}

async fn rpc_peers(State(state): State<RpcState>) -> impl IntoResponse {
    match state.peers.lock() {
        Ok(peers) => {
            let peers = peers
                .values()
                .map(|peer| PeerResponse {
                    addr: peer.addr.to_string(),
                    failures: peer.failures,
                    last_tip: peer.last_tip.map(|height| height.0),
                })
                .collect::<Vec<_>>();
            Json(peers).into_response()
        }
        Err(_) => rpc_error(StatusCode::INTERNAL_SERVER_ERROR, "state_lock_failed"),
    }
}

async fn rpc_balance(
    State(state): State<RpcState>,
    AxumPath(address): AxumPath<String>,
) -> impl IntoResponse {
    let address = match parse_address_string(&address) {
        Ok(address) => address,
        Err(error) => return rpc_error(StatusCode::BAD_REQUEST, error),
    };
    match state.node.lock() {
        Ok(node) => (
            StatusCode::OK,
            [("content-type", "application/json")],
            balance_json(&node, &address),
        )
            .into_response(),
        Err(_) => rpc_error(StatusCode::INTERNAL_SERVER_ERROR, "state_lock_failed"),
    }
}

async fn rpc_latest_blocks(State(state): State<RpcState>) -> impl IntoResponse {
    match state.node.lock() {
        Ok(node) => {
            let tip = node.tip_height().unwrap_or(Height(0)).0;
            let start = tip.saturating_sub(9);
            let mut blocks = Vec::new();
            for height in (start..=tip).rev() {
                match node.storage.load_block_by_height(Height(height)) {
                    Ok(Some(block)) => blocks.push(block_response(&node, &block, None)),
                    Ok(None) => {}
                    Err(error) => {
                        return rpc_error(
                            StatusCode::INTERNAL_SERVER_ERROR,
                            format!("failed to load block: {error}"),
                        );
                    }
                }
            }
            Json(blocks).into_response()
        }
        Err(_) => rpc_error(StatusCode::INTERNAL_SERVER_ERROR, "state_lock_failed"),
    }
}

async fn rpc_block_by_height(
    State(state): State<RpcState>,
    AxumPath(height): AxumPath<u64>,
) -> impl IntoResponse {
    match state.node.lock() {
        Ok(node) => match node.storage.load_block_by_height(Height(height)) {
            Ok(Some(block)) => Json(block_response(&node, &block, None)).into_response(),
            Ok(None) => rpc_error(StatusCode::NOT_FOUND, "block_not_found"),
            Err(error) => rpc_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to load block: {error}"),
            ),
        },
        Err(_) => rpc_error(StatusCode::INTERNAL_SERVER_ERROR, "state_lock_failed"),
    }
}

async fn rpc_block_by_hash(
    State(state): State<RpcState>,
    AxumPath(hash): AxumPath<String>,
) -> impl IntoResponse {
    let hash = match parse_hash_hex(&hash) {
        Ok(hash) => hash,
        Err(error) => return rpc_error(StatusCode::BAD_REQUEST, error),
    };
    let block_hash = BlockHash::from(hash);
    match state.node.lock() {
        Ok(node) => match node.storage.load_block_by_hash(&block_hash) {
            Ok(Some(block)) => Json(block_response(&node, &block, None)).into_response(),
            Ok(None) => rpc_error(StatusCode::NOT_FOUND, "block_not_found"),
            Err(error) => rpc_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to load block: {error}"),
            ),
        },
        Err(_) => rpc_error(StatusCode::INTERNAL_SERVER_ERROR, "state_lock_failed"),
    }
}

fn rpc_error(status: StatusCode, error: impl Into<String>) -> axum::response::Response {
    (
        status,
        Json(ErrorResponse {
            error: error.into(),
        }),
    )
        .into_response()
}

include!("events.rs");
include!("explorer.rs");
include!("transactions.rs");
include!("mining.rs");
