fn run_node(args: &[String]) -> Result<(), String> {
    let mut config = parse_run_config(args)?;
    print_core_startup_info();
    warn_if_public_rpc(&config);
    if let Some(path) = &config.peers_file {
        config.peers.extend(load_peers_file(path)?);
    }
    dedupe_peers(&mut config.peers);
    if config.peers.len() > config.max_peers {
        config.peers.truncate(config.max_peers);
    }
    let mut node = open_node(&config.db_path, config.miner_address)?;
    node.mempool = runtime::mempool::Mempool::with_config(MempoolConfig {
        min_relay_fee: config.min_relay_fee,
        market_fee: config.market_fee,
        low_fee_ttl_secs: config.low_fee_expiry.as_secs(),
        transaction_ttl_secs: config.mempool_expiry.as_secs(),
        ..MempoolConfig::default()
    });
    if config.listen_addrs.is_empty() {
        return Err("at least one --listen address is required".to_string());
    }
    dedupe_socket_addrs(&mut config.listen_addrs);
    dedupe_socket_addrs(&mut config.public_addrs);
    let mut listeners = Vec::new();
    let mut bound_addrs = Vec::new();
    for addr in &config.listen_addrs {
        let listener = bind_nonblocking(*addr, "p2p")?;
        bound_addrs.push(
            listener
                .local_addr()
                .map_err(|error| format!("failed to read listener address: {error}"))?,
        );
        listeners.push(listener);
    }
    let node = Arc::new(Mutex::new(node));
    let log_counters = Arc::new(LogCounters::default());
    let mining_stats = Arc::new(MiningStats::default());

    let mut service = NodeService::new(
        node.clone(),
        config,
        listeners,
        log_counters.clone(),
        mining_stats.clone(),
    );
    service.preflight()?;

    let (height, tip_hash, difficulty, dynamic_market_fee_rate) = {
        let node = node
            .lock()
            .map_err(|_| "node state lock poisoned".to_string())?;
        (
            node.tip_height().unwrap_or(Height(0)).0,
            short_hash(node.tip_hash()),
            format_difficulty(node.next_difficulty()),
            node.mempool.dynamic_market_fee_rate(),
        )
    };

    println!(
        "Paqus Node db::{}|p2p::{}|rpc::{}|height::{}|tip::{}|difficulty::{}|peers::{}|mining::{}|min_relay_fee_rate_per_byte::{}|base_market_fee_rate_per_byte::{}|dynamic_market_fee_rate_per_byte::{}|miner_min_fee_rate_per_byte::{}|low_fee_expiry::{}s|mempool_expiry::{}s",
        service.config.db_path,
        format_socket_addrs(&bound_addrs),
        service.config.rpc_addr,
        height,
        tip_hash,
        difficulty,
        service.config.peers.len(),
        service.config.mine,
        service.config.min_relay_fee,
        service.config.market_fee,
        dynamic_market_fee_rate,
        service
            .config
            .miner_min_fee_rate
            .map(|rate| rate.to_string())
            .unwrap_or_else(|| "dynamic".to_string()),
        service.config.low_fee_expiry.as_secs(),
        service.config.mempool_expiry.as_secs()
    );

    let rpc_state = RpcState {
        node,
        peers: service.peers.clone(),
        peer_connections: service.peer_connections.clone(),
        inbound_connections: service.inbound_connections.clone(),
        mining: service.config.mine,
        log_counters,
        mining_stats,
        metrics: Arc::new(RpcMetrics::default()),
        db_path: service.config.db_path.clone(),
    };
    let _rpc_handle = start_rpc_server(rpc_state, service.config.rpc_addr)?;
    service.run()
}
fn peer_state_sync_window(
    peers: &Arc<Mutex<HashMap<SocketAddr, PeerState>>>,
    peer: SocketAddr,
) -> u64 {
    peers
        .lock()
        .ok()
        .and_then(|peers| peers.get(&peer).map(|state| state.sync_window))
        .unwrap_or(64)
}

fn max_peer_sync_window(
    peers: &Arc<Mutex<HashMap<SocketAddr, PeerState>>>,
    due_peers: &[SocketAddr],
) -> u64 {
    peers
        .lock()
        .ok()
        .map(|peers| {
            due_peers
                .iter()
                .filter_map(|peer| peers.get(peer).map(|state| state.sync_window))
                .max()
                .unwrap_or(64)
        })
        .unwrap_or(64)
}

fn warn_if_public_rpc(config: &RunConfig) {
    let ip = config.rpc_addr.ip();
    if ip.is_loopback() {
        return;
    }
    eprintln!(
        "warning: rpc is listening on {}; keep fullnode rpc internal and expose public traffic through paqus-gateway",
        config.rpc_addr
    );
}

fn print_core_startup_info() {
    println!(
        "core chain::{}|chain_id::{}|coin::{}|stage::{}|protocol::{}|storage::{}|magic::{}",
        CHAIN_NAME,
        CHAIN_ID,
        COIN_NAME,
        PROTOCOL_STAGE,
        PROTOCOL_VERSION,
        STORAGE_VERSION,
        hex::encode(NETWORK_MAGIC)
    );
    println!(
        "consensus: block_time::{}s|confirmation::{}|finality::{}|reward_maturity::{}|difficulty_start::{}|asert_half_life::{}s",
        BLOCK_TIME,
        CONFIRMATION_DEPTH,
        FINALITY_DEPTH,
        BLOCK_REWARD_MATURITY,
        DIFFICULTY_START,
        ASERT_HALF_LIFE
    );
}

