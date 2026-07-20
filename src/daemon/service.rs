struct NodeService {
    node: Arc<Mutex<Node>>,
    config: RunConfig,
    listeners: Vec<TcpListener>,
    peers: Arc<Mutex<HashMap<SocketAddr, PeerState>>>,
    peer_connections: Arc<Mutex<HashMap<SocketAddr, PeerConnection>>>,
    inbound_connections: Arc<Mutex<HashMap<SocketAddr, PeerConnection>>>,
    log_counters: Arc<LogCounters>,
    mining_stats: Arc<MiningStats>,
    requires_peer_sync_before_mining: bool,
    last_mine: Instant,
    last_status: Instant,
    last_gateway_heartbeat: Instant,
    last_activity_log: Instant,
    last_activity: NodeActivity,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NodeActivity {
    Starting,
    WaitingForPeers,
    WaitingForTransactions,
    Mining,
    Syncing,
    ServingPeers,
}

impl NodeService {
    fn new(
        node: Arc<Mutex<Node>>,
        config: RunConfig,
        listeners: Vec<TcpListener>,
        log_counters: Arc<LogCounters>,
        mining_stats: Arc<MiningStats>,
    ) -> Self {
        let requires_peer_sync_before_mining =
            config.mine && (!config.peers.is_empty() || config.gateway_url.is_some());
        let peers = config
            .peers
            .iter()
            .copied()
            .map(|peer| (peer, PeerState::new(peer)))
            .collect();
        let last_gateway_heartbeat = Instant::now()
            .checked_sub(config.gateway_heartbeat)
            .unwrap_or_else(Instant::now);
        Self {
            node,
            config,
            listeners,
            peers: Arc::new(Mutex::new(peers)),
            peer_connections: Arc::new(Mutex::new(HashMap::new())),
            inbound_connections: Arc::new(Mutex::new(HashMap::new())),
            log_counters,
            mining_stats,
            requires_peer_sync_before_mining,
            last_mine: Instant::now(),
            last_status: Instant::now(),
            last_gateway_heartbeat,
            last_activity_log: Instant::now()
                .checked_sub(ACTIVITY_LOG_INTERVAL)
                .unwrap_or_else(Instant::now),
            last_activity: NodeActivity::Starting,
        }
    }

    fn preflight(&mut self) -> Result<(), String> {
        if fs::metadata(&self.config.shutdown_file).is_ok() {
            return Err(format!(
                "shutdown file `{}` exists; remove it before starting the node",
                self.config.shutdown_file
            ));
        }

        {
            let node = self
                .node
                .lock()
                .map_err(|_| "node state lock poisoned".to_string())?;
            node.next_difficulty()
                .map_err(|error| format!("failed to calculate next difficulty: {error}"))?;
        }

        if self.config.mine
            && let Some(secret_key) = self.config.miner_secret_key.as_ref()
        {
            let public_key = derive_public_key(secret_key);
            let derived_address = address_from_public_key(&public_key);
            if derived_address != self.config.miner_address {
                return Err(format!(
                    "miner secret key does not match miner address {}",
                    address_to_string(&self.config.miner_address)
                ));
            }
        }

        self.refresh_gateway_peers(true);

        let peers = self
            .peers
            .lock()
            .map_err(|_| "peer state lock poisoned".to_string())?
            .keys()
            .copied()
            .collect::<Vec<_>>();
        if !peers.is_empty() {
            println!(
                "preflight peers={} checking handshake and catch-up",
                peers.len()
            );
        }

        for peer in peers {
            println!("preflight peer {peer} connecting");
            let result = PeerConnection::connect(peer).and_then(|mut connection| {
                println!("preflight peer {peer} connected; polling tip and sync state");
                let poll = poll_peer_connection(
                    &mut connection,
                    &self.node,
                    &self.config.public_addrs,
                    peer_state_sync_window(&self.peers, peer),
                )?;
                let infos = request_peers_connection(&mut connection).unwrap_or_else(|error| {
                    eprintln!("preflight peer {peer} discovery failed: {error}");
                    Vec::new()
                });
                if !infos.is_empty() {
                    println!("preflight peer {peer} discovered peers={}", infos.len());
                    if self.add_peer_infos(infos) {
                        let _ = self.save_peers();
                    }
                }
                if let Ok(accepted) = sync_mempool_connection(&mut connection, &self.node)
                    && accepted > 0
                {
                    println!("preflight mempool synced:: |peer::{peer}|txs::{accepted}|");
                }
                Ok(poll)
            });
            match result {
                Ok(PeerPoll::Idle { remote_tip }) => {
                    println!(
                        "preflight peer {peer} ok:: |remote_tip::{}|state::idle|",
                        remote_tip.0
                    );
                    if let Ok(mut peers) = self.peers.lock()
                        && let Some(state) = peers.get_mut(&peer)
                    {
                        state.mark_ok(Some(remote_tip));
                    }
                }
                Ok(PeerPoll::Synced {
                    remote_tip,
                    synced_blocks,
                }) => {
                    println!(
                        "preflight peer {peer} synced:: |remote_tip::{}|blocks::{}|",
                        remote_tip.0, synced_blocks
                    );
                    if let Ok(mut peers) = self.peers.lock()
                        && let Some(state) = peers.get_mut(&peer)
                    {
                        state.mark_synced(remote_tip, synced_blocks);
                    }
                }
                Err(error) => {
                    if let Ok(mut peers) = self.peers.lock()
                        && let Some(state) = peers.get_mut(&peer)
                    {
                        state.mark_failed();
                    }
                    eprintln!("preflight peer {peer} failed: {error}");
                }
            }
        }
        self.save_peers()?;

        let node = self
            .node
            .lock()
            .map_err(|_| "node state lock poisoned".to_string())?;
        println!(
            "preflight ok |height::{}|tip::{}|difficulty::{}|mempool::{}|mining::{}",
            node.tip_height().unwrap_or(Height(0)).0,
            short_hash(node.tip_hash()),
            format_difficulty(node.next_difficulty()),
            node.mempool.len() + node.extension_mempool.len(),
            self.config.mine
        );

        Ok(())
    }

    fn run(&mut self) -> Result<(), String> {
        loop {
            if SHUTDOWN_REQUESTED.load(Ordering::SeqCst)
                || fs::metadata(&self.config.shutdown_file).is_ok()
            {
                self.shutdown()?;
                return Ok(());
            }

            self.accept_p2p()?;
            self.sync_peers();
            if self.last_gateway_heartbeat.elapsed() >= self.config.gateway_heartbeat {
                self.refresh_gateway_peers(false);
            }

            self.log_activity()?;

            if self.config.mine && self.last_mine.elapsed() >= self.config.mine_interval {
                if let Some(reason) = self.mining_wait_reason()? {
                    println!("mining waiting:: |reason::{reason}|");
                } else {
                    self.set_activity(NodeActivity::Mining)?;
                    let block = mine_once_unlocked(&self.node, &self.config, &self.mining_stats)?;
                    if let Some(block) = block {
                        let height = block.height().0;
                        let hash = short_hash(Some(block.hash()));
                        let tx_count = block.transactions.len();
                        let report = self.broadcast(NetworkMessage::Block(block));
                        println!(
                            "broadcast block:: |height::{}|hash::{}|txs::{}|peers::{}|sent::{}|failed::{}|",
                            height, hash, tx_count, report.attempted, report.sent, report.failed
                        );
                    }
                }
                self.last_mine = Instant::now();
            }

            if self.last_status.elapsed() >= Duration::from_secs(30) {
                let node = self
                    .node
                    .lock()
                    .map_err(|_| "node state lock poisoned".to_string())?;
                let peer_count = self
                    .peers
                    .lock()
                    .map_err(|_| "peer state lock poisoned".to_string())?
                    .len();
                let outbound_count = self
                    .peer_connections
                    .lock()
                    .map_err(|_| "peer connection lock poisoned".to_string())?
                    .len();
                let inbound_count = self
                    .inbound_connections
                    .lock()
                    .map_err(|_| "inbound connection lock poisoned".to_string())?
                    .len();
                println!(
                    "status: |height::{}|tip::{}|difficulty::{}|known_peers::{}|outbound_peers::{}|inbound_peers::{}|mining::{}|hashrate_hps::{}|accepted_tx::{}|broadcast_tx::{}|",
                    node.tip_height().unwrap_or(Height(0)).0,
                    short_hash(node.tip_hash()),
                    format_difficulty(node.next_difficulty()),
                    peer_count,
                    outbound_count,
                    inbound_count,
                    self.config.mine,
                    self.mining_stats.last_hashrate_hps.load(Ordering::Relaxed),
                    self.log_counters.accepted_tx_total.load(Ordering::Relaxed),
                    self.log_counters.broadcast_tx_total.load(Ordering::Relaxed)
                );
                self.last_status = Instant::now();
            }

            thread::sleep(Duration::from_millis(50));
        }
    }

    fn shutdown(&mut self) -> Result<(), String> {
        let node = self
            .node
            .lock()
            .map_err(|_| "node state lock poisoned".to_string())?;
        node.flush_to_storage()
            .map_err(|error| format!("failed to flush node on shutdown: {error}"))?;
        self.save_peers()?;
        let peer_count = self
            .peers
            .lock()
            .map_err(|_| "peer state lock poisoned".to_string())?
            .len();
        println!(
            "shutdown height={} tip={} difficulty={} peers={}",
            node.tip_height().unwrap_or(Height(0)).0,
            short_hash(node.tip_hash()),
            format_difficulty(node.next_difficulty()),
            peer_count
        );
        Ok(())
    }

    fn log_activity(&mut self) -> Result<(), String> {
        let (mempool_len, mining, pending_sync) = {
            let node = self
                .node
                .lock()
                .map_err(|_| "node state lock poisoned".to_string())?;
            (
                node.mempool.len() + node.extension_mempool.len(),
                self.config.mine,
                node.has_pending_sync_work(),
            )
        };
        let peer_count = self
            .peers
            .lock()
            .map_err(|_| "peer state lock poisoned".to_string())?
            .len();

        let activity = if peer_count == 0 && self.config.gateway_url.is_some() {
            NodeActivity::WaitingForPeers
        } else if pending_sync
            || self.needs_peer_handshake_before_mining()?
            || self.peer_ahead_of_local_tip()?
        {
            NodeActivity::Syncing
        } else if mining && mempool_len == 0 {
            NodeActivity::WaitingForTransactions
        } else if mining {
            NodeActivity::Mining
        } else if peer_count > 0 {
            NodeActivity::Syncing
        } else {
            NodeActivity::ServingPeers
        };

        if activity != self.last_activity
            || self.last_activity_log.elapsed() >= ACTIVITY_LOG_INTERVAL
        {
            self.last_activity = activity;
            self.last_activity_log = Instant::now();
        }

        Ok(())
    }

    fn mining_wait_reason(&self) -> Result<Option<&'static str>, String> {
        let pending_sync = self
            .node
            .lock()
            .map_err(|_| "node state lock poisoned".to_string())?
            .has_pending_sync_work();
        if pending_sync {
            return Ok(Some("sync_pending"));
        }
        if self.needs_peer_handshake_before_mining()? {
            return Ok(Some("handshake_pending"));
        }
        if self.peer_ahead_of_local_tip()? {
            return Ok(Some("peer_ahead"));
        }
        Ok(None)
    }

    fn needs_peer_handshake_before_mining(&self) -> Result<bool, String> {
        if !self.requires_peer_sync_before_mining {
            return Ok(false);
        }
        let peers = self
            .peers
            .lock()
            .map_err(|_| "peer state lock poisoned".to_string())?;
        Ok(!peers.is_empty() && !peers.values().any(|peer| peer.last_tip.is_some()))
    }

    fn peer_ahead_of_local_tip(&self) -> Result<bool, String> {
        let local_height = self
            .node
            .lock()
            .map_err(|_| "node state lock poisoned".to_string())?
            .tip_height()
            .unwrap_or(Height(0))
            .0;
        let peers = self
            .peers
            .lock()
            .map_err(|_| "peer state lock poisoned".to_string())?;
        Ok(peers
            .values()
            .filter_map(|peer| peer.last_tip)
            .any(|height| height.0 > local_height))
    }

    fn set_activity(&mut self, activity: NodeActivity) -> Result<(), String> {
        if activity == self.last_activity
            && self.last_activity_log.elapsed() < ACTIVITY_LOG_INTERVAL
        {
            return Ok(());
        }
        self.last_activity = activity;
        self.last_activity_log = Instant::now();
        Ok(())
    }

    fn accept_p2p(&mut self) -> Result<(), String> {
        loop {
            let mut accepted = false;
            for index in 0..self.listeners.len() {
                match self.listeners[index].accept() {
                    Ok((stream, peer)) => {
                        accepted = true;
                        self.set_activity(NodeActivity::ServingPeers)?;
                        println!("p2p inbound:: |peer::{}|event::accepted|", peer);
                        let node = self.node.clone();
                        let peers = self.peers.clone();
                        let inbound_connections = self.inbound_connections.clone();
                        let public_addrs = self.config.public_addrs.clone();
                        let listen_addrs = self.config.listen_addrs.clone();
                        let max_peers = self.config.max_peers;
                        let peers_file = self.config.peers_file.clone();
                        if let Ok(mut inbound) = inbound_connections.lock() {
                            match stream.try_clone() {
                                Ok(writer) => match PeerConnection::from_stream(peer, writer) {
                                    Ok(connection) => {
                                        inbound.insert(peer, connection);
                                    }
                                    Err(error) => {
                                        eprintln!(
                                            "p2p inbound {peer} writer setup failed: {error}"
                                        );
                                    }
                                },
                                Err(error) => {
                                    eprintln!("p2p inbound {peer} clone failed: {error}");
                                }
                            }
                        }
                        thread::Builder::new()
                            .name(format!("paqus-p2p-{peer}"))
                            .spawn(move || {
                                if let Err(error) = Self::handle_p2p_stream_task(
                                    stream,
                                    peer,
                                    node,
                                    peers,
                                    public_addrs,
                                    listen_addrs,
                                    max_peers,
                                    peers_file,
                                ) {
                                    eprintln!("p2p inbound {peer} failed: {error}");
                                }
                                if let Ok(mut inbound) = inbound_connections.lock() {
                                    inbound.remove(&peer);
                                }
                            })
                            .map_err(|error| format!("failed to spawn p2p handler: {error}"))?;
                    }
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
                    Err(error) => return Err(format!("failed to accept peer: {error}")),
                }
            }
            if !accepted {
                return Ok(());
            }
        }
    }

    #[allow(clippy::too_many_arguments)] // Detached connection task receives its owned context.
    fn handle_p2p_stream_task(
        mut stream: TcpStream,
        peer: SocketAddr,
        node: Arc<Mutex<Node>>,
        peers: Arc<Mutex<HashMap<SocketAddr, PeerState>>>,
        public_addrs: Vec<SocketAddr>,
        listen_addrs: Vec<SocketAddr>,
        max_peers: usize,
        peers_file: Option<String>,
    ) -> Result<(), String> {
        configure_stream(&stream, PERSISTENT_PEER_TIMEOUT)?;
        loop {
            match read_message(&mut stream) {
                Ok(envelope) => {
                    let response = match envelope.message {
                        NetworkMessage::GetPeers => Some(NetworkMessage::Peers(
                            Self::peer_infos_from(&peers, &public_addrs),
                        )),
                        NetworkMessage::Peers(peer_infos) => {
                            if Self::add_peer_infos_to(
                                &peers,
                                peer_infos,
                                &public_addrs,
                                &listen_addrs,
                                max_peers,
                            ) {
                                let _ = Self::save_peers_from(&peers_file, &peers);
                            }
                            None
                        }
                        message => {
                            let inbound_log = inbound_message_log(&message, peer);
                            let mut node = node
                                .lock()
                                .map_err(|_| "node state lock poisoned".to_string())?;
                            let response = handle_message(&mut node, message).map_err(|error| {
                                format!("failed to handle message from {peer}: {error}")
                            })?;
                            if let Some(log) = inbound_log {
                                println!("{log}");
                            }
                            response
                        }
                    };
                    if let Some(response) = response {
                        write_message(&mut stream, &response.to_envelope())
                            .map_err(|error| format!("failed to respond to {peer}: {error}"))?;
                    }
                }
                Err(error) if is_peer_stream_closed(&error) => {
                    break;
                }
                Err(error) => {
                    eprintln!("peer {peer} sent invalid message: {error}");
                    break;
                }
            }
        }
        Ok(())
    }

    fn sync_peers(&mut self) {
        let due_peers = match self.peers.lock() {
            Ok(peers) => peers
                .iter()
                .filter_map(|(addr, peer)| (Instant::now() >= peer.next_attempt).then_some(*addr))
                .collect::<Vec<_>>(),
            Err(_) => {
                eprintln!("peer state lock poisoned");
                return;
            }
        };

        if due_peers.len() > 1 {
            let sync_window = max_peer_sync_window(&self.peers, &due_peers);
            match sync_from_peers_parallel(
                due_peers.clone(),
                &self.node,
                &self.config.public_addrs,
                sync_window,
            ) {
                Ok(report) if report.applied_blocks > 0 => {
                    if let Ok(mut peers) = self.peers.lock() {
                        for peer in &report.used_peer_addrs {
                            if let Some(state) = peers.get_mut(peer) {
                                state.mark_synced(report.remote_tip, report.applied_blocks);
                            }
                        }
                        for peer in &report.failed_peer_addrs {
                            if let Some(state) = peers.get_mut(peer) {
                                state.mark_failed();
                            }
                        }
                    }
                    println!(
                        "parallel sync complete:: |blocks::{}|peers::{}|remote_tip::{}|",
                        report.applied_blocks, report.used_peers, report.remote_tip.0
                    );
                    return;
                }
                Ok(_) => {}
                Err(error) => {
                    eprintln!("parallel sync failed; falling back to peer polling: {error}")
                }
            }
        }

        for peer in due_peers {
            let result = self.poll_persistent_peer(peer);
            match result {
                Ok(PeerPoll::Idle { remote_tip }) => {
                    if let Ok(mut peers) = self.peers.lock()
                        && let Some(state) = peers.get_mut(&peer)
                    {
                        state.mark_ok(Some(remote_tip));
                    }
                    let infos = match self.peer_connections.lock() {
                        Ok(mut connections) => connections
                            .get_mut(&peer)
                            .and_then(|connection| request_peers_connection(connection).ok()),
                        Err(_) => {
                            eprintln!("peer connection lock poisoned");
                            None
                        }
                    };
                    if let Some(infos) = infos
                        && self.add_peer_infos(infos)
                    {
                        let _ = self.save_peers();
                    }
                    self.sync_mempool_from_peer(peer);
                }
                Ok(PeerPoll::Synced {
                    remote_tip,
                    synced_blocks,
                }) => {
                    if let Ok(mut peers) = self.peers.lock()
                        && let Some(state) = peers.get_mut(&peer)
                    {
                        state.mark_synced(remote_tip, synced_blocks);
                    }
                    let infos = match self.peer_connections.lock() {
                        Ok(mut connections) => connections
                            .get_mut(&peer)
                            .and_then(|connection| request_peers_connection(connection).ok()),
                        Err(_) => {
                            eprintln!("peer connection lock poisoned");
                            None
                        }
                    };
                    if let Some(infos) = infos
                        && self.add_peer_infos(infos)
                    {
                        let _ = self.save_peers();
                    }
                    self.sync_mempool_from_peer(peer);
                }
                Err(error) => {
                    let mut dropped = false;
                    if let Ok(mut peers) = self.peers.lock()
                        && let Some(state) = peers.get_mut(&peer)
                    {
                        state.mark_failed();
                        if state.failures >= MAX_PEER_FAILURES {
                            peers.remove(&peer);
                            dropped = true;
                        }
                    }
                    if dropped {
                        if let Ok(mut connections) = self.peer_connections.lock() {
                            connections.remove(&peer);
                        }
                        let _ = self.save_peers();
                        eprintln!(
                            "peer {peer} sync failed {MAX_PEER_FAILURES} times; dropped: {error}"
                        );
                    } else {
                        if let Ok(mut connections) = self.peer_connections.lock() {
                            connections.remove(&peer);
                        }
                        eprintln!("peer {peer} sync failed: {error}");
                    }
                }
            }
        }
    }

    fn poll_persistent_peer(&mut self, peer: SocketAddr) -> Result<PeerPoll, String> {
        let sync_window = peer_state_sync_window(&self.peers, peer);
        let mut connections = self
            .peer_connections
            .lock()
            .map_err(|_| "peer connection lock poisoned".to_string())?;
        if let Entry::Vacant(entry) = connections.entry(peer) {
            let connection = PeerConnection::connect(peer)?;
            println!("p2p outbound:: |peer::{peer}|event::connected|");
            entry.insert(connection);
        }
        let connection = connections
            .get_mut(&peer)
            .ok_or_else(|| format!("missing peer connection for {peer}"))?;
        poll_peer_connection(
            connection,
            &self.node,
            &self.config.public_addrs,
            sync_window,
        )
    }

    fn sync_mempool_from_peer(&mut self, peer: SocketAddr) {
        let result = match self.peer_connections.lock() {
            Ok(mut connections) => connections
                .get_mut(&peer)
                .ok_or_else(|| format!("missing peer connection for {peer}"))
                .and_then(|connection| sync_mempool_connection(connection, &self.node)),
            Err(_) => {
                eprintln!("peer connection lock poisoned");
                return;
            }
        };
        match result {
            Ok(accepted) if accepted > 0 => {
                println!("mempool synced:: |peer::{peer}|txs::{accepted}|");
            }
            Ok(_) => {}
            Err(error) => eprintln!("mempool sync from {peer} failed: {error}"),
        }
    }

    fn add_peer_infos(&mut self, peers: Vec<PeerInfo>) -> bool {
        Self::add_peer_infos_to(
            &self.peers,
            peers,
            &self.config.public_addrs,
            &self.config.listen_addrs,
            self.config.max_peers,
        )
    }

    fn add_peer_infos_to(
        peers_state: &Arc<Mutex<HashMap<SocketAddr, PeerState>>>,
        peers: Vec<PeerInfo>,
        public_addrs: &[SocketAddr],
        listen_addrs: &[SocketAddr],
        max_peers: usize,
    ) -> bool {
        let Ok(mut current) = peers_state.lock() else {
            eprintln!("peer state lock poisoned");
            return false;
        };
        let known = current.keys().copied().collect::<HashSet<_>>();
        let mut changed = false;
        for info in peers {
            if current.len() >= max_peers {
                break;
            }
            let Ok(addr) = info.address.parse::<SocketAddr>() else {
                continue;
            };
            if public_addrs.contains(&addr) || listen_addrs.contains(&addr) {
                continue;
            }
            if known.contains(&addr) {
                continue;
            }
            if let Entry::Vacant(entry) = current.entry(addr) {
                entry.insert(PeerState::new(addr));
                changed = true;
            }
        }
        changed
    }

    fn refresh_gateway_peers(&mut self, register: bool) {
        let Some(gateway_url) = self.config.gateway_url.clone() else {
            return;
        };

        let (best_height, tip_hash) = match self.node.lock() {
            Ok(node) => (
                node.tip_height().map(|height| height.0),
                node.tip_hash().map(|hash| hex::encode(hash.0)),
            ),
            Err(_) => {
                eprintln!("node state lock poisoned");
                return;
            }
        };

        if self.config.public_addrs.is_empty() {
            if register {
                eprintln!("gateway configured without --public-addr; querying peers only");
            }
        } else {
            for public_addr in &self.config.public_addrs {
                let result = if register {
                    register_peer(&gateway_url, *public_addr, best_height, tip_hash.clone())
                } else {
                    heartbeat_peer(&gateway_url, *public_addr, best_height, tip_hash.clone())
                };
                if let Err(error) = result {
                    eprintln!("gateway update failed for {public_addr}: {error}");
                }
            }
        }

        let available = match self.peers.lock() {
            Ok(peers) => self.config.max_peers.saturating_sub(peers.len()),
            Err(_) => {
                eprintln!("peer state lock poisoned");
                return;
            }
        };
        if available > 0 {
            match request_gateway_peers(
                &gateway_url,
                available.min(32),
                self.config
                    .public_addrs
                    .first()
                    .or_else(|| self.config.listen_addrs.first())
                    .copied(),
            ) {
                Ok(peers) => {
                    if !peers.is_empty() {
                        println!("gateway discovered peer::{}|", peers.len());
                        if self.add_peer_infos(peers) {
                            let _ = self.save_peers();
                        }
                    }
                }
                Err(error) => eprintln!("gateway peer query failed: {error}"),
            }
        }

        self.last_gateway_heartbeat = Instant::now();
    }

    fn save_peers(&self) -> Result<(), String> {
        Self::save_peers_from(&self.config.peers_file, &self.peers)
    }

    fn save_peers_from(
        peers_file: &Option<String>,
        peers_state: &Arc<Mutex<HashMap<SocketAddr, PeerState>>>,
    ) -> Result<(), String> {
        let Some(path) = peers_file else {
            return Ok(());
        };
        if let Some(parent) = std::path::Path::new(path).parent() {
            fs::create_dir_all(parent)
                .map_err(|error| format!("failed to create peers file parent: {error}"))?;
        }
        let peers = peers_state
            .lock()
            .map_err(|_| "peer state lock poisoned".to_string())?;
        let mut peers = peers.keys().copied().collect::<Vec<_>>();
        peers.sort();
        save_peers_file(path, peers)
    }

    fn peer_infos_from(
        peers_state: &Arc<Mutex<HashMap<SocketAddr, PeerState>>>,
        public_addrs: &[SocketAddr],
    ) -> Vec<PeerInfo> {
        let Ok(peers) = peers_state.lock() else {
            eprintln!("peer state lock poisoned");
            return Vec::new();
        };
        let mut infos = peers
            .keys()
            .map(|addr| PeerInfo {
                address: addr.to_string(),
            })
            .collect::<Vec<_>>();
        for public_addr in public_addrs {
            infos.push(PeerInfo {
                address: public_addr.to_string(),
            });
        }
        infos.sort_by(|left, right| left.address.cmp(&right.address));
        infos.dedup_by(|left, right| left.address == right.address);
        infos
    }

    fn broadcast(&mut self, message: NetworkMessage) -> BroadcastReport {
        let peers = match self.peers.lock() {
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
        let known_peers = peers.iter().copied().collect::<HashSet<_>>();
        for peer in peers {
            let result = {
                let mut connections = match self.peer_connections.lock() {
                    Ok(connections) => connections,
                    Err(_) => {
                        report.failed += 1;
                        eprintln!("peer connection lock poisoned");
                        continue;
                    }
                };
                if let Entry::Vacant(entry) = connections.entry(peer) {
                    match PeerConnection::connect(peer) {
                        Ok(connection) => {
                            println!("p2p outbound:: |peer::{peer}|event::connected|");
                            entry.insert(connection);
                        }
                        Err(error) => {
                            report.failed += 1;
                            eprintln!("broadcast to {peer} failed: {error}");
                            continue;
                        }
                    }
                }
                connections
                    .get_mut(&peer)
                    .ok_or_else(|| format!("missing peer connection for {peer}"))
                    .and_then(|connection| announce_or_send(connection, message.clone()))
            };
            match result {
                Ok(()) => report.sent += 1,
                Err(error) => {
                    report.failed += 1;
                    if let Ok(mut connections) = self.peer_connections.lock() {
                        connections.remove(&peer);
                    }
                    eprintln!("broadcast to {peer} failed: {error}");
                }
            }
        }
        let inbound_peers = match self.inbound_connections.lock() {
            Ok(connections) => connections.keys().copied().collect::<Vec<_>>(),
            Err(_) => {
                eprintln!("inbound connection lock poisoned");
                Vec::new()
            }
        };
        for peer in inbound_peers {
            if known_peers.contains(&peer) {
                continue;
            }
            report.attempted += 1;
            let result = {
                let mut connections = match self.inbound_connections.lock() {
                    Ok(connections) => connections,
                    Err(_) => {
                        report.failed += 1;
                        eprintln!("inbound connection lock poisoned");
                        continue;
                    }
                };
                connections
                    .get_mut(&peer)
                    .ok_or_else(|| format!("missing inbound connection for {peer}"))
                    .and_then(|connection| connection.send(message.clone()))
            };
            match result {
                Ok(()) => report.sent += 1,
                Err(error) => {
                    report.failed += 1;
                    if let Ok(mut connections) = self.inbound_connections.lock() {
                        connections.remove(&peer);
                    }
                    eprintln!("broadcast to inbound {peer} failed: {error}");
                }
            }
        }
        report
    }
}

