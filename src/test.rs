use super::*;
use crate::rpc::api::{
    EventQuery, finalized_event_height, protocol_event_involves_address, protocol_event_list,
    protocol_tx_response, qcash_file_lookup_prefix,
};

fn args(values: &[&str]) -> Vec<String> {
    values.iter().map(|value| value.to_string()).collect()
}

#[test]
fn parse_address_accepts_wallet_address_string() {
    let address = Address([0xab; 20]);
    let encoded = address_to_string(&address);

    assert_eq!(parse_address_string(&encoded), Ok(address));
}

#[test]
fn parse_address_accepts_legacy_hex() {
    let address = Address([0xab; 20]);
    let encoded = hex::encode(address.0);

    assert_eq!(parse_address_string(&encoded), Ok(address));
}

#[test]
fn mining_reads_address_from_plaintext_wallet() {
    let keypair = paqus::crypto::generate_keypair();
    let address = address_from_public_key(&keypair.public_key);
    let path = std::env::temp_dir().join(format!(
        "paqus-mining-wallet-plaintext-{}.json",
        std::process::id()
    ));
    let contents = serde_json::json!({
        "version": 1,
        "address": address_to_string(&address),
        "public_key": hex::encode(keypair.public_key.0),
        "secret_key": hex::encode(keypair.secret_key.0)
    });
    fs::write(&path, serde_json::to_vec(&contents).unwrap()).unwrap();

    assert_eq!(load_wallet_address(path.to_str().unwrap()), Ok(address));
    fs::remove_file(path).unwrap();
}

#[test]
fn parse_run_config_accepts_pasted_flags_with_surrounding_spaces() {
    let config = parse_run_config(&args(&[
        "--config",
        "/tmp/full-node-missing-test-config.json",
        "./data/paqus",
        " --listen",
        "0.0.0.0:5555",
        " --listen",
        "[::]:5555",
        " --rpc-listen",
        "127.0.0.1:6666",
        " --public-addr",
        "[2404:8000:1044:4d8:822b:f9ff:fee2:365]:5555",
        " --peer",
        "[2404:8000:1044:4d8:1202:b5ff:feb0:7020]:5555",
        " --peer",
        "182.253.148.123:5555",
        " --mine",
        " --mine-attempts",
        "100000",
    ]))
    .expect("pasted flags should parse");

    assert_eq!(config.db_path, "./data/paqus");
    assert_eq!(config.listen_addrs.len(), 2);
    assert_eq!(config.peers.len(), 2);
    assert_eq!(config.public_addrs.len(), 1);
    assert_eq!(config.rpc_addr, "127.0.0.1:6666".parse().unwrap());
    assert!(config.mine);
    assert_eq!(config.mine_attempts, 100000);
}

#[test]
fn run_config_defaults_to_local_rpc_without_bootstrap_peer() {
    let config = RunConfig::default();

    assert_eq!(config.rpc_addr, "127.0.0.1:6666".parse().unwrap());
    assert!(config.peers.is_empty());
}

#[test]
fn qcash_file_lookup_accepts_file_names_and_prefixes() {
    assert_eq!(
        qcash_file_lookup_prefix("100_E5D6217A74B06B8E.XPQ").unwrap(),
        "e5d6217a74b06b8e"
    );
    assert_eq!(
        qcash_file_lookup_prefix("E5D6217A74B06B8E").unwrap(),
        "e5d6217a74b06b8e"
    );
    assert!(qcash_file_lookup_prefix("100_not-hex.XPQ").is_err());
}

#[test]
fn database_backup_restore_roundtrip_and_refuse_overwrite() {
    let root = std::env::temp_dir().join(format!(
        "paqus-db-ops-{}-{}",
        std::process::id(),
        unix_timestamp().unwrap()
    ));
    let source = root.join("source");
    let backup = root.join("backup");
    let restored = root.join("restored");
    let _ = fs::remove_dir_all(&root);

    let source_node = open_node(source.to_string_lossy().as_ref(), Address([9; 20])).unwrap();
    let expected_tip = source_node.tip_hash();
    drop(source_node);
    backup_node_database(
        source.to_string_lossy().as_ref(),
        backup.to_string_lossy().as_ref(),
    )
    .unwrap();
    assert!(
        backup_node_database(
            source.to_string_lossy().as_ref(),
            backup.to_string_lossy().as_ref(),
        )
        .is_err()
    );
    restore_node_database(
        backup.to_string_lossy().as_ref(),
        restored.to_string_lossy().as_ref(),
    )
    .unwrap();
    let restored_node = open_node(restored.to_string_lossy().as_ref(), Address([9; 20])).unwrap();
    assert_eq!(restored_node.tip_hash(), expected_tip);
    drop(restored_node);

    fs::remove_dir_all(root).unwrap();
}

#[test]
fn protocol_event_query_filters_height_kind_and_paginates() {
    let owner = Address([1; 20]);
    let events = vec![
        ProtocolEvent::new(
            Height(3),
            BlockHash([3; 32]),
            None,
            0,
            ProtocolEventKind::CoinbasePaid {
                miner: owner,
                subsidy: Amount(1),
            },
        ),
        ProtocolEvent::new(
            Height(4),
            BlockHash([4; 32]),
            None,
            0,
            ProtocolEventKind::QCashWithdrawn {
                signer: owner,
                amount: Amount(1),
            },
        ),
        ProtocolEvent::new(
            Height(5),
            BlockHash([5; 32]),
            None,
            0,
            ProtocolEventKind::CoinbasePaid {
                miner: owner,
                subsidy: Amount(1),
            },
        ),
    ];

    let response = protocol_event_list(
        events,
        EventQuery {
            offset: Some(1),
            limit: Some(1),
            kind: Some("coinbase_paid".to_string()),
            from_height: Some(3),
            to_height: Some(5),
        },
    )
    .unwrap();

    assert_eq!(response.total, 2);
    assert_eq!(response.events.len(), 1);
    assert_eq!(response.events[0].event.block_height, Height(5));
}

#[test]
fn protocol_event_query_rejects_invalid_limits_and_kinds() {
    assert!(matches!(
        protocol_event_list(
            vec![],
            EventQuery {
                limit: Some(0),
                ..EventQuery::default()
            }
        ),
        Err("event_limit_must_be_between_1_and_500")
    ));
    assert!(matches!(
        protocol_event_list(
            vec![],
            EventQuery {
                kind: Some("unknown".to_string()),
                ..EventQuery::default()
            }
        ),
        Err("unknown_event_kind")
    ));
}

#[test]
fn event_stream_waits_for_finality_depth() {
    assert_eq!(finalized_event_height(Some(Height(1))), None);
    assert_eq!(
        finalized_event_height(Some(Height(u64::from(FINALITY_DEPTH)))),
        Some(0)
    );
    assert_eq!(
        finalized_event_height(Some(Height(u64::from(FINALITY_DEPTH) + 7))),
        Some(7)
    );
}

#[test]
fn event_stream_address_filter_covers_transfer_participants() {
    let sender = Address([7; 20]);
    let recipient = Address([8; 20]);
    let unrelated = Address([9; 20]);
    let event = ProtocolEventKind::Transfer {
        from: sender,
        to: recipient,
        amount: Amount(10),
        fee: Amount(1),
    };

    assert!(protocol_event_involves_address(&event, &sender));
    assert!(protocol_event_involves_address(&event, &recipient));
    assert!(!protocol_event_involves_address(&event, &unrelated));
}

#[test]
fn generic_explorer_response_exposes_txid_wtxid_and_witness_identity() {
    let keypair = paqus::crypto::generate_keypair();
    let signer = address_from_public_key(&keypair.public_key);
    let payload = Transaction::new(signer, Address([2; 20]), Amount(10), Amount(2), Nonce(3));
    let signature = paqus::crypto::sign(&keypair.secret_key, &payload.signing_bytes());
    let transaction = SignedProtocolTransaction::Transfer(SignedTransaction::new(
        payload,
        keypair.public_key,
        signature,
    ));

    let response = protocol_tx_response(&transaction, Some(Height(7)), None, "confirmed");
    let json = serde_json::to_value(response).unwrap();

    assert_eq!(json["family"], "transfer");
    assert_eq!(json["operation"], "transfer");
    assert_eq!(json["txid"], hex::encode(transaction.hash().0));
    assert_eq!(json["wtxid"], hex::encode(transaction.wtxid().0));
    assert_eq!(json["signer"], address_to_string(&signer));
    assert_eq!(json["witness_addresses"][0], address_to_string(&signer));
    assert_eq!(json["block_height"], 7);
    assert!(
        json["virtual_size"].as_u64().unwrap()
            < json["stripped_size"].as_u64().unwrap() + json["witness_size"].as_u64().unwrap()
    );
}
