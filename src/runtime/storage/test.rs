use super::{StateSnapshot, Storage, StorageError};
use crate::runtime::params::{DEFAULT_TRANSACTION_FEE, STORAGE_VERSION};
use paqus::block::{Block, Height, Nonce};
use paqus::consensus::supply::Amount;
use paqus::crypto::{Address, BlockHash, Hash, address_from_public_key, generate_keypair, sign};
use paqus::ledger::Ledger;
use paqus::state::Account;
use paqus::transaction::{SignedTransaction, Transaction};

fn address(byte: u8) -> Address {
    Address([byte; 20])
}

fn block(height: u64, previous_hash: Hash) -> Block {
    Block::new(
        Height(height),
        previous_hash,
        address(9),
        1_700_000_000 + height,
        Nonce(0),
        vec![],
    )
}

fn signed_transaction(to: Address, amount: u64, nonce: u64) -> SignedTransaction {
    let keypair = generate_keypair();
    let from = address_from_public_key(&keypair.public_key);
    let payload = Transaction::new(
        from,
        to,
        Amount(amount),
        Amount(DEFAULT_TRANSACTION_FEE),
        Nonce(nonce),
    );
    let signature = sign(&keypair.secret_key, &payload.signing_bytes());
    SignedTransaction::new(payload, keypair.public_key, signature)
}

#[test]
fn stores_and_loads_blocks_by_height_and_hash() {
    let storage = Storage::temporary().unwrap();
    let block = block(0, Hash([0; 64]));
    let hash = block.hash();

    storage.save_block(&block).unwrap();

    assert_eq!(
        storage.load_block_by_height(Height(0)).unwrap(),
        Some(block.clone())
    );
    assert_eq!(storage.load_block_by_hash(&hash).unwrap(), Some(block));
}

#[test]
fn side_blocks_do_not_overwrite_canonical_height_index() {
    let storage = Storage::temporary().unwrap();
    let genesis = block(0, Hash([0; 64]));
    let canonical = block(1, genesis.hash().into());
    let side = Block::with_difficulty(
        Height(1),
        genesis.hash(),
        address(8),
        1,
        1_700_000_101,
        Nonce(7),
        vec![],
    );
    let side_hash = side.hash();

    storage.save_block(&genesis).unwrap();
    storage.save_block(&canonical).unwrap();
    storage.save_side_block(&side).unwrap();

    assert_eq!(
        storage.load_block_by_height(Height(1)).unwrap(),
        Some(canonical)
    );
    assert_eq!(storage.load_block_by_hash(&side_hash).unwrap(), Some(side));
}

#[test]
fn indexes_transactions_by_hash_and_address() {
    let storage = Storage::temporary().unwrap();
    let transaction = signed_transaction(address(2), 10, 0);
    let tx_hash = transaction.hash();
    let sender = transaction.transaction.from;
    let receiver = transaction.transaction.to;
    let block = Block::with_difficulty(
        Height(1),
        Hash([0; 64]),
        address(9),
        1,
        1_700_000_001,
        Nonce(0),
        vec![transaction.clone()],
    );
    let block_hash = block.hash();

    storage.save_block(&block).unwrap();

    let (location, loaded) = storage.load_transaction(&tx_hash).unwrap().unwrap();
    assert_eq!(location.block_height, Height(1));
    assert_eq!(location.block_hash, block_hash);
    assert_eq!(location.tx_index, 0);
    assert_eq!(loaded, transaction);

    let sent = storage.load_address_transaction_locations(&sender).unwrap();
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0].tx_hash, tx_hash);
    assert!(sent[0].sent);

    let received = storage
        .load_address_transaction_locations(&receiver)
        .unwrap();
    assert_eq!(received.len(), 1);
    assert_eq!(received[0].tx_hash, tx_hash);
    assert!(!received[0].sent);
}

#[test]
fn indexes_canonical_blocks_by_miner_address() {
    let storage = Storage::temporary().unwrap();
    let miner = address(7);
    let side_miner = address(8);
    let genesis = block(0, Hash([0; 64]));
    let canonical = Block::with_coinbase(
        Height(1),
        genesis.hash(),
        miner,
        1,
        1_700_000_001,
        Nonce(0),
        Some(paqus::block::CoinbaseTransaction::new(
            miner,
            Amount(0),
            Amount(0),
        )),
        vec![],
    );
    let side = Block::with_coinbase(
        Height(1),
        genesis.hash(),
        side_miner,
        1,
        1_700_000_002,
        Nonce(1),
        Some(paqus::block::CoinbaseTransaction::new(
            side_miner,
            Amount(0),
            Amount(0),
        )),
        vec![],
    );

    storage.save_block(&genesis).unwrap();
    storage.save_block(&canonical).unwrap();
    storage.save_side_block(&side).unwrap();

    let mined = storage.load_miner_block_locations(&miner).unwrap();
    assert_eq!(mined.len(), 1);
    assert_eq!(mined[0].block_height, Height(1));
    assert_eq!(mined[0].block_hash, canonical.hash());
    assert!(
        storage
            .load_miner_block_locations(&side_miner)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn save_ledger_rebuilds_canonical_transaction_indexes() {
    let storage = Storage::temporary().unwrap();
    let genesis = block(0, Hash([0; 64]));
    let old_transaction = signed_transaction(address(2), 10, 0);
    let old_hash = old_transaction.hash();
    let old_block = Block::with_difficulty(
        Height(1),
        genesis.hash(),
        address(9),
        1,
        1_700_000_001,
        Nonce(1),
        vec![old_transaction],
    );
    let new_transaction = signed_transaction(address(3), 11, 0);
    let new_hash = new_transaction.hash();
    let new_block = Block::with_difficulty(
        Height(1),
        genesis.hash(),
        address(8),
        1,
        1_700_000_002,
        Nonce(2),
        vec![new_transaction.clone()],
    );

    let mut old_ledger = Ledger::new();
    old_ledger.chain.insert_block(genesis.clone()).unwrap();
    old_ledger.chain.insert_block(old_block).unwrap();
    storage.save_ledger(&old_ledger).unwrap();
    assert!(storage.load_transaction(&old_hash).unwrap().is_some());

    let mut new_ledger = Ledger::new();
    new_ledger.chain.insert_block(genesis).unwrap();
    new_ledger.chain.insert_block(new_block.clone()).unwrap();
    storage.save_ledger(&new_ledger).unwrap();

    assert!(storage.load_transaction(&old_hash).unwrap().is_none());
    let (location, loaded) = storage.load_transaction(&new_hash).unwrap().unwrap();
    assert_eq!(location.block_height, Height(1));
    assert_eq!(location.block_hash, new_block.hash());
    assert_eq!(loaded, new_transaction);
    assert_eq!(
        storage.load_block_by_height(Height(1)).unwrap(),
        Some(new_block)
    );
}

#[test]
fn save_ledger_rebuilds_miner_block_index() {
    let storage = Storage::temporary().unwrap();
    let genesis = block(0, Hash([0; 64]));
    let old_miner = address(4);
    let new_miner = address(5);
    let old_block = Block::with_coinbase(
        Height(1),
        genesis.hash(),
        old_miner,
        1,
        1_700_000_001,
        Nonce(1),
        Some(paqus::block::CoinbaseTransaction::new(
            old_miner,
            Amount(0),
            Amount(0),
        )),
        vec![],
    );
    let new_block = Block::with_coinbase(
        Height(1),
        genesis.hash(),
        new_miner,
        1,
        1_700_000_002,
        Nonce(2),
        Some(paqus::block::CoinbaseTransaction::new(
            new_miner,
            Amount(0),
            Amount(0),
        )),
        vec![],
    );

    let mut old_ledger = Ledger::new();
    old_ledger.chain.insert_block(genesis.clone()).unwrap();
    old_ledger.chain.insert_block(old_block).unwrap();
    storage.save_ledger(&old_ledger).unwrap();
    assert_eq!(
        storage
            .load_miner_block_locations(&old_miner)
            .unwrap()
            .len(),
        1
    );

    let mut new_ledger = Ledger::new();
    new_ledger.chain.insert_block(genesis).unwrap();
    new_ledger.chain.insert_block(new_block.clone()).unwrap();
    storage.save_ledger(&new_ledger).unwrap();

    assert!(
        storage
            .load_miner_block_locations(&old_miner)
            .unwrap()
            .is_empty()
    );
    let mined = storage.load_miner_block_locations(&new_miner).unwrap();
    assert_eq!(mined.len(), 1);
    assert_eq!(mined[0].block_hash, new_block.hash());
}

#[test]
fn save_ledger_skips_state_snapshot_at_non_protocol_snapshot_height() {
    let storage = Storage::temporary().unwrap();
    let genesis = block(0, Hash([0; 64]));
    let next = block(1, genesis.hash().into());
    let mut ledger = Ledger::new();
    ledger.chain.insert_block(genesis).unwrap();
    ledger.chain.insert_block(next.clone()).unwrap();

    storage.save_ledger(&ledger).unwrap();

    assert_eq!(storage.load_tip().unwrap(), Some((Height(1), next.hash())));
    assert!(storage.load_state_snapshot(Height(1)).unwrap().is_none());
}

#[test]
fn initializes_storage_version_for_empty_database() {
    let storage = Storage::temporary().unwrap();

    assert_eq!(
        storage.load_storage_version().unwrap(),
        Some(STORAGE_VERSION)
    );
}

#[test]
fn rejects_unsupported_storage_version() {
    let storage = Storage::temporary().unwrap();
    storage
        .test_put_meta(b"storage_version", &STORAGE_VERSION.saturating_add(1))
        .unwrap();

    assert!(matches!(
        storage.load_ledger(),
        Err(StorageError::UnsupportedStorageVersion {
            expected: STORAGE_VERSION,
            found
        }) if found == STORAGE_VERSION.saturating_add(1)
    ));
}

#[test]
fn rejects_existing_database_without_storage_version() {
    let storage = Storage::temporary().unwrap();
    storage.test_remove_meta(b"storage_version").unwrap();
    storage.save_block(&block(0, Hash([0; 64]))).unwrap();

    assert!(matches!(
        storage.load_ledger(),
        Err(StorageError::MissingStorageVersion)
    ));
}

#[test]
fn rejects_block_loaded_from_wrong_height_key() {
    let storage = Storage::temporary().unwrap();
    let block = block(1, Hash([0; 64]));

    storage
        .test_put_blocks_by_height(&Height(0).0.to_be_bytes(), &block)
        .unwrap();

    assert!(matches!(
        storage.load_block_by_height(Height(0)),
        Err(StorageError::Integrity(
            "stored block height does not match height key"
        ))
    ));
}

#[test]
fn rejects_block_loaded_from_wrong_hash_key() {
    let storage = Storage::temporary().unwrap();
    let block = block(0, Hash([0; 64]));
    let wrong_hash = BlockHash([7; 64]);

    storage
        .test_put_blocks_by_hash(wrong_hash.0.as_slice(), &block)
        .unwrap();

    assert!(matches!(
        storage.load_block_by_hash(&wrong_hash),
        Err(StorageError::Integrity(
            "stored block hash does not match hash key"
        ))
    ));
}

#[test]
fn stores_and_loads_accounts() {
    let storage = Storage::temporary().unwrap();
    let account = Account::trusted_with_nonce(address(1), Amount(25), Nonce(7));

    storage.save_account(&account).unwrap();

    assert_eq!(storage.load_account(&address(1)).unwrap(), Some(account));
    assert_eq!(storage.load_account(&address(2)).unwrap(), None);
}

#[test]
fn stores_and_loads_chain_tip() {
    let storage = Storage::temporary().unwrap();
    let hash = BlockHash([7; 64]);

    assert_eq!(storage.load_tip().unwrap(), None);

    storage.save_tip(Height(3), &hash).unwrap();

    assert_eq!(storage.load_tip().unwrap(), Some((Height(3), hash)));
}

#[test]
fn validates_stored_chain_integrity() {
    let storage = Storage::temporary().unwrap();
    let genesis = block(0, Hash([0; 64]));
    let next = block(1, genesis.hash().into());

    storage.save_block(&genesis).unwrap();
    storage.save_block(&next).unwrap();
    storage.save_tip(next.height(), &next.hash()).unwrap();

    assert!(storage.validate_chain_integrity().is_ok());
}

#[test]
fn rejects_chain_integrity_when_tip_block_is_missing() {
    let storage = Storage::temporary().unwrap();

    storage.save_tip(Height(3), &BlockHash([7; 64])).unwrap();

    assert!(matches!(
        storage.validate_chain_integrity(),
        Err(StorageError::Integrity(
            "stored tip height block is missing"
        ))
    ));
}

#[test]
fn rejects_chain_integrity_when_previous_link_is_broken() {
    let storage = Storage::temporary().unwrap();
    let genesis = block(0, Hash([0; 64]));
    let next = block(1, Hash([9; 64]));

    storage.save_block(&genesis).unwrap();
    storage.save_block(&next).unwrap();
    storage.save_tip(next.height(), &next.hash()).unwrap();

    assert!(matches!(
        storage.validate_chain_integrity(),
        Err(StorageError::Integrity(
            "stored chain block previous hash is broken"
        ))
    ));
}

#[test]
fn stores_ledger_snapshot() {
    let storage = Storage::temporary().unwrap();
    let mut ledger = Ledger::new();
    let mut genesis = block(0, Hash([0; 64]));

    ledger.create_account(address(1), Amount(100)).unwrap();
    genesis.set_state_root(ledger.state_root());
    let hash = genesis.hash();
    ledger.chain.insert_block(genesis.clone()).unwrap();

    storage.save_ledger(&ledger).unwrap();

    assert_eq!(
        storage.load_account(&address(1)).unwrap().unwrap().balance,
        Amount(100)
    );
    assert_eq!(
        storage.load_block_by_height(Height(0)).unwrap(),
        Some(genesis)
    );
    assert_eq!(storage.load_tip().unwrap(), Some((Height(0), hash)));
}

#[test]
fn loads_ledger_snapshot() {
    let storage = Storage::temporary().unwrap();
    let mut ledger = Ledger::new();
    let genesis = block(0, Hash([0; 64]));
    let hash = genesis.hash();

    ledger.create_account(address(1), Amount(100)).unwrap();
    ledger.chain.insert_block(genesis).unwrap();
    storage.save_ledger(&ledger).unwrap();

    let restored = storage.load_ledger().unwrap();

    assert_eq!(restored.balance(&address(1)), Some(Amount(100)));
    assert_eq!(restored.tip_height(), Some(Height(0)));
    assert_eq!(restored.tip_hash(), Some(hash));
}

#[test]
fn stores_and_loads_genesis_accounts() {
    let storage = Storage::temporary().unwrap();
    let mut accounts = std::collections::BTreeMap::new();
    accounts.insert(address(1), Account::new(address(1), Amount(100)));

    storage.save_genesis_accounts(&accounts).unwrap();

    assert_eq!(storage.load_genesis_accounts().unwrap(), Some(accounts));
}

#[test]
fn stores_and_loads_state_snapshot() {
    let storage = Storage::temporary().unwrap();
    let mut ledger = Ledger::new();
    let mut genesis = block(0, Hash([0; 64]));

    ledger.create_account(address(1), Amount(100)).unwrap();
    genesis.set_state_root(ledger.state_root());
    let hash = genesis.hash();
    ledger.chain.insert_block(genesis.clone()).unwrap();
    storage.save_block(&genesis).unwrap();
    storage.save_state_snapshot(&ledger).unwrap();

    let snapshot = storage.load_state_snapshot(Height(0)).unwrap().unwrap();

    assert_eq!(snapshot.height, Height(0));
    assert_eq!(snapshot.block_hash, hash);
    assert_eq!(snapshot.state_root, ledger.state_root());
    assert_eq!(
        snapshot
            .accounts
            .get(&address(1))
            .map(|account| account.balance),
        Some(Amount(100))
    );
    assert!(snapshot.verify_state_root());
    assert!(
        snapshot.verify_against_block(&storage.load_block_by_height(Height(0)).unwrap().unwrap())
    );
}

#[test]
fn rejects_state_snapshot_at_non_protocol_snapshot_height() {
    let storage = Storage::temporary().unwrap();
    let mut ledger = Ledger::new();
    let genesis = block(0, Hash([0; 64]));
    let next = block(1, genesis.hash().into());

    ledger.chain.insert_block(genesis).unwrap();
    ledger.chain.insert_block(next).unwrap();

    assert!(matches!(
        storage.save_state_snapshot(&ledger),
        Err(StorageError::Integrity(
            "state snapshot height is not a protocol snapshot height"
        ))
    ));
}

#[test]
fn difficulty_window_uses_previous_block_for_single_block_interval() {
    let storage = Storage::temporary().unwrap();
    let genesis = block(0, Hash([0; 64]));
    let next = block(1, genesis.hash().into());

    storage.save_block(&genesis).unwrap();
    storage.save_block(&next).unwrap();

    assert_eq!(storage.difficulty_window(Height(0), 1).unwrap(), None);
    assert_eq!(
        storage.difficulty_window(Height(1), 1).unwrap(),
        Some((genesis.timestamp(), next.timestamp(), 1, next.difficulty()))
    );
}

#[test]
fn difficulty_window_uses_configured_block_interval() {
    let storage = Storage::temporary().unwrap();
    let mut previous_hash = Hash([0; 64]);

    for height in 0..=10 {
        let block = block(height, previous_hash);
        previous_hash = block.hash().into();
        storage.save_block(&block).unwrap();
    }

    assert_eq!(storage.difficulty_window(Height(9), 10).unwrap(), None);
    assert_eq!(
        storage.difficulty_window(Height(10), 10).unwrap(),
        Some((
            block(0, Hash([0; 64])).timestamp(),
            block(10, Hash([0; 64])).timestamp(),
            10,
            block(10, Hash([0; 64])).difficulty()
        ))
    );
}

#[test]
fn rejects_tampered_state_snapshot_root() {
    let storage = Storage::temporary().unwrap();
    let mut accounts = std::collections::BTreeMap::new();
    accounts.insert(address(1), Account::new(address(1), Amount(100)));
    let snapshot = StateSnapshot {
        height: Height(0),
        block_hash: BlockHash([1; 64]),
        state_root: Hash([9; 64]),
        accounts,
    };

    storage
        .test_put_state_snapshot(&Height(0).0.to_be_bytes(), &snapshot)
        .unwrap();

    assert!(matches!(
        storage.load_state_snapshot(Height(0)),
        Err(StorageError::Integrity(
            "stored state snapshot root does not match accounts"
        ))
    ));
}
