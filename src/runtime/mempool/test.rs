use super::{Mempool, MempoolConfig, MempoolError};
use crate::runtime::params::{BASE_FEE, MEMPOOL_EXPIRY_SECS};
use paqus::block::Block;
use paqus::crypto::{address_from_public_key, generate_keypair, sign};
use paqus::ledger::{Ledger, LedgerError};
use paqus::state::StateError;
use paqus::transaction::{SignedTransaction, Transaction, TransactionError};
use paqus::types::{Address, Amount, Hash, Height, Nonce};

fn address(byte: u8) -> Address {
    Address([byte; 20])
}

fn signed_transaction(nonce: u64) -> SignedTransaction {
    signed_transaction_at(nonce, current_unix_timestamp())
}

fn signed_transaction_at(nonce: u64, timestamp: u64) -> SignedTransaction {
    let keypair = generate_keypair();
    let from = address_from_public_key(&keypair.public_key);
    let payload = Transaction::new_at(
        from,
        address(2),
        Amount(10),
        Amount(BASE_FEE),
        Nonce(nonce),
        timestamp,
    );
    let signature = sign(&keypair.secret_key, &payload.signing_bytes());

    SignedTransaction::new(payload, keypair.public_key, signature)
}

fn signed_transaction_from(
    secret_key: &paqus::types::SecretKey,
    public_key: paqus::types::PublicKey,
    to: Address,
    amount: u32,
    nonce: u64,
) -> SignedTransaction {
    signed_transaction_from_with_fee(
        secret_key,
        public_key,
        to,
        amount,
        crate::runtime::params::BASE_FEE,
        nonce,
    )
}

fn signed_transaction_from_with_fee(
    secret_key: &paqus::types::SecretKey,
    public_key: paqus::types::PublicKey,
    to: Address,
    amount: u32,
    fee: u32,
    nonce: u64,
) -> SignedTransaction {
    let from = address_from_public_key(&public_key);
    let payload = Transaction::new_at(
        from,
        to,
        Amount(amount),
        Amount(fee),
        Nonce(nonce),
        current_unix_timestamp(),
    );
    let signature = sign(secret_key, &payload.signing_bytes());

    SignedTransaction::new(payload, public_key, signature)
}

fn ledger_with_accounts(from: Address, to: Address, balance: u32) -> Ledger {
    let mut ledger = Ledger::new();
    ledger.create_account(from, Amount(balance)).unwrap();
    ledger.create_account(to, Amount(0)).unwrap();
    ledger
}

#[test]
fn inserts_valid_signed_transaction() {
    let mut mempool = Mempool::new();
    let transaction = signed_transaction(0);
    let hash = transaction.hash();

    assert_eq!(mempool.insert(transaction), Ok(hash));
    assert!(mempool.contains(&hash));
    assert_eq!(mempool.len(), 1);
}

#[test]
fn uses_default_transaction_limit() {
    let mempool = Mempool::new();

    assert_eq!(
        mempool.config(),
        MempoolConfig {
            max_transactions: crate::runtime::params::MAX_MEMPOOL_TXS,
            max_bytes: crate::runtime::params::MAX_MEMPOOL_BYTES,
            transaction_ttl_secs: MEMPOOL_EXPIRY_SECS,
        }
    );
}

#[test]
fn rejects_transaction_when_mempool_is_full() {
    let mut mempool = Mempool::with_config(MempoolConfig {
        max_transactions: 1,
        max_bytes: crate::runtime::params::MAX_MEMPOOL_BYTES,
        transaction_ttl_secs: MEMPOOL_EXPIRY_SECS,
    });
    let first = signed_transaction(0);
    let second = signed_transaction(1);

    assert_eq!(mempool.insert(first.clone()), Ok(first.hash()));
    assert_eq!(
        mempool.insert(first),
        Err(MempoolError::DuplicateTransaction)
    );
    assert_eq!(mempool.insert(second), Err(MempoolError::MempoolFull));
}

#[test]
fn rejects_transaction_when_mempool_byte_limit_is_full() {
    let transaction = signed_transaction(0);
    let mut mempool = Mempool::with_config(MempoolConfig {
        max_transactions: 10,
        max_bytes: transaction.serialized_size().saturating_sub(1),
        transaction_ttl_secs: MEMPOOL_EXPIRY_SECS,
    });

    assert_eq!(mempool.insert(transaction), Err(MempoolError::MempoolFull));
}

#[test]
fn replaces_same_sender_nonce_when_fee_is_higher() {
    let keypair = generate_keypair();
    let from = address_from_public_key(&keypair.public_key);
    let to = address(2);
    let ledger = ledger_with_accounts(from, to, 100);
    let original =
        signed_transaction_from_with_fee(&keypair.secret_key, keypair.public_key, to, 10, 2, 0);
    let replacement =
        signed_transaction_from_with_fee(&keypair.secret_key, keypair.public_key, to, 10, 3, 0);
    let original_hash = original.hash();
    let replacement_hash = replacement.hash();
    let mut mempool = Mempool::new();

    assert_eq!(
        mempool.insert_validated(&ledger, original),
        Ok(original_hash)
    );
    assert_eq!(
        mempool.insert_validated(&ledger, replacement),
        Ok(replacement_hash)
    );
    assert!(!mempool.contains(&original_hash));
    assert!(mempool.contains(&replacement_hash));
    assert_eq!(mempool.len(), 1);
}

#[test]
fn rejects_same_sender_nonce_replacement_when_fee_is_not_higher() {
    let keypair = generate_keypair();
    let from = address_from_public_key(&keypair.public_key);
    let to = address(2);
    let ledger = ledger_with_accounts(from, to, 100);
    let original =
        signed_transaction_from_with_fee(&keypair.secret_key, keypair.public_key, to, 10, 3, 0);
    let replacement =
        signed_transaction_from_with_fee(&keypair.secret_key, keypair.public_key, to, 10, 2, 0);
    let mut mempool = Mempool::new();

    mempool.insert_validated(&ledger, original).unwrap();
    assert_eq!(
        mempool.insert_validated(&ledger, replacement),
        Err(MempoolError::ReplacementFeeTooLow)
    );
}

#[test]
fn rejects_duplicate_transaction() {
    let mut mempool = Mempool::new();
    let transaction = signed_transaction(0);

    assert_eq!(mempool.insert(transaction.clone()), Ok(transaction.hash()));
    assert_eq!(
        mempool.insert(transaction),
        Err(MempoolError::DuplicateTransaction)
    );
}

#[test]
fn prunes_expired_transactions() {
    let mut mempool = Mempool::new();
    let expired = signed_transaction_at(0, 1_000);
    let fresh = signed_transaction_at(1, 1_000 + MEMPOOL_EXPIRY_SECS);
    let expired_hash = expired.hash();
    let fresh_hash = fresh.hash();

    mempool.insert_at(expired, 1_000).unwrap();
    mempool
        .insert_at(fresh, 1_000 + MEMPOOL_EXPIRY_SECS)
        .unwrap();

    assert_eq!(mempool.prune_expired(1_000 + MEMPOOL_EXPIRY_SECS + 1), 1);
    assert!(!mempool.contains(&expired_hash));
    assert!(mempool.contains(&fresh_hash));
}

#[test]
fn insert_prunes_expired_transactions_before_capacity_check() {
    let mut mempool = Mempool::with_config(MempoolConfig {
        max_transactions: 1,
        max_bytes: crate::runtime::params::MAX_MEMPOOL_BYTES,
        transaction_ttl_secs: MEMPOOL_EXPIRY_SECS,
    });
    let expired = signed_transaction_at(0, 1_000);
    let replacement = signed_transaction_at(1, 1_000 + MEMPOOL_EXPIRY_SECS + 1);
    let replacement_hash = replacement.hash();

    mempool.insert_at(expired, 1_000).unwrap();

    assert_eq!(
        mempool.insert_at(replacement, 1_000 + MEMPOOL_EXPIRY_SECS + 1),
        Ok(replacement_hash)
    );
    assert_eq!(mempool.len(), 1);
    assert!(mempool.contains(&replacement_hash));
}

#[test]
fn rejects_transaction_with_expired_timestamp() {
    let mut mempool = Mempool::new();
    let transaction = signed_transaction_at(0, 1_000);

    assert_eq!(
        mempool.insert_at(
            transaction,
            1_000 + crate::runtime::params::MAX_TRANSACTION_AGE as u64 + 1
        ),
        Err(MempoolError::InvalidTransaction(
            TransactionError::Expired
        ))
    );
}

#[test]
fn rejects_transaction_from_too_far_in_future() {
    let mut mempool = Mempool::new();
    let transaction = signed_transaction_at(
        0,
        1_000 + crate::runtime::params::MAX_TRANSACTION_FUTURE_TIME as u64 + 1,
    );

    assert_eq!(
        mempool.insert_at(transaction, 1_000),
        Err(MempoolError::InvalidTransaction(
            TransactionError::FromFuture
        ))
    );
}

#[test]
fn rejects_invalid_signed_transaction() {
    let mut mempool = Mempool::new();
    let mut transaction = signed_transaction(0);
    transaction.transaction.from = address(9);

    assert_eq!(
        mempool.insert(transaction),
        Err(MempoolError::InvalidTransaction(
            TransactionError::SenderAddressMismatch
        ))
    );
}

#[test]
fn inserts_transaction_validated_against_ledger_state() {
    let keypair = generate_keypair();
    let from = address_from_public_key(&keypair.public_key);
    let to = address(2);
    let ledger = ledger_with_accounts(from, to, 25);
    let transaction = signed_transaction_from(&keypair.secret_key, keypair.public_key, to, 10, 0);
    let hash = transaction.hash();
    let mut mempool = Mempool::new();

    assert_eq!(mempool.insert_validated(&ledger, transaction), Ok(hash));
    assert!(mempool.contains(&hash));
}

#[test]
fn rejects_transaction_when_ledger_account_is_missing() {
    let keypair = generate_keypair();
    let transaction =
        signed_transaction_from(&keypair.secret_key, keypair.public_key, address(2), 10, 0);
    let ledger = Ledger::new();
    let mut mempool = Mempool::new();

    assert_eq!(
        mempool.insert_validated(&ledger, transaction),
        Err(MempoolError::InvalidLedgerState(
            LedgerError::AccountNotFound
        ))
    );
}

#[test]
fn rejects_transaction_with_invalid_ledger_nonce() {
    let keypair = generate_keypair();
    let from = address_from_public_key(&keypair.public_key);
    let to = address(2);
    let ledger = ledger_with_accounts(from, to, 25);
    let transaction = signed_transaction_from(&keypair.secret_key, keypair.public_key, to, 10, 1);
    let mut mempool = Mempool::new();

    assert_eq!(
        mempool.insert_validated(&ledger, transaction),
        Err(MempoolError::InvalidLedgerState(LedgerError::InvalidState(
            StateError::InvalidNonce
        )))
    );
}

#[test]
fn accounts_for_pending_transactions_when_validating_ledger_state() {
    let keypair = generate_keypair();
    let from = address_from_public_key(&keypair.public_key);
    let to = address(2);
    let ledger = ledger_with_accounts(from, to, 25);
    let first = signed_transaction_from(&keypair.secret_key, keypair.public_key, to, 10, 0);
    let second = signed_transaction_from(&keypair.secret_key, keypair.public_key, to, 10, 1);
    let too_expensive = signed_transaction_from(&keypair.secret_key, keypair.public_key, to, 10, 2);
    let mut mempool = Mempool::new();

    assert_eq!(
        mempool.insert_validated(&ledger, first.clone()),
        Ok(first.hash())
    );
    assert_eq!(
        mempool.insert_validated(&ledger, second.clone()),
        Ok(second.hash())
    );
    assert_eq!(
        mempool.insert_validated(&ledger, too_expensive),
        Err(MempoolError::InvalidLedgerState(LedgerError::InvalidState(
            StateError::InsufficientBalance
        )))
    );
}

#[test]
fn selects_transactions_for_block() {
    let mut mempool = Mempool::new();
    let first = signed_transaction(0);
    let second = signed_transaction(1);
    let third = signed_transaction(2);

    mempool.insert(first).unwrap();
    mempool.insert(second).unwrap();
    mempool.insert(third).unwrap();

    assert_eq!(mempool.select_for_block(2).len(), 2);
    assert_eq!(mempool.select_for_block(10).len(), 3);
}

#[test]
fn selects_transactions_by_fee_without_breaking_sender_nonce_order() {
    let first_keypair = generate_keypair();
    let second_keypair = generate_keypair();
    let first_sender = address_from_public_key(&first_keypair.public_key);
    let second_sender = address_from_public_key(&second_keypair.public_key);
    let receiver = address(2);
    let mut mempool = Mempool::new();
    let first_slow = signed_transaction_from_with_fee(
        &first_keypair.secret_key,
        first_keypair.public_key,
        receiver,
        10,
        crate::runtime::params::SLOW_FEE,
        0,
    );
    let first_aggressive = signed_transaction_from_with_fee(
        &first_keypair.secret_key,
        first_keypair.public_key,
        receiver,
        10,
        crate::runtime::params::AGGRESSIVE_FEE,
        1,
    );
    let second_fast = signed_transaction_from_with_fee(
        &second_keypair.secret_key,
        second_keypair.public_key,
        receiver,
        10,
        crate::runtime::params::FAST_FEE,
        0,
    );

    mempool.insert(first_aggressive).unwrap();
    mempool.insert(first_slow).unwrap();
    mempool.insert(second_fast).unwrap();

    let selected = mempool.select_for_block(3);

    assert_eq!(selected[0].transaction.from, second_sender);
    assert_eq!(
        selected[0].transaction.fee,
        Amount(crate::runtime::params::FAST_FEE)
    );
    assert_eq!(selected[1].transaction.from, first_sender);
    assert_eq!(selected[1].transaction.nonce, Nonce(0));
    assert_eq!(selected[2].transaction.from, first_sender);
    assert_eq!(selected[2].transaction.nonce, Nonce(1));
    assert_eq!(
        selected[2].transaction.fee,
        Amount(crate::runtime::params::AGGRESSIVE_FEE)
    );
}

#[test]
fn removes_confirmed_transactions() {
    let mut mempool = Mempool::new();
    let confirmed = signed_transaction(0);
    let pending = signed_transaction(1);
    let confirmed_hash = confirmed.hash();
    let pending_hash = pending.hash();

    mempool.insert(confirmed.clone()).unwrap();
    mempool.insert(pending).unwrap();

    let block = Block::new(
        Height(1),
        Hash([0; 64]),
        address(9),
        1_700_000_000,
        Nonce(0),
        vec![confirmed],
    );

    mempool.remove_confirmed(&block);

    assert!(!mempool.contains(&confirmed_hash));
    assert!(mempool.contains(&pending_hash));
    assert_eq!(mempool.len(), 1);
}

#[test]
fn creates_candidate_block_from_mempool_transactions() {
    let keypair = generate_keypair();
    let from = address_from_public_key(&keypair.public_key);
    let to = address(2);
    let miner = address(9);
    let mut ledger = ledger_with_accounts(from, to, 25);
    ledger.create_account(miner, Amount(0)).unwrap();
    ledger
        .apply_block(Block::new(
            Height(0),
            Hash([0; 64]),
            miner,
            1_700_000_000,
            Nonce(0),
            vec![],
        ))
        .unwrap();
    let mut mempool = Mempool::new();
    let transaction = signed_transaction_from(&keypair.secret_key, keypair.public_key, to, 10, 0);

    mempool.insert_validated(&ledger, transaction).unwrap();

    let block = mempool
        .create_candidate_block(&ledger, miner, 1_700_000_000, Nonce(0), 10)
        .unwrap();

    assert_eq!(block.height(), Height(1));
    assert_eq!(block.previous_hash(), ledger.tip_hash().unwrap());
    assert_eq!(block.transaction_count(), 1);
    assert_eq!(
        block.state_root(),
        ledger.state_root_after_block(&block).unwrap()
    );
    assert_eq!(ledger.apply_block(block), Ok(()));
}

#[test]
fn rejects_transaction_spending_immature_mining_reward() {
    let keypair = generate_keypair();
    let miner = address_from_public_key(&keypair.public_key);
    let to = address(2);
    let mut ledger = Ledger::new();
    ledger.create_account(to, Amount(0)).unwrap();
    ledger.create_account(miner, Amount(0)).unwrap();
    let genesis = Block::new(
        Height(0),
        Hash([0; 64]),
        miner,
        1_700_000_000,
        Nonce(0),
        vec![],
    );
    ledger.chain.insert_block(genesis).unwrap();
    let funding_keypair = generate_keypair();
    let funding_sender = address_from_public_key(&funding_keypair.public_key);
    ledger.create_account(funding_sender, Amount(100)).unwrap();
    let funding = signed_transaction_from(
        &funding_keypair.secret_key,
        funding_keypair.public_key,
        to,
        1,
        0,
    );

    let mut reward_block = Block::with_difficulty(
        Height(1),
        ledger.tip_hash().unwrap(),
        miner,
        1,
        1_700_000_001,
        Nonce(0),
        vec![funding],
    );
    reward_block.set_state_root(ledger.state_root_after_block(&reward_block).unwrap());
    ledger.apply_block(reward_block).unwrap();

    let transaction = signed_transaction_from(&keypair.secret_key, keypair.public_key, to, 10, 0);
    let mut mempool = Mempool::new();

    assert_eq!(
        mempool.insert_validated(&ledger, transaction),
        Err(MempoolError::InvalidLedgerState(LedgerError::InvalidState(
            StateError::InsufficientBalance
        )))
    );
}
