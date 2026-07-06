use super::{MiningConfig, mine_candidate_block};
use crate::runtime::mempool::Mempool;
use crate::runtime::params::BASE_FEE;
use paqus::block::{Block, Height, Nonce};
use paqus::consensus::{Consensus, ConsensusConfig};
use paqus::consensus::supply::Amount;
use paqus::crypto::{Address, Hash, address_from_public_key, generate_keypair, sign};
use paqus::ledger::Ledger;
use paqus::transaction::{SignedTransaction, Transaction};
use std::time::{SystemTime, UNIX_EPOCH};

fn address(byte: u8) -> Address {
    Address([byte; 20])
}

#[test]
fn mines_coinbase_only_candidate_without_user_transactions() {
    let consensus = Consensus {
        config: ConsensusConfig { difficulty: 0 },
    };
    let mut ledger = Ledger::new();
    let miner = address(9);
    ledger.create_account(miner, Amount(0)).unwrap();
    let genesis = Block::new(
        Height(0),
        Hash([0; 64]),
        miner,
        1_700_000_000,
        Nonce(0),
        vec![],
    );
    ledger.apply_block(genesis).unwrap();
    let mempool = Mempool::new();

    let result = mine_candidate_block(
        &mempool,
        &ledger,
        &consensus,
        miner,
        1_700_000_000,
        MiningConfig {
            difficulty: 0,
            max_attempts: 1,
            transaction_limit: 10,
        },
    )
    .unwrap()
    .expect("coinbase-only block should be mineable");

    assert_eq!(result.block.transaction_count(), 0);
    assert!(result.block.coinbase.is_some());
    assert_eq!(consensus.validate_proof_of_work(&result.block), Ok(()));
}

#[test]
fn mines_candidate_block_until_pow_is_valid() {
    let consensus = Consensus {
        config: ConsensusConfig { difficulty: 0 },
    };
    let keypair = generate_keypair();
    let sender = address_from_public_key(&keypair.public_key);
    let receiver = address(2);
    let miner = address(9);
    let mut ledger = Ledger::new();
    ledger.create_account(sender, Amount(100)).unwrap();
    ledger.create_account(receiver, Amount(0)).unwrap();
    ledger.create_account(miner, Amount(0)).unwrap();

    let transaction = {
        let payload = Transaction::new_at(
            sender,
            receiver,
            Amount(10),
            Amount(BASE_FEE),
            Nonce(0),
            current_unix_timestamp(),
        );
        let signature = sign(&keypair.secret_key, &payload.signing_bytes());
        SignedTransaction::new(payload, keypair.public_key, signature)
    };
    let mut mempool = Mempool::new();
    mempool.insert_validated(&ledger, transaction).unwrap();

    let result = mine_candidate_block(
        &mempool,
        &ledger,
        &consensus,
        miner,
        1_700_000_000,
        MiningConfig {
            difficulty: 0,
            max_attempts: 1,
            transaction_limit: 10,
        },
    )
    .unwrap()
    .expect("difficulty 0 should produce a test block immediately");

    assert_eq!(result.attempts, 1);
    assert_eq!(result.block.difficulty(), 0);
    assert_eq!(result.block.transaction_count(), 1);
    assert_eq!(consensus.validate_proof_of_work(&result.block), Ok(()));
}

fn current_unix_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}
