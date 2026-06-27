use super::Node;
use crate::runtime::params::{BASE_FEE, BLOCK_REWARD_MATURITY};
use paqus::block::{Block, BlockError};
use paqus::consensus::{Consensus, ConsensusConfig, ConsensusError};
use paqus::crypto::{KeyPair, address_from_public_key, generate_keypair, sign};
use paqus::genesis::GENESIS_PREMINE_ADDRESS;
use paqus::ledger::Ledger;
use paqus::state::Account;
use paqus::transaction::{SignedTransaction, Transaction};
use paqus::types::{Address, Amount, Hash, Height, Nonce, PublicKey, Signature};
use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};

fn address(byte: u8) -> Address {
    Address([byte; 20])
}

fn signed_transaction_to(to: Address, amount: u32, nonce: u64) -> SignedTransaction {
    let keypair = generate_keypair();
    signed_transaction_from_keypair(&keypair, to, amount, nonce)
}

fn signed_transaction_from_keypair(
    keypair: &KeyPair,
    to: Address,
    amount: u32,
    nonce: u64,
) -> SignedTransaction {
    let from = address_from_public_key(&keypair.public_key);
    let payload = Transaction::new_at(
        from,
        to,
        Amount(amount),
        Amount(BASE_FEE),
        Nonce(nonce),
        current_unix_timestamp(),
    );
    let signature = sign(&keypair.secret_key, &payload.signing_bytes());
    SignedTransaction::new(payload, keypair.public_key, signature)
}

fn dummy_signed_transaction(nonce: u64) -> SignedTransaction {
    SignedTransaction::new(
        Transaction::new_at(
            address(1),
            address(2),
            Amount(10),
            Amount(BASE_FEE),
            Nonce(nonce),
            current_unix_timestamp(),
        ),
        PublicKey([1; 2592]),
        Signature([1; 4627]),
    )
}

fn current_unix_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}

fn block(height: u64, previous_hash: Hash, difficulty: u32, nonce: u64) -> Block {
    block_with_transactions(
        height,
        previous_hash,
        difficulty,
        nonce,
        vec![dummy_signed_transaction(height)],
    )
}

fn block_with_transactions(
    height: u64,
    previous_hash: Hash,
    difficulty: u32,
    nonce: u64,
    transactions: Vec<SignedTransaction>,
) -> Block {
    Block::with_difficulty(
        Height(height),
        previous_hash,
        address(9),
        difficulty,
        1_700_000_000 + height,
        Nonce(nonce),
        transactions,
    )
}

#[test]
fn submits_transaction_to_mempool() {
    let transaction = signed_transaction_to(address(2), 10, 0);
    let sender = transaction.transaction.from;
    let mut ledger = Ledger::new();
    ledger.create_account(sender, Amount(25)).unwrap();
    ledger.create_account(address(2), Amount(0)).unwrap();
    let mut node = Node::temporary(
        ledger,
        Consensus {
            config: ConsensusConfig { difficulty: 0 },
        },
    )
    .unwrap();

    let hash = transaction.hash();

    assert_eq!(node.submit_transaction(transaction).unwrap(), hash);
    assert!(node.mempool.contains(&hash));
}

#[test]
fn mines_and_applies_block_from_mempool() {
    let transaction = signed_transaction_to(address(2), 10, 0);
    let sender = transaction.transaction.from;
    let miner = address(9);
    let mut ledger = Ledger::new();
    ledger.create_account(sender, Amount(25)).unwrap();
    ledger.create_account(address(2), Amount(0)).unwrap();
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
    let mut node = Node::temporary(
        ledger,
        Consensus {
            config: ConsensusConfig { difficulty: 0 },
        },
    )
    .unwrap();

    node.submit_transaction(transaction).unwrap();
    let result = node.mine_block(miner, 1_700_000_001, 2_000, 10).unwrap();

    assert_eq!(result.block.height(), Height(1));
    assert_eq!(node.tip_height(), Some(Height(1)));
    assert!(node.mempool.is_empty());
    assert_eq!(node.balance(&sender), Some(Amount(13)));
    assert_eq!(node.balance(&address(2)), Some(Amount(10)));
}

#[test]
fn initializes_genesis_when_storage_is_empty() {
    let dir = tempfile_dir();
    let node = Node::init_or_load(&dir, Consensus::with_default_config()).unwrap();

    assert_eq!(node.tip_height(), Some(Height(0)));
    assert!(node.balance(&GENESIS_PREMINE_ADDRESS).is_some());
}

#[test]
fn stores_side_fork_without_changing_active_tip_when_work_is_lower() {
    let genesis = Block::new(
        Height(0),
        Hash([0; 64]),
        address(9),
        1_700_000_000,
        Nonce(0),
        vec![],
    );
    let active = block(1, genesis.hash(), 3, 1);
    let side = block(1, genesis.hash(), 1, 2);
    let side_hash = side.hash();
    let active_hash = active.hash();
    let mut ledger = Ledger::new();
    ledger.chain.insert_block(genesis).unwrap();
    ledger.chain.insert_block(active).unwrap();
    let mut node = Node::temporary(
        ledger,
        Consensus {
            config: ConsensusConfig { difficulty: 0 },
        },
    )
    .unwrap();

    assert!(node.apply_block(side).is_ok());
    assert!(node.fork_choice.contains(&side_hash));
    assert_eq!(node.tip_hash(), Some(active_hash));
}

#[test]
fn rejects_block_with_unexpected_difficulty() {
    let genesis = Block::new(
        Height(0),
        Hash([0; 64]),
        address(9),
        1_700_000_000,
        Nonce(0),
        vec![],
    );
    let mut ledger = Ledger::new();
    ledger.chain.insert_block(genesis.clone()).unwrap();
    let storage = crate::runtime::storage::Storage::temporary().unwrap();
    storage.save_ledger(&ledger).unwrap();
    let mut node = Node::new(ledger, storage, Consensus::with_default_config());
    let block = block(1, genesis.hash(), 2, 1);

    let error = node.apply_block(block).unwrap_err();

    assert!(matches!(
        error,
        crate::runtime::node::NodeError::Consensus(ConsensusError::UnexpectedDifficulty)
    ));
}

#[test]
fn rejects_block_timestamp_too_far_in_future() {
    let genesis = Block::new(
        Height(0),
        Hash([0; 64]),
        address(9),
        1_700_000_000,
        Nonce(0),
        vec![],
    );
    let mut ledger = Ledger::new();
    ledger.chain.insert_block(genesis.clone()).unwrap();
    let mut node = Node::temporary(
        ledger,
        Consensus {
            config: ConsensusConfig { difficulty: 0 },
        },
    )
    .unwrap();
    let mut block = block(1, genesis.hash(), 1, 1);
    block.header.timestamp = u64::MAX;

    let error = node.apply_block(block).unwrap_err();

    assert!(matches!(
        error,
        crate::runtime::node::NodeError::Consensus(ConsensusError::InvalidBlock(
            BlockError::FutureTimestamp
        ))
    ));
}

#[test]
fn reorgs_state_when_side_fork_becomes_best_tip() {
    let genesis = Block::new(
        Height(0),
        Hash([0; 64]),
        address(9),
        1_700_000_000,
        Nonce(0),
        vec![],
    );
    let active_keypair = generate_keypair();
    let side_keypair = generate_keypair();
    let active_sender = address_from_public_key(&active_keypair.public_key);
    let side_sender = address_from_public_key(&side_keypair.public_key);
    let active_receiver = address(3);
    let side_receiver = address(4);
    let active_transaction =
        signed_transaction_from_keypair(&active_keypair, active_receiver, 10, 0);
    let side_transaction = signed_transaction_from_keypair(&side_keypair, side_receiver, 10, 0);
    let mut active =
        block_with_transactions(1, genesis.hash(), 1, 1, vec![active_transaction.clone()]);
    let mut side = block_with_transactions(1, genesis.hash(), 4, 2, vec![side_transaction]);
    let mut genesis_accounts = BTreeMap::new();
    genesis_accounts.insert(active_sender, Account::new(active_sender, Amount(25)));
    genesis_accounts.insert(side_sender, Account::new(side_sender, Amount(25)));
    genesis_accounts.insert(active_receiver, Account::new(active_receiver, Amount(0)));
    genesis_accounts.insert(side_receiver, Account::new(side_receiver, Amount(0)));
    genesis_accounts.insert(address(9), Account::new(address(9), Amount(0)));

    let mut ledger = Ledger {
        accounts: genesis_accounts.clone(),
        chain: Default::default(),
    };
    ledger.chain.insert_block(genesis).unwrap();
    active.set_state_root(ledger.state_root_after_block(&active).unwrap());
    side.set_state_root(ledger.state_root_after_block(&side).unwrap());
    let side_hash = side.hash();
    ledger.apply_block(active).unwrap();
    let mut node = Node::with_genesis_accounts(
        ledger,
        crate::runtime::storage::Storage::temporary().unwrap(),
        Consensus {
            config: ConsensusConfig { difficulty: 0 },
        },
        genesis_accounts,
    );

    assert!(node.apply_block(side).is_ok());
    assert_eq!(
        node.fork_choice.best_tip().map(|node| node.hash),
        Some(side_hash)
    );
    assert_eq!(node.tip_hash(), Some(side_hash));
    assert_eq!(node.balance(&active_sender), Some(Amount(25)));
    assert_eq!(node.balance(&active_receiver), Some(Amount(0)));
    assert_eq!(node.balance(&side_sender), Some(Amount(13)));
    assert_eq!(node.balance(&side_receiver), Some(Amount(10)));
    assert!(node.mempool.contains(&active_transaction.hash()));
}

#[test]
fn reports_confirmed_available_and_pending_balances() {
    let genesis = Block::new(
        Height(0),
        Hash([0; 64]),
        address(9),
        1_700_000_000,
        Nonce(0),
        vec![],
    );
    let keypair = generate_keypair();
    let sender = address_from_public_key(&keypair.public_key);
    let receiver = address(2);
    let miner = address(9);
    let mut genesis_accounts = BTreeMap::new();
    genesis_accounts.insert(sender, Account::new(sender, Amount(100)));
    genesis_accounts.insert(receiver, Account::new(receiver, Amount(0)));
    genesis_accounts.insert(miner, Account::new(miner, Amount(0)));

    let mut ledger = Ledger {
        accounts: genesis_accounts.clone(),
        chain: Default::default(),
    };
    ledger.chain.insert_block(genesis).unwrap();

    for height in 1..=11 {
        let transaction = signed_transaction_from_keypair(&keypair, receiver, 1, height - 1);
        let mut block = block_with_transactions(
            height,
            ledger.tip_hash().unwrap(),
            1,
            height,
            vec![transaction],
        );
        block.set_state_root(ledger.state_root_after_block(&block).unwrap());
        ledger.apply_block(block).unwrap();
    }

    let mut node = Node::with_genesis_accounts(
        ledger,
        crate::runtime::storage::Storage::temporary().unwrap(),
        Consensus {
            config: ConsensusConfig { difficulty: 0 },
        },
        genesis_accounts,
    );
    let pending_transaction = signed_transaction_from_keypair(&keypair, receiver, 5, 11);

    node.submit_transaction(pending_transaction).unwrap();

    assert_eq!(node.confirmed_balance(&sender), Some(Amount(67)));
    assert_eq!(node.confirmed_balance(&receiver), Some(Amount(11)));
    assert_eq!(node.available_balance(&sender), Some(Amount(67)));
    assert_eq!(node.available_balance(&receiver), Some(Amount(10)));
    assert_eq!(
        node.account_view(&receiver),
        Some(crate::runtime::node::AccountView {
            balance: Amount(10),
            unspendable: Amount(1),
            nonce: Nonce(0),
        })
    );

    let sender_pending = node.pending_balance(&sender);
    assert_eq!(sender_pending.incoming, Amount(0));
    assert_eq!(sender_pending.outgoing, Amount(7));

    let receiver_pending = node.pending_balance(&receiver);
    assert_eq!(receiver_pending.incoming, Amount(5));
    assert_eq!(receiver_pending.outgoing, Amount(0));

    assert_eq!(
        node.account_view(&sender),
        Some(crate::runtime::node::AccountView {
            balance: Amount(67),
            unspendable: Amount(0),
            nonce: Nonce(11),
        })
    );
}

#[test]
fn keeps_mining_rewards_unspendable_until_maturity() {
    let genesis = Block::new(
        Height(0),
        Hash([0; 64]),
        address(9),
        1_700_000_000,
        Nonce(0),
        vec![],
    );
    let keypair = generate_keypair();
    let sender = address_from_public_key(&keypair.public_key);
    let receiver = address(2);
    let miner = address(9);
    let mut genesis_accounts = BTreeMap::new();
    genesis_accounts.insert(sender, Account::new(sender, Amount(1_000)));
    genesis_accounts.insert(receiver, Account::new(receiver, Amount(0)));
    genesis_accounts.insert(miner, Account::new(miner, Amount(0)));

    let mut ledger = Ledger {
        accounts: genesis_accounts.clone(),
        chain: Default::default(),
    };
    ledger.chain.insert_block(genesis).unwrap();

    for height in 1..=100 {
        let transaction = signed_transaction_from_keypair(&keypair, receiver, 1, height - 1);
        let mut block = block_with_transactions(
            height,
            ledger.tip_hash().unwrap(),
            1,
            height,
            vec![transaction],
        );
        block.set_state_root(ledger.state_root_after_block(&block).unwrap());
        ledger.apply_block(block).unwrap();
    }

    let mut node = Node::with_genesis_accounts(
        ledger,
        crate::runtime::storage::Storage::temporary().unwrap(),
        Consensus {
            config: ConsensusConfig { difficulty: 0 },
        },
        genesis_accounts,
    );

    let miner_view = node.account_view(&miner).unwrap();
    let matured_at_100 = 100_u64.saturating_sub(BLOCK_REWARD_MATURITY as u64);
    let matured_rewards_at_100 = (1..=matured_at_100)
        .map(|height| paqus::consensus::block_reward(Height(height)).0)
        .sum::<u32>();
    assert_eq!(
        miner_view.balance,
        Amount(matured_rewards_at_100 + 100 * BASE_FEE)
    );
    assert!(miner_view.unspendable.0 > 0);

    let transaction = signed_transaction_from_keypair(&keypair, receiver, 1, 100);
    let mut block =
        block_with_transactions(101, node.tip_hash().unwrap(), 1, 101, vec![transaction]);
    block.set_state_root(node.ledger.state_root_after_block(&block).unwrap());
    node.apply_block(block).unwrap();

    assert_eq!(
        node.account_view(&miner).unwrap().balance,
        Amount(
            (1..=101_u64.saturating_sub(BLOCK_REWARD_MATURITY as u64))
                .map(|height| paqus::consensus::block_reward(Height(height)).0)
                .sum::<u32>()
                + 101 * BASE_FEE
        )
    );
}

fn tempfile_dir() -> std::path::PathBuf {
    let mut path = std::env::temp_dir();
    path.push(format!(
        "paquscore-node-test-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    path
}
