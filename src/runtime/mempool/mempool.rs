use crate::runtime::mempool::error::MempoolError;
use crate::runtime::params::{
    DEFAULT_MARKET_FEE, DEFAULT_MIN_RELAY_FEE, HASH_SIZE, LOW_FEE_EXPIRY_SECS, MAX_MEMPOOL_BYTES,
    MAX_MEMPOOL_TXS, MAX_RELAY_TRANSACTION_AGE_SECS, MAX_RELAY_TRANSACTION_FUTURE_SECS,
    MEMPOOL_EXPIRY_SECS, MIN_RELAY_FEE_FLOOR,
};
use paqus::block::{Block, CoinbaseTransaction};
use paqus::ledger::{Ledger, LedgerError};
use paqus::state::StateError;
use paqus::transaction::SignedTransaction;
use paqus::types::{Address, BlockHash, BlockNonce, Hash, Height, TransactionHash};
use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Mempool {
    transactions: BTreeMap<TransactionHash, MempoolEntry>,
    config: MempoolConfig,
    total_bytes: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MempoolConfig {
    pub max_transactions: usize,
    pub max_bytes: usize,
    pub transaction_ttl_secs: u64,
    pub low_fee_ttl_secs: u64,
    pub min_relay_fee: u32,
    pub market_fee: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct MempoolEntry {
    transaction: SignedTransaction,
    inserted_at: u64,
}

impl Default for MempoolConfig {
    fn default() -> Self {
        Self {
            max_transactions: MAX_MEMPOOL_TXS,
            max_bytes: MAX_MEMPOOL_BYTES,
            transaction_ttl_secs: MEMPOOL_EXPIRY_SECS,
            low_fee_ttl_secs: LOW_FEE_EXPIRY_SECS,
            min_relay_fee: DEFAULT_MIN_RELAY_FEE,
            market_fee: DEFAULT_MARKET_FEE,
        }
    }
}

impl Mempool {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_config(config: MempoolConfig) -> Self {
        Self {
            transactions: BTreeMap::new(),
            config,
            total_bytes: 0,
        }
    }

    pub fn config(&self) -> MempoolConfig {
        self.config
    }

    pub fn insert(
        &mut self,
        transaction: SignedTransaction,
    ) -> Result<TransactionHash, MempoolError> {
        self.insert_at(transaction, current_unix_timestamp())
    }

    pub fn insert_at(
        &mut self,
        transaction: SignedTransaction,
        now: u64,
    ) -> Result<TransactionHash, MempoolError> {
        self.prune_expired(now);
        self.validate_timestamp_policy(&transaction, now)?;
        transaction.validate_signed_at(now)?;
        self.validate_fee_policy(&transaction)?;
        self.insert_unchecked(transaction, now, None)
    }

    pub fn insert_validated(
        &mut self,
        ledger: &Ledger,
        transaction: SignedTransaction,
    ) -> Result<TransactionHash, MempoolError> {
        self.insert_validated_at(ledger, transaction, current_unix_timestamp())
    }

    pub fn insert_validated_at(
        &mut self,
        ledger: &Ledger,
        transaction: SignedTransaction,
        now: u64,
    ) -> Result<TransactionHash, MempoolError> {
        self.prune_expired(now);
        self.validate_timestamp_policy(&transaction, now)?;
        transaction.validate_signed_at(now)?;
        self.validate_fee_policy(&transaction)?;
        let replacement = self.replacement_candidate(&transaction)?;
        self.validate_against_ledger_excluding(ledger, &transaction, replacement)?;
        self.insert_unchecked(transaction, now, replacement)
    }

    fn insert_unchecked(
        &mut self,
        transaction: SignedTransaction,
        inserted_at: u64,
        replacement: Option<TransactionHash>,
    ) -> Result<TransactionHash, MempoolError> {
        let hash = transaction.hash();
        if self.transactions.contains_key(&hash) {
            return Err(MempoolError::DuplicateTransaction);
        }
        let transaction_size = transaction.serialized_size();

        let replacement_size = replacement
            .and_then(|hash| self.transactions.get(&hash))
            .map(|entry| entry.transaction.serialized_size())
            .unwrap_or(0);

        if replacement.is_none() && self.transactions.len() >= self.config.max_transactions {
            return Err(MempoolError::MempoolFull);
        }

        if self
            .total_bytes
            .saturating_sub(replacement_size)
            .saturating_add(transaction_size)
            > self.config.max_bytes
        {
            return Err(MempoolError::MempoolFull);
        }

        if let Some(replacement) = replacement {
            self.remove(&replacement);
        }
        self.transactions.insert(
            hash,
            MempoolEntry {
                transaction,
                inserted_at,
            },
        );
        self.total_bytes = self.total_bytes.saturating_add(transaction_size);
        Ok(hash)
    }

    fn replacement_candidate(
        &self,
        transaction: &SignedTransaction,
    ) -> Result<Option<TransactionHash>, MempoolError> {
        let replacement = self
            .transactions
            .iter()
            .find(|(_, entry)| {
                entry.transaction.transaction.from == transaction.transaction.from
                    && entry.transaction.transaction.nonce == transaction.transaction.nonce
            })
            .map(|(hash, entry)| (*hash, entry.transaction.transaction.fee));

        let Some((hash, old_fee)) = replacement else {
            return Ok(None);
        };

        if transaction.transaction.fee.0 <= old_fee.0 {
            return Err(MempoolError::ReplacementFeeTooLow);
        }

        Ok(Some(hash))
    }

    fn validate_fee_policy(&self, transaction: &SignedTransaction) -> Result<(), MempoolError> {
        let min_relay_fee = self.config.min_relay_fee.max(MIN_RELAY_FEE_FLOOR);
        if transaction.transaction.fee.0 < min_relay_fee {
            return Err(MempoolError::FeeTooLow);
        }
        Ok(())
    }

    fn validate_timestamp_policy(
        &self,
        transaction: &SignedTransaction,
        now: u64,
    ) -> Result<(), MempoolError> {
        if transaction.transaction.timestamp > now.saturating_add(MAX_RELAY_TRANSACTION_FUTURE_SECS)
        {
            return Err(MempoolError::InvalidTransaction(
                paqus::transaction::TransactionError::FromFuture,
            ));
        }
        if now.saturating_sub(transaction.transaction.timestamp) > MAX_RELAY_TRANSACTION_AGE_SECS {
            return Err(MempoolError::InvalidTransaction(
                paqus::transaction::TransactionError::Expired,
            ));
        }
        Ok(())
    }

    pub fn validate_against_ledger(
        &self,
        ledger: &Ledger,
        transaction: &SignedTransaction,
    ) -> Result<(), MempoolError> {
        transaction.validate_signed()?;
        self.validate_fee_policy(transaction)?;
        self.validate_against_ledger_excluding(ledger, transaction, None)
    }

    fn validate_against_ledger_excluding(
        &self,
        ledger: &Ledger,
        transaction: &SignedTransaction,
        excluded: Option<TransactionHash>,
    ) -> Result<(), MempoolError> {
        transaction.validate_signed()?;
        self.validate_fee_policy(transaction)?;

        let payload = &transaction.transaction;
        let sender = ledger
            .account(&payload.from)
            .ok_or(LedgerError::AccountNotFound)?;

        let current_height = ledger.tip_height().unwrap_or(Height(0));
        let mut expected_nonce = sender.nonce;
        let mut spendable = sender.available_balance_at(current_height);
        let mut pending_from_sender: Vec<_> = self
            .transactions
            .iter()
            .filter(|(hash, _)| Some(**hash) != excluded)
            .map(|(_, entry)| &entry.transaction)
            .filter(|pending| pending.transaction.from == payload.from)
            .collect();
        pending_from_sender.sort_by_key(|pending| pending.transaction.nonce);

        for pending in pending_from_sender {
            if pending.transaction.nonce != expected_nonce {
                return Err(LedgerError::InvalidState(StateError::InvalidNonce).into());
            }

            let total = pending
                .transaction
                .amount
                .0
                .checked_add(pending.transaction.fee.0)
                .ok_or(LedgerError::InvalidState(StateError::BalanceOverflow))?;
            if spendable.0 < total {
                return Err(LedgerError::InvalidState(StateError::InsufficientBalance).into());
            }

            spendable.0 -= total;
            expected_nonce.0 = expected_nonce.0.saturating_add(1);
        }

        if payload.nonce != expected_nonce {
            return Err(LedgerError::InvalidState(StateError::InvalidNonce).into());
        }

        let total = payload
            .amount
            .0
            .checked_add(payload.fee.0)
            .ok_or(LedgerError::InvalidState(StateError::BalanceOverflow))?;
        if spendable.0 < total {
            return Err(LedgerError::InvalidState(StateError::InsufficientBalance).into());
        }

        Ok(())
    }

    pub fn remove(&mut self, hash: &TransactionHash) -> Option<SignedTransaction> {
        self.transactions.remove(hash).map(|entry| {
            self.total_bytes = self
                .total_bytes
                .saturating_sub(entry.transaction.serialized_size());
            entry.transaction
        })
    }

    pub fn get(&self, hash: &TransactionHash) -> Option<&SignedTransaction> {
        self.transactions.get(hash).map(|entry| &entry.transaction)
    }

    pub fn transactions(&self) -> impl Iterator<Item = &SignedTransaction> {
        self.transactions.values().map(|entry| &entry.transaction)
    }

    pub fn contains(&self, hash: &TransactionHash) -> bool {
        self.transactions.contains_key(hash)
    }

    pub fn len(&self) -> usize {
        self.transactions.len()
    }

    pub fn total_bytes(&self) -> usize {
        self.total_bytes
    }

    pub fn is_empty(&self) -> bool {
        self.transactions.is_empty()
    }

    pub fn clear(&mut self) {
        self.transactions.clear();
        self.total_bytes = 0;
    }

    pub fn prune_expired(&mut self, now: u64) -> usize {
        let before = self.transactions.len();
        let mut retained_bytes = 0_usize;
        self.transactions.retain(|_, entry| {
            let ttl = if entry.transaction.transaction.fee.0 < self.config.market_fee {
                self.config.low_fee_ttl_secs
            } else {
                self.config.transaction_ttl_secs
            };
            let retain = now.saturating_sub(entry.inserted_at) <= ttl;
            if retain {
                retained_bytes = retained_bytes.saturating_add(entry.transaction.serialized_size());
            }
            retain
        });
        self.total_bytes = retained_bytes;
        before.saturating_sub(self.transactions.len())
    }

    pub fn select_for_block(&self, limit: usize) -> Vec<SignedTransaction> {
        let mut by_sender: BTreeMap<Address, Vec<SignedTransaction>> = BTreeMap::new();
        for transaction in self.transactions() {
            by_sender
                .entry(transaction.transaction.from)
                .or_default()
                .push(transaction.clone());
        }
        for transactions in by_sender.values_mut() {
            transactions
                .sort_by_key(|transaction| (transaction.transaction.nonce, transaction.hash()));
        }

        let mut selected = Vec::new();
        while selected.len() < limit {
            let Some(sender) = by_sender
                .iter()
                .filter_map(|(sender, transactions)| {
                    transactions.first().map(|transaction| {
                        (
                            *sender,
                            transaction.transaction.fee.0,
                            transaction.transaction.nonce,
                            transaction.hash(),
                        )
                    })
                })
                .max_by(|left, right| {
                    left.1
                        .cmp(&right.1)
                        .then_with(|| right.2.cmp(&left.2))
                        .then_with(|| right.3.cmp(&left.3))
                })
                .map(|candidate| candidate.0)
            else {
                break;
            };

            let transactions = by_sender
                .get_mut(&sender)
                .expect("selected sender should exist");
            selected.push(transactions.remove(0));
            if transactions.is_empty() {
                by_sender.remove(&sender);
            }
        }

        selected
    }

    pub fn create_candidate_block(
        &self,
        ledger: &Ledger,
        miner_address: Address,
        timestamp: u64,
        nonce: BlockNonce,
        transaction_limit: usize,
    ) -> Result<Block, LedgerError> {
        let height = ledger
            .tip_height()
            .map(|height| Height(height.0.saturating_add(1)))
            .unwrap_or(Height(0));
        let previous_hash = ledger.tip_hash().unwrap_or(BlockHash([0; HASH_SIZE]));

        let transactions = self.select_for_block(transaction_limit);
        let fees = paqus::types::Amount(
            transactions
                .iter()
                .map(|transaction| transaction.transaction.fee.0)
                .sum(),
        );
        let coinbase = if height.0 == 0 && previous_hash == Hash([0; HASH_SIZE]) {
            None
        } else {
            Some(CoinbaseTransaction::new(
                miner_address,
                ledger.mintable_subsidy(height)?,
                fees,
            ))
        };

        let mut block = Block::with_coinbase(
            height,
            previous_hash,
            miner_address,
            crate::runtime::params::DIFFICULTY_START,
            timestamp,
            nonce,
            coinbase,
            transactions,
        );
        let state_root = ledger.state_root_after_block(&block)?;
        block.set_state_root(state_root);
        Ok(block)
    }

    pub fn remove_confirmed(&mut self, block: &Block) {
        for transaction in &block.transactions {
            self.transactions.remove(&transaction.hash());
        }
    }
}

fn current_unix_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use paqus::crypto::{address_from_public_key, generate_keypair, sign};
    use paqus::ledger::Ledger;
    use paqus::transaction::{SignedTransaction, Transaction};
    use paqus::types::{Amount, Nonce};

    fn address(byte: u8) -> Address {
        Address([byte; 20])
    }

    fn signed_transaction_from(
        secret_key: &paqus::types::SecretKey,
        public_key: paqus::types::PublicKey,
        to: Address,
        amount: u32,
        nonce: u64,
    ) -> SignedTransaction {
        signed_transaction_from_with_fee_at(
            secret_key,
            public_key,
            to,
            amount,
            crate::runtime::params::DEFAULT_TRANSACTION_FEE,
            nonce,
            current_unix_timestamp(),
        )
    }

    fn signed_transaction_from_with_fee_at(
        secret_key: &paqus::types::SecretKey,
        public_key: paqus::types::PublicKey,
        to: Address,
        amount: u32,
        fee: u32,
        nonce: u64,
        timestamp: u64,
    ) -> SignedTransaction {
        let from = address_from_public_key(&public_key);
        let payload = Transaction::new_at(
            from,
            to,
            Amount(amount),
            Amount(fee),
            Nonce(nonce),
            timestamp,
        );
        let signature = sign(secret_key, &payload.signing_bytes());

        SignedTransaction::new(payload, public_key, signature)
    }

    #[test]
    fn prunes_low_fee_transactions_after_low_fee_expiry() {
        let keypair = generate_keypair();
        let transaction = signed_transaction_from_with_fee_at(
            &keypair.secret_key,
            keypair.public_key,
            address(2),
            10,
            1,
            0,
            1_000,
        );
        let hash = transaction.hash();
        let mut mempool = Mempool::with_config(MempoolConfig {
            min_relay_fee: 1,
            market_fee: 5,
            low_fee_ttl_secs: crate::runtime::params::LOW_FEE_EXPIRY_SECS,
            transaction_ttl_secs: crate::runtime::params::MEMPOOL_EXPIRY_SECS,
            ..MempoolConfig::default()
        });

        mempool.insert_at(transaction, 1_000).unwrap();

        assert_eq!(
            mempool.prune_expired(1_000 + crate::runtime::params::LOW_FEE_EXPIRY_SECS + 1),
            1
        );
        assert!(!mempool.contains(&hash));
    }

    #[test]
    fn keeps_market_fee_transactions_until_full_mempool_expiry() {
        let keypair = generate_keypair();
        let transaction = signed_transaction_from_with_fee_at(
            &keypair.secret_key,
            keypair.public_key,
            address(2),
            10,
            crate::runtime::params::DEFAULT_MARKET_FEE,
            0,
            1_000,
        );
        let hash = transaction.hash();
        let mut mempool = Mempool::with_config(MempoolConfig {
            market_fee: crate::runtime::params::DEFAULT_MARKET_FEE,
            low_fee_ttl_secs: crate::runtime::params::LOW_FEE_EXPIRY_SECS,
            transaction_ttl_secs: crate::runtime::params::MEMPOOL_EXPIRY_SECS,
            ..MempoolConfig::default()
        });

        mempool.insert_at(transaction, 1_000).unwrap();

        assert_eq!(
            mempool.prune_expired(1_000 + crate::runtime::params::LOW_FEE_EXPIRY_SECS + 1),
            0
        );
        assert!(mempool.contains(&hash));
        assert_eq!(
            mempool.prune_expired(1_000 + crate::runtime::params::MEMPOOL_EXPIRY_SECS + 1),
            1
        );
        assert!(!mempool.contains(&hash));
    }

    #[test]
    fn candidate_block_caps_subsidy_to_remaining_mined_supply() {
        let keypair = generate_keypair();
        let from = address_from_public_key(&keypair.public_key);
        let to = address(2);
        let miner = address(9);
        let mut ledger = Ledger::new();
        ledger
            .create_account(from, Amount(paqus::params::MAX_UNIT_SUPPLY - 50))
            .unwrap();
        ledger.create_account(to, Amount(0)).unwrap();
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
        let transaction =
            signed_transaction_from(&keypair.secret_key, keypair.public_key, to, 1, 0);
        mempool.insert_validated(&ledger, transaction).unwrap();

        let block = mempool
            .create_candidate_block(&ledger, miner, 1_700_000_001, Nonce(0), 10)
            .unwrap();

        assert_eq!(block.coinbase.as_ref().unwrap().subsidy, Amount(50));
        assert_eq!(ledger.apply_block(block), Ok(()));
    }

    #[test]
    fn candidate_block_uses_zero_subsidy_after_mined_supply_is_exhausted() {
        let keypair = generate_keypair();
        let from = address_from_public_key(&keypair.public_key);
        let to = address(2);
        let miner = address(9);
        let mut ledger = Ledger::new();
        ledger
            .create_account(from, Amount(paqus::params::MAX_UNIT_SUPPLY))
            .unwrap();
        ledger.create_account(to, Amount(0)).unwrap();
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
        let transaction =
            signed_transaction_from(&keypair.secret_key, keypair.public_key, to, 1, 0);
        mempool.insert_validated(&ledger, transaction).unwrap();

        let block = mempool
            .create_candidate_block(&ledger, miner, 1_700_000_001, Nonce(0), 10)
            .unwrap();
        let coinbase = block.coinbase.as_ref().unwrap();

        assert_eq!(coinbase.subsidy, Amount(0));
        assert_eq!(coinbase.fees, Amount(crate::runtime::params::BASE_FEE));
        assert_eq!(ledger.apply_block(block), Ok(()));
    }
}
