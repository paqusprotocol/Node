use crate::runtime::cache::CoreCache;
use crate::runtime::mempool::error::MempoolError;
use crate::runtime::params::{
    DEFAULT_MARKET_FEE, DEFAULT_MIN_RELAY_FEE, DYNAMIC_MARKET_FEE_MAX_MULTIPLIER,
    FEE_RATE_UNIT_BYTES, HASH_SIZE, LOW_FEE_EXPIRY_SECS, MAX_MEMPOOL_BYTES, MAX_MEMPOOL_TXS,
    MAX_RELAY_TRANSACTION_AGE_SECS, MAX_RELAY_TRANSACTION_FUTURE_SECS, MEMPOOL_EXPIRY_SECS,
    MIN_RELAY_FEE_FLOOR,
};
use paqus::block::{
    Block, BlockNonce, CoinbaseTransaction, Height, MAX_BLOCK_SIZE, MAX_BLOCK_WEIGHT, Nonce,
};
use paqus::consensus::supply::Amount;
use paqus::crypto::{Address, BlockHash, Hash, TransactionHash};
use paqus::ledger::{Ledger, LedgerError};
use paqus::state::StateError;
use paqus::transaction::{AccountNonce, SignedTransaction};
use std::cmp::Reverse;
use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};

type FeeHeadKey = (u64, u64, Reverse<AccountNonce>, Reverse<TransactionHash>);
type ExpiryKey = (u64, TransactionHash);

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Mempool {
    transactions: BTreeMap<TransactionHash, MempoolEntry>,
    by_sender_nonce: BTreeMap<(Address, AccountNonce), TransactionHash>,
    head_by_sender: BTreeMap<Address, (TransactionHash, FeeHeadKey)>,
    heads_by_fee: BTreeMap<FeeHeadKey, TransactionHash>,
    expires_by_hash: BTreeMap<TransactionHash, ExpiryKey>,
    expiries: BTreeMap<ExpiryKey, TransactionHash>,
    by_address: BTreeMap<(Address, TransactionHash, bool), TransactionHash>,
    config: MempoolConfig,
    total_bytes: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MempoolConfig {
    pub max_transactions: usize,
    pub max_bytes: usize,
    pub transaction_ttl_secs: u64,
    pub low_fee_ttl_secs: u64,
    pub min_relay_fee: u64,
    pub market_fee: u64,
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
            by_sender_nonce: BTreeMap::new(),
            head_by_sender: BTreeMap::new(),
            heads_by_fee: BTreeMap::new(),
            expires_by_hash: BTreeMap::new(),
            expiries: BTreeMap::new(),
            by_address: BTreeMap::new(),
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

    pub fn insert_at_with_cache(
        &mut self,
        transaction: SignedTransaction,
        now: u64,
        cache: &mut CoreCache,
    ) -> Result<TransactionHash, MempoolError> {
        self.prune_expired(now);
        self.validate_timestamp_policy(&transaction, now)?;
        cache.validate_signed_transaction_at(&transaction, now)?;
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

    pub fn insert_validated_at_with_cache(
        &mut self,
        ledger: &Ledger,
        transaction: SignedTransaction,
        now: u64,
        cache: &mut CoreCache,
    ) -> Result<TransactionHash, MempoolError> {
        self.prune_expired(now);
        self.validate_timestamp_policy(&transaction, now)?;
        cache.validate_signed_transaction_at(&transaction, now)?;
        self.validate_fee_policy(&transaction)?;
        let replacement = self.replacement_candidate(&transaction)?;
        self.validate_against_ledger_excluding_with_cache(
            ledger,
            &transaction,
            replacement,
            cache,
        )?;
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
        let sender_nonce = (transaction.transaction.from, transaction.transaction.nonce);
        if let Some(existing) = self.by_sender_nonce.get(&sender_nonce)
            && Some(*existing) != replacement
        {
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
        let expiry_key = (self.expires_at(&transaction, inserted_at), hash);
        self.transactions.insert(
            hash,
            MempoolEntry {
                transaction,
                inserted_at,
            },
        );
        self.by_sender_nonce.insert(sender_nonce, hash);
        self.refresh_sender_head(sender_nonce.0);
        self.expires_by_hash.insert(hash, expiry_key);
        self.expiries.insert(expiry_key, hash);
        if let Some(entry) = self.transactions.get(&hash) {
            let transaction = &entry.transaction.transaction;
            self.by_address.insert((transaction.from, hash, true), hash);
            if transaction.to != transaction.from {
                self.by_address.insert((transaction.to, hash, false), hash);
            }
        }
        self.total_bytes = self.total_bytes.saturating_add(transaction_size);
        Ok(hash)
    }

    fn replacement_candidate(
        &self,
        transaction: &SignedTransaction,
    ) -> Result<Option<TransactionHash>, MempoolError> {
        let replacement = self
            .by_sender_nonce
            .get(&(transaction.transaction.from, transaction.transaction.nonce))
            .and_then(|hash| {
                self.transactions
                    .get(hash)
                    .map(|entry| (*hash, entry.transaction.transaction.fee))
            });

        let Some((hash, old_fee)) = replacement else {
            return Ok(None);
        };

        if transaction.transaction.fee.0 <= old_fee.0 {
            return Err(MempoolError::ReplacementFeeTooLow);
        }

        Ok(Some(hash))
    }

    fn validate_fee_policy(&self, transaction: &SignedTransaction) -> Result<(), MempoolError> {
        let min_relay_fee = required_fee_for_rate(
            self.config.min_relay_fee.max(MIN_RELAY_FEE_FLOOR),
            transaction.virtual_size(),
        )
        .max(MIN_RELAY_FEE_FLOOR);
        if transaction.transaction.fee.0 < min_relay_fee {
            return Err(MempoolError::FeeTooLow);
        }
        Ok(())
    }

    fn transaction_ttl(&self, transaction: &SignedTransaction) -> u64 {
        let market_fee =
            required_fee_for_rate(self.dynamic_market_fee_rate(), transaction.virtual_size());
        if transaction.transaction.fee.0 < market_fee {
            self.config.low_fee_ttl_secs
        } else {
            self.config.transaction_ttl_secs
        }
    }

    fn expires_at(&self, transaction: &SignedTransaction, inserted_at: u64) -> u64 {
        inserted_at
            .saturating_add(self.transaction_ttl(transaction))
            .saturating_add(1)
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

    pub fn validate_against_ledger_with_cache(
        &self,
        ledger: &Ledger,
        transaction: &SignedTransaction,
        cache: &mut CoreCache,
    ) -> Result<(), MempoolError> {
        cache.validate_signed_transaction(transaction)?;
        self.validate_fee_policy(transaction)?;
        self.validate_against_ledger_excluding_with_cache(ledger, transaction, None, cache)
    }

    fn validate_against_ledger_excluding(
        &self,
        ledger: &Ledger,
        transaction: &SignedTransaction,
        excluded: Option<TransactionHash>,
    ) -> Result<(), MempoolError> {
        transaction.validate_signed()?;
        self.validate_fee_policy(transaction)?;
        self.validate_ledger_fit_excluding(ledger, transaction, excluded)
    }

    fn validate_against_ledger_excluding_with_cache(
        &self,
        ledger: &Ledger,
        transaction: &SignedTransaction,
        excluded: Option<TransactionHash>,
        cache: &mut CoreCache,
    ) -> Result<(), MempoolError> {
        cache.validate_signed_transaction(transaction)?;
        self.validate_fee_policy(transaction)?;
        self.validate_ledger_fit_excluding(ledger, transaction, excluded)
    }

    fn validate_ledger_fit_excluding(
        &self,
        ledger: &Ledger,
        transaction: &SignedTransaction,
        excluded: Option<TransactionHash>,
    ) -> Result<(), MempoolError> {
        let payload = &transaction.transaction;
        let sender = ledger
            .account(&payload.from)
            .ok_or(LedgerError::AccountNotFound)?;

        let current_height = ledger.tip_height().unwrap_or(Height(0));
        let mut expected_nonce = sender.nonce;
        let mut spendable = sender.available_balance_at(current_height);
        for pending in self.pending_from_sender(payload.from, excluded) {
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

    fn pending_from_sender(
        &self,
        sender: Address,
        excluded: Option<TransactionHash>,
    ) -> impl Iterator<Item = &SignedTransaction> {
        self.by_sender_nonce
            .range((sender, Nonce(0))..=(sender, Nonce(u64::MAX)))
            .filter_map(move |(_, hash)| {
                if Some(*hash) == excluded {
                    return None;
                }
                self.transactions.get(hash).map(|entry| &entry.transaction)
            })
    }

    fn head_key(&self, hash: TransactionHash) -> Option<FeeHeadKey> {
        let transaction = &self.transactions.get(&hash)?.transaction;
        let fee = transaction.transaction.fee.0;
        Some((
            fee_rate_key(fee, transaction.virtual_size()),
            fee,
            Reverse(transaction.transaction.nonce),
            Reverse(hash),
        ))
    }

    fn first_sender_hash(&self, sender: Address) -> Option<TransactionHash> {
        self.by_sender_nonce
            .range((sender, Nonce(0))..=(sender, Nonce(u64::MAX)))
            .next()
            .map(|(_, hash)| *hash)
    }

    fn remove_sender_head(&mut self, sender: Address) {
        if let Some((_, key)) = self.head_by_sender.remove(&sender) {
            self.heads_by_fee.remove(&key);
        }
    }

    fn refresh_sender_head(&mut self, sender: Address) {
        self.remove_sender_head(sender);
        let Some(hash) = self.first_sender_hash(sender) else {
            return;
        };
        let Some(key) = self.head_key(hash) else {
            return;
        };
        self.head_by_sender.insert(sender, (hash, key));
        self.heads_by_fee.insert(key, hash);
    }

    pub fn remove(&mut self, hash: &TransactionHash) -> Option<SignedTransaction> {
        self.transactions.remove(hash).map(|entry| {
            let sender = entry.transaction.transaction.from;
            self.by_sender_nonce
                .remove(&(sender, entry.transaction.transaction.nonce));
            self.refresh_sender_head(sender);
            if let Some(expiry_key) = self.expires_by_hash.remove(hash) {
                self.expiries.remove(&expiry_key);
            }
            self.by_address
                .remove(&(entry.transaction.transaction.from, *hash, true));
            if entry.transaction.transaction.to != entry.transaction.transaction.from {
                self.by_address
                    .remove(&(entry.transaction.transaction.to, *hash, false));
            }
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

    pub fn transactions_for_address(
        &self,
        address: &Address,
    ) -> impl Iterator<Item = &SignedTransaction> {
        self.by_address
            .range(
                (*address, TransactionHash([0; HASH_SIZE]), false)
                    ..=(*address, TransactionHash([u8::MAX; HASH_SIZE]), true),
            )
            .filter_map(|(_, hash)| self.transactions.get(hash).map(|entry| &entry.transaction))
    }

    pub fn contains(&self, hash: &TransactionHash) -> bool {
        self.transactions.contains_key(hash)
    }

    pub fn contains_sender(&self, sender: Address) -> bool {
        self.by_sender_nonce
            .range((sender, Nonce(0))..=(sender, Nonce(u64::MAX)))
            .next()
            .is_some()
    }

    pub fn len(&self) -> usize {
        self.transactions.len()
    }

    pub fn total_bytes(&self) -> usize {
        self.total_bytes
    }

    pub fn mempool_pressure_bps(&self) -> u64 {
        let byte_pressure = occupancy_bps(self.total_bytes, self.config.max_bytes);
        let tx_pressure = occupancy_bps(self.transactions.len(), self.config.max_transactions);
        byte_pressure.max(tx_pressure)
    }

    pub fn dynamic_market_fee_rate(&self) -> u64 {
        let base_rate = self.config.market_fee.max(self.config.min_relay_fee);
        let pressure_bps = self.mempool_pressure_bps();
        let pressure_premium = base_rate
            .saturating_mul(DYNAMIC_MARKET_FEE_MAX_MULTIPLIER)
            .saturating_mul(pressure_bps)
            .saturating_add(9_999)
            / 10_000;
        base_rate.saturating_add(pressure_premium)
    }

    pub fn is_empty(&self) -> bool {
        self.transactions.is_empty()
    }

    pub fn clear(&mut self) {
        self.transactions.clear();
        self.by_sender_nonce.clear();
        self.head_by_sender.clear();
        self.heads_by_fee.clear();
        self.expires_by_hash.clear();
        self.expiries.clear();
        self.by_address.clear();
        self.total_bytes = 0;
    }

    pub fn prune_expired(&mut self, now: u64) -> usize {
        let expired: Vec<_> = self
            .expiries
            .range(..=(now, TransactionHash([u8::MAX; HASH_SIZE])))
            .map(|(_, hash)| *hash)
            .collect();
        let removed = expired.len();
        for hash in expired {
            self.remove(&hash);
        }
        removed
    }

    pub fn select_for_block(&self, limit: usize) -> Vec<SignedTransaction> {
        self.select_for_block_with_min_fee_rate(limit, self.dynamic_market_fee_rate())
    }

    pub fn select_for_block_with_min_fee_rate(
        &self,
        limit: usize,
        min_fee_rate: u64,
    ) -> Vec<SignedTransaction> {
        self.select_for_block_with_policy(limit, min_fee_rate, None)
    }

    pub fn select_for_block_at_height(
        &self,
        limit: usize,
        min_fee_rate: u64,
        height: Height,
    ) -> Vec<SignedTransaction> {
        self.select_for_block_with_policy(limit, min_fee_rate, Some(height))
    }

    fn select_for_block_with_policy(
        &self,
        limit: usize,
        min_fee_rate: u64,
        height: Option<Height>,
    ) -> Vec<SignedTransaction> {
        let mut heads_by_fee = self.heads_by_fee.clone();
        let mut head_by_sender = self.head_by_sender.clone();
        let mut selected = Vec::new();
        while selected.len() < limit {
            let Some((key, hash)) = heads_by_fee.pop_last() else {
                break;
            };
            let Some(transaction) = self.transactions.get(&hash).map(|entry| &entry.transaction)
            else {
                continue;
            };
            if fee_rate_key(transaction.transaction.fee.0, transaction.virtual_size())
                < min_fee_rate
            {
                continue;
            }
            if height.is_some_and(|height| {
                transaction
                    .transaction
                    .validity
                    .validate_at(height)
                    .is_err()
            }) {
                continue;
            }
            let sender = transaction.transaction.from;
            if head_by_sender.get(&sender) != Some(&(hash, key)) {
                continue;
            }

            selected.push(transaction.clone());
            head_by_sender.remove(&sender);

            let next_hash = self
                .by_sender_nonce
                .range(
                    (
                        sender,
                        Nonce(transaction.transaction.nonce.0.saturating_add(1)),
                    )..=(sender, Nonce(u64::MAX)),
                )
                .next()
                .map(|(_, hash)| *hash);
            if let Some(next_hash) = next_hash
                && let Some(next_key) = self.head_key(next_hash)
            {
                head_by_sender.insert(sender, (next_hash, next_key));
                heads_by_fee.insert(next_key, next_hash);
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
        self.create_candidate_block_with_min_fee_rate(
            ledger,
            miner_address,
            timestamp,
            nonce,
            transaction_limit,
            self.dynamic_market_fee_rate(),
        )
    }

    pub fn create_candidate_block_with_min_fee_rate(
        &self,
        ledger: &Ledger,
        miner_address: Address,
        timestamp: u64,
        nonce: BlockNonce,
        transaction_limit: usize,
        min_fee_rate: u64,
    ) -> Result<Block, LedgerError> {
        let height = ledger
            .tip_height()
            .map(|height| Height(height.0.saturating_add(1)))
            .unwrap_or(Height(0));
        let previous_hash = ledger.tip_hash().unwrap_or(BlockHash([0; HASH_SIZE]));

        let transactions = self.select_for_block_at_height(transaction_limit, min_fee_rate, height);
        let fees = Amount(
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
                ledger.mintable_subsidy(height),
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
        while block.serialized_size() > MAX_BLOCK_SIZE || block.weight() > MAX_BLOCK_WEIGHT {
            if block.transactions.pop().is_none() {
                break;
            }
            if let Some(coinbase) = &mut block.coinbase {
                coinbase.fees = Amount(
                    block
                        .transactions
                        .iter()
                        .map(|transaction| transaction.transaction.fee.0)
                        .sum(),
                );
            }
            block.refresh_commitments();
        }
        let state_root = ledger.state_root_after_block(&block)?;
        block.set_state_root(state_root);
        Ok(block)
    }

    pub fn remove_confirmed(&mut self, block: &Block) {
        for transaction in &block.transactions {
            self.remove(&transaction.hash());
        }
    }
}

fn current_unix_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}

fn required_fee_for_rate(rate: u64, transaction_size: usize) -> u64 {
    if rate == 0 || transaction_size == 0 {
        return 0;
    }
    let size = transaction_size as u64;
    size.saturating_mul(rate)
        .saturating_add(FEE_RATE_UNIT_BYTES as u64 - 1)
        / FEE_RATE_UNIT_BYTES as u64
}

fn fee_rate_key(fee: u64, transaction_size: usize) -> u64 {
    if transaction_size == 0 {
        return u64::MAX;
    }
    fee.saturating_mul(FEE_RATE_UNIT_BYTES as u64) / transaction_size as u64
}

fn occupancy_bps(used: usize, capacity: usize) -> u64 {
    if capacity == 0 {
        return 10_000;
    }
    ((used as u128).saturating_mul(10_000) / capacity as u128).min(10_000) as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use paqus::block::Nonce;
    use paqus::crypto::{PublicKey, SecretKey, address_from_public_key, generate_keypair, sign};
    use paqus::ledger::Ledger;
    use paqus::transaction::{SignedTransaction, Transaction, ValidityWindow};

    fn address(byte: u8) -> Address {
        Address([byte; 20])
    }

    fn signed_transaction_from(
        secret_key: &SecretKey,
        public_key: PublicKey,
        to: Address,
        amount: u64,
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
        secret_key: &SecretKey,
        public_key: PublicKey,
        to: Address,
        amount: u64,
        fee: u64,
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

    fn signed_transaction_from_with_fee_rate_at(
        secret_key: &SecretKey,
        public_key: PublicKey,
        to: Address,
        amount: u64,
        fee_rate: u64,
        nonce: u64,
        timestamp: u64,
    ) -> SignedTransaction {
        let template = signed_transaction_from_with_fee_at(
            secret_key, public_key, to, amount, 0, nonce, timestamp,
        );
        let fee = required_fee_for_rate(fee_rate, template.virtual_size());
        signed_transaction_from_with_fee_at(
            secret_key, public_key, to, amount, fee, nonce, timestamp,
        )
    }

    #[test]
    fn prunes_low_fee_transactions_after_low_fee_expiry() {
        let keypair = generate_keypair();
        let transaction = signed_transaction_from_with_fee_rate_at(
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
        let transaction = signed_transaction_from_with_fee_rate_at(
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
    fn dynamic_market_fee_rate_increases_with_mempool_pressure() {
        let first_keypair = generate_keypair();
        let second_keypair = generate_keypair();
        let first = signed_transaction_from_with_fee_rate_at(
            &first_keypair.secret_key,
            first_keypair.public_key,
            address(2),
            10,
            2,
            0,
            1_000,
        );
        let second = signed_transaction_from_with_fee_rate_at(
            &second_keypair.secret_key,
            second_keypair.public_key,
            address(3),
            10,
            2,
            0,
            1_001,
        );
        let second_hash = second.hash();
        let mut mempool = Mempool::with_config(MempoolConfig {
            max_transactions: 2,
            min_relay_fee: 1,
            market_fee: 2,
            low_fee_ttl_secs: crate::runtime::params::LOW_FEE_EXPIRY_SECS,
            transaction_ttl_secs: crate::runtime::params::MEMPOOL_EXPIRY_SECS,
            ..MempoolConfig::default()
        });

        assert_eq!(mempool.mempool_pressure_bps(), 0);
        assert_eq!(mempool.dynamic_market_fee_rate(), 2);

        mempool.insert_at(first, 1_000).unwrap();

        assert_eq!(mempool.mempool_pressure_bps(), 5_000);
        assert_eq!(mempool.dynamic_market_fee_rate(), 10);

        mempool.insert_at(second, 1_001).unwrap();

        assert_eq!(
            mempool.prune_expired(1_001 + crate::runtime::params::LOW_FEE_EXPIRY_SECS + 1),
            1
        );
        assert!(!mempool.contains(&second_hash));
    }

    #[test]
    fn sender_nonce_index_tracks_replacement_prune_and_clear() {
        let keypair = generate_keypair();
        let sender = address_from_public_key(&keypair.public_key);
        let mut ledger = Ledger::new();
        ledger.create_account(sender, Amount(100)).unwrap();

        let mut mempool = Mempool::with_config(MempoolConfig {
            min_relay_fee: 1,
            market_fee: 5,
            low_fee_ttl_secs: crate::runtime::params::LOW_FEE_EXPIRY_SECS,
            transaction_ttl_secs: crate::runtime::params::MEMPOOL_EXPIRY_SECS,
            ..MempoolConfig::default()
        });
        let original = signed_transaction_from_with_fee_rate_at(
            &keypair.secret_key,
            keypair.public_key,
            address(2),
            10,
            1,
            0,
            1_000,
        );
        let original_hash = mempool
            .insert_validated_at(&ledger, original, 1_000)
            .unwrap();
        assert_eq!(
            mempool.by_sender_nonce.get(&(sender, Nonce(0))),
            Some(&original_hash)
        );
        assert_eq!(
            mempool.head_by_sender.get(&sender).map(|(hash, _)| *hash),
            Some(original_hash)
        );
        assert!(mempool.expires_by_hash.contains_key(&original_hash));
        assert_eq!(mempool.expiries.len(), 1);
        assert_eq!(mempool.transactions_for_address(&sender).count(), 1);

        let replacement = signed_transaction_from_with_fee_rate_at(
            &keypair.secret_key,
            keypair.public_key,
            address(2),
            10,
            2,
            0,
            1_001,
        );
        let replacement_hash = mempool
            .insert_validated_at(&ledger, replacement, 1_001)
            .unwrap();
        assert!(!mempool.contains(&original_hash));
        assert_eq!(
            mempool.by_sender_nonce.get(&(sender, Nonce(0))),
            Some(&replacement_hash)
        );
        assert_eq!(
            mempool.head_by_sender.get(&sender).map(|(hash, _)| *hash),
            Some(replacement_hash)
        );
        assert_eq!(mempool.heads_by_fee.len(), 1);
        assert!(!mempool.expires_by_hash.contains_key(&original_hash));
        assert!(mempool.expires_by_hash.contains_key(&replacement_hash));
        assert_eq!(mempool.expiries.len(), 1);
        assert_eq!(mempool.transactions_for_address(&sender).count(), 1);

        assert_eq!(
            mempool.prune_expired(1_001 + crate::runtime::params::LOW_FEE_EXPIRY_SECS + 1),
            1
        );
        assert!(mempool.by_sender_nonce.is_empty());
        assert!(mempool.head_by_sender.is_empty());
        assert!(mempool.heads_by_fee.is_empty());
        assert!(mempool.expires_by_hash.is_empty());
        assert!(mempool.expiries.is_empty());
        assert_eq!(mempool.transactions_for_address(&sender).count(), 0);

        let fresh = signed_transaction_from_with_fee_rate_at(
            &keypair.secret_key,
            keypair.public_key,
            address(2),
            10,
            5,
            0,
            2_000,
        );
        mempool.insert_validated_at(&ledger, fresh, 2_000).unwrap();
        assert_eq!(mempool.by_sender_nonce.len(), 1);
        assert_eq!(mempool.head_by_sender.len(), 1);
        assert_eq!(mempool.heads_by_fee.len(), 1);
        assert_eq!(mempool.expires_by_hash.len(), 1);
        assert_eq!(mempool.expiries.len(), 1);
        assert_eq!(mempool.transactions_for_address(&sender).count(), 1);
        mempool.clear();
        assert!(mempool.by_sender_nonce.is_empty());
        assert!(mempool.head_by_sender.is_empty());
        assert!(mempool.heads_by_fee.is_empty());
        assert!(mempool.expires_by_hash.is_empty());
        assert!(mempool.expiries.is_empty());
        assert_eq!(mempool.transactions_for_address(&sender).count(), 0);
    }

    #[test]
    fn selects_transactions_by_fee_without_breaking_sender_nonce_order() {
        let first_keypair = generate_keypair();
        let second_keypair = generate_keypair();
        let first_sender = address_from_public_key(&first_keypair.public_key);
        let second_sender = address_from_public_key(&second_keypair.public_key);
        let receiver = address(2);
        let mut mempool = Mempool::new();
        let now = current_unix_timestamp();
        let first_slow = signed_transaction_from_with_fee_rate_at(
            &first_keypair.secret_key,
            first_keypair.public_key,
            receiver,
            10,
            2,
            0,
            now,
        );
        let first_aggressive = signed_transaction_from_with_fee_rate_at(
            &first_keypair.secret_key,
            first_keypair.public_key,
            receiver,
            10,
            9,
            1,
            now,
        );
        let second_fast = signed_transaction_from_with_fee_rate_at(
            &second_keypair.secret_key,
            second_keypair.public_key,
            receiver,
            10,
            5,
            0,
            now,
        );
        let second_fast_fee = second_fast.transaction.fee;
        let first_aggressive_fee = first_aggressive.transaction.fee;

        mempool.insert(first_aggressive).unwrap();
        mempool.insert(first_slow).unwrap();
        mempool.insert(second_fast).unwrap();

        let selected = mempool.select_for_block_with_min_fee_rate(3, 0);

        assert_eq!(selected[0].transaction.from, second_sender);
        assert_eq!(selected[0].transaction.fee, second_fast_fee);
        assert_eq!(selected[1].transaction.from, first_sender);
        assert_eq!(selected[1].transaction.nonce, Nonce(0));
        assert_eq!(selected[2].transaction.from, first_sender);
        assert_eq!(selected[2].transaction.nonce, Nonce(1));
        assert_eq!(selected[2].transaction.fee, first_aggressive_fee);
    }

    #[test]
    fn candidate_selection_respects_block_height_validity_window() {
        let keypair = generate_keypair();
        let sender = address_from_public_key(&keypair.public_key);
        let payload = Transaction::new_at(
            sender,
            address(2),
            Amount(1),
            Amount(crate::runtime::params::DEFAULT_TRANSACTION_FEE),
            Nonce(0),
            current_unix_timestamp(),
        )
        .with_validity_window(ValidityWindow::new(Height(5), Height(7)).unwrap());
        let signature = sign(&keypair.secret_key, &payload.signing_bytes());
        let signed = SignedTransaction::new(payload, keypair.public_key, signature);
        let mut mempool = Mempool::new();
        mempool.insert(signed.clone()).unwrap();

        assert!(
            mempool
                .select_for_block_at_height(1, 0, Height(4))
                .is_empty()
        );
        assert_eq!(
            mempool.select_for_block_at_height(1, 0, Height(5)),
            vec![signed.clone()]
        );
        assert_eq!(
            mempool.select_for_block_at_height(1, 0, Height(7)),
            vec![signed]
        );
        assert!(
            mempool
                .select_for_block_at_height(1, 0, Height(8))
                .is_empty()
        );
    }

    #[test]
    fn miner_min_fee_rate_filters_candidate_block_transactions() {
        let low_keypair = generate_keypair();
        let high_keypair = generate_keypair();
        let low_sender = address_from_public_key(&low_keypair.public_key);
        let high_sender = address_from_public_key(&high_keypair.public_key);
        let to = address(2);
        let miner = address(9);
        let mut ledger = Ledger::new();
        ledger.create_account(low_sender, Amount(100)).unwrap();
        ledger.create_account(high_sender, Amount(100)).unwrap();
        ledger.create_account(to, Amount(0)).unwrap();
        ledger.create_account(miner, Amount(0)).unwrap();
        ledger
            .apply_block(Block::new(
                Height(0),
                Hash([0; HASH_SIZE]),
                miner,
                1_700_000_000,
                Nonce(0),
                vec![],
            ))
            .unwrap();

        let mut mempool = Mempool::new();
        let now = current_unix_timestamp();
        let low_fee = signed_transaction_from_with_fee_rate_at(
            &low_keypair.secret_key,
            low_keypair.public_key,
            to,
            1,
            2,
            0,
            now,
        );
        let high_fee = signed_transaction_from_with_fee_rate_at(
            &high_keypair.secret_key,
            high_keypair.public_key,
            to,
            1,
            10,
            0,
            now,
        );
        mempool.insert_validated(&ledger, low_fee).unwrap();
        mempool.insert_validated(&ledger, high_fee).unwrap();

        let block = mempool
            .create_candidate_block_with_min_fee_rate(
                &ledger,
                miner,
                1_700_000_001,
                Nonce(0),
                10,
                10,
            )
            .unwrap();

        assert_eq!(block.transaction_count(), 1);
        assert_eq!(block.transactions[0].transaction.from, high_sender);
    }
}
