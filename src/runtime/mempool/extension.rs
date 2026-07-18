use super::MempoolError;
use crate::runtime::params::MAX_MEMPOOL_TXS;
use paqus::block::{Block, BlockHeight, MAX_BLOCK_SIZE, MAX_BLOCK_WEIGHT};
use paqus::crypto::{Address, TransactionHash};
use paqus::ledger::Ledger;
use paqus::state::CashCoinId;
use paqus::transaction::{EcashTransactionKind, SignedProtocolTransaction, TransactionFamily};
use std::collections::{BTreeMap, BTreeSet};

/// Pool for every non-transfer SegWit transaction family.
///
/// One pending extension transaction per signer keeps account nonce ordering
/// deterministic across the block's family-ordered execution lanes.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ExtensionMempool {
    transactions: BTreeMap<TransactionHash, SignedProtocolTransaction>,
    by_signer: BTreeMap<Address, TransactionHash>,
    reserved_coins: BTreeMap<CashCoinId, TransactionHash>,
}

impl ExtensionMempool {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert_validated(
        &mut self,
        ledger: &Ledger,
        transaction: SignedProtocolTransaction,
    ) -> Result<TransactionHash, MempoolError> {
        if transaction.family() == TransactionFamily::Transfer {
            return Err(MempoolError::UnsupportedFamily);
        }
        if self.transactions.len() >= MAX_MEMPOOL_TXS {
            return Err(MempoolError::MempoolFull);
        }
        let hash = transaction.hash();
        let signer = transaction.signer();
        if self.transactions.contains_key(&hash) || self.by_signer.contains_key(&signer) {
            return Err(MempoolError::DuplicateTransaction);
        }
        let coin_ids = deposit_coin_ids(&transaction);
        if coin_ids
            .iter()
            .any(|coin_id| self.reserved_coins.contains_key(coin_id))
        {
            return Err(MempoolError::CashCoinReserved);
        }

        let height = ledger
            .tip_height()
            .map(|height| paqus::block::Height(height.0.saturating_add(1)))
            .unwrap_or(paqus::block::Height(0));
        apply_extension(ledger, &transaction, height)?;

        for coin_id in coin_ids {
            self.reserved_coins.insert(coin_id, hash);
        }
        self.by_signer.insert(signer, hash);
        self.transactions.insert(hash, transaction);
        Ok(hash)
    }

    pub fn contains(&self, hash: &TransactionHash) -> bool {
        self.transactions.contains_key(hash)
    }

    pub fn contains_signer(&self, signer: Address) -> bool {
        self.by_signer.contains_key(&signer)
    }

    pub fn get(&self, hash: &TransactionHash) -> Option<&SignedProtocolTransaction> {
        self.transactions.get(hash)
    }

    pub fn transactions(&self) -> impl Iterator<Item = &SignedProtocolTransaction> {
        self.transactions.values()
    }

    pub fn transactions_for_family(
        &self,
        family: TransactionFamily,
    ) -> impl Iterator<Item = &SignedProtocolTransaction> {
        self.transactions
            .values()
            .filter(move |transaction| transaction.family() == family)
    }

    pub fn len(&self) -> usize {
        self.transactions.len()
    }

    pub fn is_empty(&self) -> bool {
        self.transactions.is_empty()
    }

    pub fn remove(&mut self, hash: &TransactionHash) -> Option<SignedProtocolTransaction> {
        let transaction = self.transactions.remove(hash)?;
        self.by_signer.remove(&transaction.signer());
        for coin_id in deposit_coin_ids(&transaction) {
            if self.reserved_coins.get(&coin_id) == Some(hash) {
                self.reserved_coins.remove(&coin_id);
            }
        }
        Some(transaction)
    }

    pub fn select_for_block(
        &self,
        height: BlockHeight,
        _block_timestamp: u64,
        limit: usize,
        min_fee_rate: u64,
    ) -> Vec<SignedProtocolTransaction> {
        let mut candidates = self
            .transactions
            .values()
            .filter(|transaction| {
                transaction.validity().validate_at(height).is_ok()
                    && fee_rate(transaction.fee().0, transaction.virtual_size()) >= min_fee_rate
            })
            .cloned()
            .collect::<Vec<_>>();
        candidates.sort_by_key(|transaction| {
            (
                std::cmp::Reverse(fee_rate(transaction.fee().0, transaction.virtual_size())),
                transaction.hash(),
            )
        });
        candidates.truncate(limit);
        candidates
    }

    pub fn append_selected_to_block(
        &self,
        block: &mut Block,
        transaction_limit: usize,
        min_fee_rate: u64,
    ) {
        let remaining = transaction_limit.saturating_sub(block.transaction_count());
        for transaction in
            self.select_for_block(block.height(), block.timestamp(), remaining, min_fee_rate)
        {
            let family = transaction.family();
            match transaction {
                SignedProtocolTransaction::Transfer(_) => unreachable!("transfer uses main pool"),
                SignedProtocolTransaction::Ecash(tx) => block.ecash_transactions.push(tx),
            }
            refresh_block_fees_and_commitments(block);
            if block.serialized_size() > MAX_BLOCK_SIZE || block.weight() > MAX_BLOCK_WEIGHT {
                pop_last_family_transaction(block, family);
                refresh_block_fees_and_commitments(block);
            }
        }
        refresh_block_fees_and_commitments(block);
    }

    pub fn remove_confirmed(&mut self, block: &Block) {
        let hashes = block
            .ecash_transactions
            .iter()
            .map(|tx| tx.hash())
            .collect::<BTreeSet<_>>();
        for hash in hashes {
            self.remove(&hash);
        }
    }
}

fn fee_rate(fee: u64, virtual_size: usize) -> u64 {
    if virtual_size == 0 {
        return u64::MAX;
    }
    fee.saturating_mul(crate::runtime::params::FEE_RATE_UNIT_BYTES as u64) / virtual_size as u64
}

fn refresh_block_fees_and_commitments(block: &mut Block) {
    if let Ok(fees) = block.checked_total_fees()
        && let Some(coinbase) = &mut block.coinbase
    {
        coinbase.fees = fees;
    }
    block.refresh_commitments();
}

fn pop_last_family_transaction(block: &mut Block, family: TransactionFamily) {
    match family {
        TransactionFamily::Transfer => unreachable!("transfer uses main pool"),
        TransactionFamily::Ecash => {
            block.ecash_transactions.pop();
        }
    }
}

fn apply_extension(
    ledger: &Ledger,
    transaction: &SignedProtocolTransaction,
    height: BlockHeight,
) -> Result<(), MempoolError> {
    let mut staged = ledger.clone();
    match transaction {
        SignedProtocolTransaction::Transfer(_) => return Err(MempoolError::UnsupportedFamily),
        SignedProtocolTransaction::Ecash(tx) => {
            staged.apply_signed_ecash_transaction(tx, height)?;
        }
    }
    Ok(())
}

fn deposit_coin_ids(transaction: &SignedProtocolTransaction) -> Vec<CashCoinId> {
    match transaction {
        SignedProtocolTransaction::Ecash(tx) => match &tx.transaction.kind {
            EcashTransactionKind::DepositCash { metadata, .. } => metadata
                .inputs
                .iter()
                .map(|input| CashCoinId(input.coin_id))
                .collect(),
            EcashTransactionKind::WithdrawCash { .. } => Vec::new(),
        },
        _ => Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use paqus::block::{Height, Nonce};
    use paqus::consensus::supply::{Amount, XPQ};
    use paqus::crypto::{
        Address, TransactionHash, address_from_public_key, generate_keypair, sign,
    };
    use paqus::ecash::{
        CashCoinFile, CashDenomination, DepositCashMetadata, WithdrawCashMetadata,
        cash_coin_commitment,
    };
    use paqus::transaction::{EcashTransaction, SignedEcashTransaction};

    fn signed_deposit(
        keypair: &paqus::crypto::KeyPair,
        metadata: DepositCashMetadata,
    ) -> SignedProtocolTransaction {
        let signer = address_from_public_key(&keypair.public_key);
        let transaction = EcashTransaction::deposit(signer, signer, Amount(0), Nonce(0), metadata);
        let signature = sign(&keypair.secret_key, &transaction.signing_bytes());
        SignedEcashTransaction::new(transaction, keypair.public_key, signature).into()
    }

    #[test]
    fn reserves_ecash_coin_across_the_unified_extension_pool() {
        let secret = [11; 32];
        let withdraw = WithdrawCashMetadata::with_denominations(
            Amount(XPQ),
            &[CashDenomination::One],
            &[cash_coin_commitment(&secret)],
        )
        .unwrap();
        let withdraw_hash = TransactionHash([12; 32]);
        let mut ledger = Ledger::new();
        ledger
            .offchain_coins
            .apply_withdraw(Address([13; 20]), withdraw_hash, &withdraw, Height(0))
            .unwrap();
        ledger.finalize_ecash_at(Height(100));
        ledger.chain.tip_height = Some(Height(0));
        let file = CashCoinFile::new(withdraw_hash, &withdraw.outputs[0], secret).unwrap();
        let first_keypair = generate_keypair();
        let second_keypair = generate_keypair();
        let first_signer = address_from_public_key(&first_keypair.public_key);
        let second_signer = address_from_public_key(&second_keypair.public_key);
        ledger.create_account(first_signer, Amount(0)).unwrap();
        ledger.create_account(second_signer, Amount(0)).unwrap();
        let first = signed_deposit(
            &first_keypair,
            DepositCashMetadata::new(&[file], first_signer).unwrap(),
        );
        let second = signed_deposit(
            &second_keypair,
            DepositCashMetadata::new(&[file], second_signer).unwrap(),
        );
        let mut pool = ExtensionMempool::new();
        let first_hash = pool.insert_validated(&ledger, first).unwrap();
        assert_eq!(
            pool.insert_validated(&ledger, second.clone()),
            Err(MempoolError::CashCoinReserved)
        );
        pool.remove(&first_hash).unwrap();
        assert!(pool.insert_validated(&ledger, second).is_ok());
    }
}
