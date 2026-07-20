use paqus::block::Block;
use paqus::block::BlockHeight;
use paqus::crypto::{Address, BlockHash, PublicKey};
use paqus::crypto::{CachedVerifyingKey, cached_verifying_key, try_address_from_public_key};
use paqus::ledger::Ledger;
use paqus::state::Account;
use paqus::transaction::{SignedTransaction, TransactionError};
use std::collections::BTreeMap;

#[derive(Clone, Debug, Default)]
pub struct CoreCache {
    accounts: BTreeMap<Address, Account>,
    blocks_by_height: BTreeMap<BlockHeight, Block>,
    blocks_by_hash: BTreeMap<BlockHash, Block>,
    addresses_by_public_key: BTreeMap<PublicKey, Address>,
    verifying_keys_by_public_key: BTreeMap<PublicKey, CachedVerifyingKey>,
    tip_height: Option<BlockHeight>,
    tip_hash: Option<BlockHash>,
}

impl CoreCache {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_ledger(ledger: &Ledger) -> Self {
        let mut cache = Self::new();

        for account in ledger.accounts().values() {
            cache.insert_account(account.clone());
        }

        for block in ledger.chain.blocks.values() {
            cache.insert_block(block.clone());
        }

        cache.tip_height = ledger.tip_height();
        cache.tip_hash = ledger.tip_hash();
        cache
    }

    pub fn insert_account(&mut self, account: Account) {
        self.accounts.insert(account.address, account);
    }

    pub fn account(&self, address: &Address) -> Option<&Account> {
        self.accounts.get(address)
    }

    pub fn address_for_public_key(
        &mut self,
        public_key: &PublicKey,
    ) -> Result<Address, TransactionError> {
        if let Some(address) = self.addresses_by_public_key.get(public_key) {
            return Ok(*address);
        }

        let address = try_address_from_public_key(public_key)
            .map_err(|_| TransactionError::EmptyPublicKey)?;
        self.addresses_by_public_key.insert(*public_key, address);
        Ok(address)
    }

    pub fn verifying_key_for_public_key(&mut self, public_key: &PublicKey) -> &CachedVerifyingKey {
        self.verifying_keys_by_public_key
            .entry(*public_key)
            .or_insert_with(|| cached_verifying_key(public_key))
    }

    pub fn validate_signed_transaction(
        &mut self,
        transaction: &SignedTransaction,
    ) -> Result<(), TransactionError> {
        transaction.validate()?;
        if self.address_for_public_key(&transaction.witness.public_key)?
            != transaction.transaction.from
        {
            return Err(TransactionError::SenderAddressMismatch);
        }
        self.verify_transaction_signature(transaction)
    }

    pub fn validate_signed_transaction_at(
        &mut self,
        transaction: &SignedTransaction,
        now: u64,
    ) -> Result<(), TransactionError> {
        transaction.validate_at(now)?;
        if self.address_for_public_key(&transaction.witness.public_key)?
            != transaction.transaction.from
        {
            return Err(TransactionError::SenderAddressMismatch);
        }
        self.verify_transaction_signature(transaction)
    }

    fn verify_transaction_signature(
        &mut self,
        transaction: &SignedTransaction,
    ) -> Result<(), TransactionError> {
        let payload_bytes = transaction.transaction.signing_bytes();
        self.verifying_key_for_public_key(&transaction.witness.public_key)
            .verify(&payload_bytes, &transaction.witness.signature)
            .map_err(|_| TransactionError::InvalidSignature)
    }

    pub fn insert_block(&mut self, block: Block) {
        let height = block.height();
        let hash = block.hash();

        self.blocks_by_height.insert(height, block.clone());
        self.blocks_by_hash.insert(hash, block);
        self.tip_height = Some(height);
        self.tip_hash = Some(hash);
    }

    pub fn block_by_height(&self, height: &BlockHeight) -> Option<&Block> {
        self.blocks_by_height.get(height)
    }

    pub fn block_by_hash(&self, hash: &BlockHash) -> Option<&Block> {
        self.blocks_by_hash.get(hash)
    }

    pub fn tip_height(&self) -> Option<BlockHeight> {
        self.tip_height
    }

    pub fn tip_hash(&self) -> Option<BlockHash> {
        self.tip_hash
    }
}

#[cfg(test)]
mod test {
    use super::CoreCache;
    use paqus::block::{Block, Height, Nonce};
    use paqus::consensus::supply::Amount;
    use paqus::crypto::{Address, HASH_SIZE, Hash, address_from_public_key, generate_keypair};
    use paqus::ledger::Ledger;
    use paqus::state::Account;

    fn address(byte: u8) -> Address {
        Address([byte; 20])
    }

    #[test]
    fn caches_accounts_and_blocks() {
        let mut cache = CoreCache::new();
        let account = Account::new(address(1), Amount(100));
        let block = Block::new(
            Height(0),
            Hash([0; HASH_SIZE]),
            address(9),
            1_700_000_000,
            Nonce(0),
            vec![],
        );
        let block_hash = block.hash();

        cache.insert_account(account.clone());
        cache.insert_block(block.clone());

        assert_eq!(cache.account(&address(1)), Some(&account));
        assert_eq!(cache.block_by_height(&Height(0)), Some(&block));
        assert_eq!(cache.block_by_hash(&block_hash), Some(&block));
        assert_eq!(cache.tip_height(), Some(Height(0)));
        assert_eq!(cache.tip_hash(), Some(block_hash));
    }

    #[test]
    fn caches_addresses_by_public_key() {
        let keypair = generate_keypair();
        let mut cache = CoreCache::new();
        let expected = address_from_public_key(&keypair.public_key);

        assert_eq!(
            cache.address_for_public_key(&keypair.public_key),
            Ok(expected)
        );
        assert_eq!(
            cache.addresses_by_public_key.get(&keypair.public_key),
            Some(&expected)
        );
        assert_eq!(
            cache.address_for_public_key(&keypair.public_key),
            Ok(expected)
        );
    }

    #[test]
    fn caches_verifying_keys_by_public_key() {
        let keypair = generate_keypair();
        let mut cache = CoreCache::new();

        cache.verifying_key_for_public_key(&keypair.public_key);
        assert!(
            cache
                .verifying_keys_by_public_key
                .contains_key(&keypair.public_key)
        );
        cache.verifying_key_for_public_key(&keypair.public_key);
        assert_eq!(cache.verifying_keys_by_public_key.len(), 1);
    }

    #[test]
    fn builds_from_ledger_state() {
        let mut ledger = Ledger::new();
        let block = Block::new(
            Height(0),
            Hash([0; HASH_SIZE]),
            address(9),
            1_700_000_000,
            Nonce(0),
            vec![],
        );
        let block_hash = block.hash();

        ledger.create_account(address(1), Amount(100)).unwrap();
        ledger.chain.insert_block(block).unwrap();

        let cache = CoreCache::from_ledger(&ledger);

        assert_eq!(cache.account(&address(1)).unwrap().balance, Amount(100));
        assert_eq!(cache.tip_height(), Some(Height(0)));
        assert_eq!(cache.tip_hash(), Some(block_hash));
    }
}
