use crate::runtime::cache::CoreCache;
use crate::runtime::mempool::Mempool;
use crate::runtime::miner::{MiningConfig, MiningResult, mine_candidate_block};
use crate::runtime::node::error::NodeError;
use crate::runtime::params::{DIFFICULTY_ADJUSTMENT_INTERVAL, HASH_SIZE};
use crate::runtime::storage::Storage;
use paqus::block::{Block, BlockHeight, Height};
use paqus::consensus::Consensus;
use paqus::consensus::supply::{Amount, Balance};
use paqus::crypto::{Address, BlockHash, Hash, TransactionHash};
use paqus::genesis::{GENESIS_HASH, genesis_block};
use paqus::ledger::fork_choice::ForkChoice;
use paqus::ledger::{Chain, Ledger};
use paqus::transaction::{AccountNonce, SignedTransaction};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

const MAX_ORPHAN_BLOCKS: usize = 1024;
const MAX_ORPHAN_HEIGHT_DISTANCE: u64 = 512;
const ORPHAN_BLOCK_TTL_SECS: u64 = 10 * 60;
const MISSING_PARENT_RETRY_SECS: u64 = 5;

#[derive(Clone, Debug)]
struct OrphanBlock {
    block: Block,
    received_at: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PendingBalance {
    pub incoming: Amount,
    pub outgoing: Amount,
}

impl Default for PendingBalance {
    fn default() -> Self {
        Self {
            incoming: Amount(0),
            outgoing: Amount(0),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BalanceSummary {
    pub confirmed: Amount,
    pub available: Amount,
    pub pending: PendingBalance,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AccountView {
    pub balance: Amount,
    pub unspendable: Amount,
    pub nonce: AccountNonce,
}

impl Default for BalanceSummary {
    fn default() -> Self {
        Self {
            confirmed: Amount(0),
            available: Amount(0),
            pending: PendingBalance::default(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct Node {
    pub ledger: Ledger,
    pub mempool: Mempool,
    pub storage: Storage,
    pub consensus: Consensus,
    pub cache: CoreCache,
    pub fork_choice: ForkChoice,
    genesis_accounts: BTreeMap<Address, paqus::state::Account>,
    orphan_blocks: BTreeMap<BlockHash, OrphanBlock>,
    orphan_children_by_parent: BTreeMap<BlockHash, Vec<BlockHash>>,
    missing_parent_requests: VecDeque<BlockHash>,
    missing_parent_request_set: BTreeSet<BlockHash>,
    missing_parent_retry_at: BTreeMap<BlockHash, u64>,
}

impl Node {
    pub fn new(ledger: Ledger, storage: Storage, consensus: Consensus) -> Self {
        let genesis_accounts = if ledger.tip_height() == Some(Height(0)) {
            ledger.accounts.clone()
        } else {
            BTreeMap::new()
        };
        Self::with_genesis_accounts(ledger, storage, consensus, genesis_accounts)
    }

    pub fn with_genesis_accounts(
        ledger: Ledger,
        storage: Storage,
        consensus: Consensus,
        genesis_accounts: BTreeMap<Address, paqus::state::Account>,
    ) -> Self {
        let cache = CoreCache::from_ledger(&ledger);
        let mut fork_choice = ForkChoice::new();
        for block in ledger.chain.blocks.values() {
            fork_choice
                .insert_block(block.clone())
                .expect("ledger chain should build a valid fork choice index");
        }
        Self {
            ledger,
            mempool: Mempool::new(),
            storage,
            consensus,
            cache,
            fork_choice,
            genesis_accounts,
            orphan_blocks: BTreeMap::new(),
            orphan_children_by_parent: BTreeMap::new(),
            missing_parent_requests: VecDeque::new(),
            missing_parent_request_set: BTreeSet::new(),
            missing_parent_retry_at: BTreeMap::new(),
        }
    }

    pub fn temporary(ledger: Ledger, consensus: Consensus) -> Result<Self, NodeError> {
        Ok(Self::new(ledger, Storage::temporary()?, consensus))
    }

    pub fn init_or_load(path: impl AsRef<Path>, consensus: Consensus) -> Result<Self, NodeError> {
        let storage = Storage::open(path)?;
        let ledger = if storage.load_tip()?.is_some() {
            storage.load_ledger()?
        } else {
            let genesis = genesis_block();
            assert_eq!(genesis.hash().0, GENESIS_HASH);
            let mut ledger = Ledger::new();
            ledger.apply_block(genesis)?;
            storage.save_ledger(&ledger)?;
            storage.save_genesis_accounts(&ledger.accounts)?;
            ledger
        };

        let genesis_accounts = match storage.load_genesis_accounts()? {
            Some(accounts) => accounts,
            None => {
                let accounts = if let Some(snapshot) = storage.load_state_snapshot(Height(0))? {
                    snapshot.accounts
                } else if ledger.tip_height() == Some(Height(0)) {
                    ledger.accounts.clone()
                } else {
                    let genesis = storage
                        .load_block_by_height(Height(0))?
                        .ok_or(NodeError::MissingGenesisState)?;
                    let mut genesis_ledger = Ledger::new();
                    genesis_ledger.apply_block(genesis)?;
                    genesis_ledger.accounts
                };
                storage.save_genesis_accounts(&accounts)?;
                accounts
            }
        };
        let mut node = Self::with_genesis_accounts(ledger, storage, consensus, genesis_accounts);
        node.index_stored_blocks()?;
        Ok(node)
    }

    pub fn submit_transaction(
        &mut self,
        transaction: SignedTransaction,
    ) -> Result<TransactionHash, NodeError> {
        Ok(self.mempool.insert_validated_at_with_cache(
            &self.ledger,
            transaction,
            current_unix_timestamp(),
            &mut self.cache,
        )?)
    }

    pub fn apply_block(&mut self, block: Block) -> Result<(), NodeError> {
        self.prune_expired_orphans(current_unix_timestamp());
        match self.apply_known_parent_block(block.clone()) {
            Ok(()) => {
                self.process_orphans_for_parent(block.hash());
                Ok(())
            }
            Err(NodeError::ForkChoice(
                paqus::ledger::fork_choice::ForkChoiceError::MissingParent,
            )) => {
                self.cache_orphan_block(block);
                Ok(())
            }
            Err(NodeError::ForkChoice(
                paqus::ledger::fork_choice::ForkChoiceError::DuplicateBlock,
            )) => Ok(()),
            Err(NodeError::Ledger(paqus::ledger::LedgerError::DuplicateBlock)) => Ok(()),
            Err(error) => Err(error),
        }
    }

    fn apply_known_parent_block(&mut self, block: Block) -> Result<(), NodeError> {
        self.validate_block_for_known_parent(&block)?;
        let active_staged = self.validate_block_state_for_known_parent(&block)?;
        let block_hash = self.fork_choice.insert_block(block.clone())?;
        let best_tip_hash = self.fork_choice.best_tip().map(|node| node.hash);

        if best_tip_hash != Some(block_hash) {
            self.storage.save_side_block(&block)?;
            return Ok(());
        }

        let extends_active_tip = match self.ledger.tip_hash() {
            Some(tip_hash) => block.previous_hash() == tip_hash,
            None => block.height().0 == 0,
        };
        if !extends_active_tip {
            return self.reorg_to_best_tip();
        }

        self.ledger = active_staged.unwrap_or_else(|| {
            let mut ledger = self.ledger.clone();
            ledger
                .apply_block(block.clone())
                .expect("validated active block should apply");
            ledger
        });
        self.mempool.remove_confirmed(&block);
        self.cache.insert_block(block.clone());
        for transaction in &block.transactions {
            if let Some(sender) = self.ledger.account(&transaction.transaction.from) {
                self.cache.insert_account(sender.clone());
            }
            if let Some(receiver) = self.ledger.account(&transaction.transaction.to) {
                self.cache.insert_account(receiver.clone());
            }
        }
        if let Some(miner) = self.ledger.account(&block.miner_address()) {
            self.cache.insert_account(miner.clone());
        }
        self.storage.save_ledger(&self.ledger)?;
        Ok(())
    }

    fn cache_orphan_block(&mut self, block: Block) {
        let now = current_unix_timestamp();
        self.prune_expired_orphans(now);
        if block.height().0 == 0 {
            return;
        }
        if self.orphan_is_too_far_ahead(&block) {
            return;
        }

        let hash = block.hash();
        if self.fork_choice.contains(&hash) || self.orphan_blocks.contains_key(&hash) {
            return;
        }

        if self.orphan_blocks.len() >= MAX_ORPHAN_BLOCKS {
            if let Some(evicted_hash) = self.orphan_blocks.keys().next().copied() {
                self.remove_orphan(evicted_hash);
            }
        }

        let parent = BlockHash::from(block.previous_hash().as_hash());
        self.queue_missing_parent_request(parent);
        self.orphan_children_by_parent
            .entry(parent)
            .or_default()
            .push(hash);
        self.orphan_blocks.insert(
            hash,
            OrphanBlock {
                block,
                received_at: now,
            },
        );
    }

    fn queue_missing_parent_request(&mut self, hash: BlockHash) {
        if self.fork_choice.contains(&hash) {
            return;
        }
        self.queue_missing_parent_request_at(hash, current_unix_timestamp());
    }

    fn queue_missing_parent_request_at(&mut self, hash: BlockHash, retry_at: u64) {
        if self.fork_choice.contains(&hash) {
            return;
        }
        self.missing_parent_retry_at
            .entry(hash)
            .and_modify(|existing| *existing = (*existing).min(retry_at))
            .or_insert(retry_at);
        if self.missing_parent_request_set.insert(hash) {
            self.missing_parent_requests.push_back(hash);
        }
    }

    pub fn drain_missing_parent_requests(&mut self) -> Vec<BlockHash> {
        self.drain_missing_parent_requests_at(current_unix_timestamp())
    }

    fn drain_missing_parent_requests_at(&mut self, now: u64) -> Vec<BlockHash> {
        let mut ready = Vec::new();
        let mut pending = VecDeque::new();
        while let Some(hash) = self.missing_parent_requests.pop_front() {
            let retry_at = self
                .missing_parent_retry_at
                .get(&hash)
                .copied()
                .unwrap_or(0);
            if retry_at <= now {
                self.missing_parent_request_set.remove(&hash);
                self.missing_parent_retry_at.remove(&hash);
                ready.push(hash);
            } else {
                pending.push_back(hash);
            }
        }
        self.missing_parent_requests = pending;
        ready
    }

    pub fn retry_missing_parent_request(&mut self, hash: BlockHash) {
        self.queue_missing_parent_request_at(
            hash,
            current_unix_timestamp().saturating_add(MISSING_PARENT_RETRY_SECS),
        );
    }

    pub fn has_pending_sync_work(&self) -> bool {
        !self.orphan_blocks.is_empty()
            || !self.missing_parent_requests.is_empty()
            || !self.missing_parent_retry_at.is_empty()
    }

    fn orphan_is_too_far_ahead(&self, block: &Block) -> bool {
        let tip_height = self.ledger.tip_height().map(|height| height.0).unwrap_or(0);
        block.height().0 > tip_height.saturating_add(MAX_ORPHAN_HEIGHT_DISTANCE)
    }

    fn remove_orphan(&mut self, hash: BlockHash) {
        self.remove_orphan_index(hash);
        self.orphan_blocks.remove(&hash);
    }

    fn remove_orphan_index(&mut self, hash: BlockHash) {
        let empty_parents: Vec<_> = self
            .orphan_children_by_parent
            .iter_mut()
            .filter_map(|(parent, children)| {
                children.retain(|child| *child != hash);
                children.is_empty().then_some(*parent)
            })
            .collect();
        for parent in empty_parents {
            self.orphan_children_by_parent.remove(&parent);
        }
    }

    fn prune_expired_orphans(&mut self, now: u64) {
        let expired: Vec<_> = self
            .orphan_blocks
            .iter()
            .filter_map(|(hash, orphan)| {
                let expired = now.saturating_sub(orphan.received_at) > ORPHAN_BLOCK_TTL_SECS
                    || self.orphan_is_too_far_ahead(&orphan.block);
                expired.then_some(*hash)
            })
            .collect();
        for hash in expired {
            self.remove_orphan(hash);
        }
    }

    fn process_orphans_for_parent(&mut self, parent_hash: BlockHash) {
        self.prune_expired_orphans(current_unix_timestamp());
        let mut parents = vec![parent_hash];

        while let Some(parent) = parents.pop() {
            let Some(children) = self.orphan_children_by_parent.remove(&parent) else {
                continue;
            };

            for child_hash in children {
                let Some(orphan) = self.orphan_blocks.remove(&child_hash) else {
                    continue;
                };
                let child = orphan.block;

                match self.apply_known_parent_block(child.clone()) {
                    Ok(()) => parents.push(child_hash),
                    Err(NodeError::ForkChoice(
                        paqus::ledger::fork_choice::ForkChoiceError::MissingParent,
                    )) => self.cache_orphan_block(child),
                    Err(_) => {}
                }
            }
        }
    }

    fn validate_block_state_for_known_parent(
        &self,
        block: &Block,
    ) -> Result<Option<Ledger>, NodeError> {
        let extends_active_tip = match self.ledger.tip_hash() {
            Some(tip_hash) => block.previous_hash() == tip_hash,
            None => block.height().0 == 0,
        };

        if extends_active_tip {
            Self::validate_canonical_state_root(&self.ledger, block)?;
            let (ledger, _) = self.ledger.execute_block(block)?;
            return Ok(Some(ledger));
        }

        let parent_hash = BlockHash::from(block.previous_hash().as_hash());
        let ledger = self.ledger_for_branch_tip(parent_hash)?;
        Self::validate_canonical_state_root(&ledger, block)?;
        Ok(None)
    }

    fn validate_canonical_state_root(ledger: &Ledger, block: &Block) -> Result<(), NodeError> {
        let expected_state_root = ledger.state_root_after_block(block)?;
        if block.state_root() != expected_state_root {
            return Err(paqus::ledger::LedgerError::InvalidStateRoot.into());
        }
        if !block.is_genesis() && block.state_root() == Hash([0; HASH_SIZE]) {
            return Err(paqus::ledger::LedgerError::InvalidStateRoot.into());
        }
        Ok(())
    }

    fn reorg_to_best_tip(&mut self) -> Result<(), NodeError> {
        let old_blocks: Vec<_> = self.ledger.chain.blocks.values().cloned().collect();
        let old_tip_hash = self.ledger.tip_hash();
        let best_tip = self
            .fork_choice
            .best_tip()
            .ok_or(NodeError::ReorgRequired)?
            .hash;
        let ancestor = self
            .common_ancestor(old_tip_hash, best_tip)
            .ok_or(NodeError::ReorgRequired)?;
        let winning_branch = self
            .fork_choice
            .branch_from_ancestor(ancestor, best_tip)
            .ok_or(NodeError::ReorgRequired)?;

        self.ledger = self.ledger_for_branch_tip(best_tip)?;
        self.cache = CoreCache::from_ledger(&self.ledger);
        self.storage.save_ledger(&self.ledger)?;

        let winning_hashes: std::collections::BTreeSet<_> = self
            .fork_choice
            .ancestor_hashes(best_tip)
            .into_iter()
            .collect();
        for old_block in old_blocks {
            if winning_hashes.contains(&old_block.hash()) {
                continue;
            }
            for transaction in old_block.transactions {
                let _ = self.mempool.insert_validated(&self.ledger, transaction);
            }
        }

        // Keep the variable intentionally used for the common ancestor search, even when the
        // winning branch starts at genesis.
        let _ = winning_branch;
        Ok(())
    }

    fn ledger_for_branch_tip(&self, tip: BlockHash) -> Result<Ledger, NodeError> {
        let genesis_hash = self
            .fork_choice
            .ancestor_hashes(tip)
            .last()
            .copied()
            .unwrap_or(tip);
        let genesis = self
            .fork_choice
            .get(&genesis_hash)
            .ok_or(NodeError::ReorgRequired)?
            .block
            .clone();
        let mut ledger = Ledger {
            accounts: self.genesis_accounts.clone(),
            chain: Chain::new(),
        };
        ledger.chain.insert_block(genesis)?;

        let branch = self
            .fork_choice
            .branch_from_ancestor(genesis_hash, tip)
            .ok_or(NodeError::ReorgRequired)?;
        for block in branch {
            ledger.apply_block(block)?;
        }

        Ok(ledger)
    }

    fn index_stored_blocks(&mut self) -> Result<(), NodeError> {
        let mut blocks = self.storage.load_blocks_by_hash()?;
        blocks.sort_by_key(|block| block.height().0);

        let mut progressed = true;
        while progressed {
            progressed = false;
            let mut remaining = Vec::new();
            for block in blocks {
                let hash = block.hash();
                if self.fork_choice.contains(&hash) {
                    progressed = true;
                    continue;
                }
                match self.fork_choice.insert_block(block.clone()) {
                    Ok(_) => progressed = true,
                    Err(paqus::ledger::fork_choice::ForkChoiceError::MissingParent) => {
                        remaining.push(block);
                    }
                    Err(error) => return Err(error.into()),
                }
            }
            blocks = remaining;
        }

        Ok(())
    }

    fn common_ancestor(&self, old_tip: Option<BlockHash>, new_tip: BlockHash) -> Option<BlockHash> {
        let old_tip = old_tip?;
        let old_ancestors: std::collections::BTreeSet<_> = self
            .fork_choice
            .ancestor_hashes(old_tip)
            .into_iter()
            .collect();

        self.fork_choice
            .ancestor_hashes(new_tip)
            .into_iter()
            .find(|hash| old_ancestors.contains(hash))
    }

    fn validate_block_for_known_parent(&self, block: &Block) -> Result<(), NodeError> {
        let now = current_unix_timestamp();
        if block.height().0 == 0 {
            self.consensus.validate_genesis_block_at(block, now)?;
            return Ok(());
        }

        let parent = self
            .fork_choice
            .get(&BlockHash::from(block.previous_hash().as_hash()))
            .ok_or(paqus::ledger::fork_choice::ForkChoiceError::MissingParent)?;
        if self.consensus.config.difficulty != 0 {
            let expected_difficulty = self.next_difficulty_after_branch_tip(parent.hash)?;
            if block.difficulty() != expected_difficulty {
                return Err(paqus::consensus::ConsensusError::UnexpectedDifficulty.into());
            }
        }
        self.consensus
            .validate_next_block_with_tip_at(block, &parent.block, now)?;
        Ok(())
    }

    pub fn mine_block(
        &mut self,
        miner_address: Address,
        timestamp: u64,
        max_attempts: u64,
        transaction_limit: usize,
    ) -> Result<MiningResult, NodeError> {
        self.mempool.prune_expired(timestamp);
        let difficulty = self.next_difficulty()?;
        let result = mine_candidate_block(
            &self.mempool,
            &self.ledger,
            &self.consensus,
            miner_address,
            timestamp,
            MiningConfig {
                difficulty,
                max_attempts,
                transaction_limit,
                min_fee_rate: self.mempool.dynamic_market_fee_rate(),
            },
        )?
        .ok_or(NodeError::MiningExhausted)?;

        self.apply_block(result.block.clone())?;
        Ok(result)
    }

    pub fn next_difficulty(&self) -> Result<u32, NodeError> {
        let Some(tip_height) = self.ledger.tip_height() else {
            return Ok(self.consensus.config.difficulty);
        };

        self.next_difficulty_after_tip(tip_height)
    }

    fn next_difficulty_after_tip(&self, tip_height: BlockHeight) -> Result<u32, NodeError> {
        let Some((first_timestamp, last_timestamp, block_count, current_difficulty)) = self
            .storage
            .difficulty_window(tip_height, DIFFICULTY_ADJUSTMENT_INTERVAL)?
        else {
            return Ok(self.consensus.config.difficulty);
        };

        Ok(self.consensus.retarget_difficulty(
            current_difficulty,
            first_timestamp,
            last_timestamp,
            block_count,
        )?)
    }

    fn next_difficulty_after_branch_tip(&self, tip_hash: BlockHash) -> Result<u32, NodeError> {
        let tip = self
            .fork_choice
            .get(&tip_hash)
            .ok_or(paqus::ledger::fork_choice::ForkChoiceError::MissingParent)?;
        if tip.height.0 < DIFFICULTY_ADJUSTMENT_INTERVAL {
            return Ok(self.consensus.config.difficulty);
        }

        let first_height = Height(tip.height.0 - DIFFICULTY_ADJUSTMENT_INTERVAL);
        let first_hash = self
            .fork_choice
            .ancestor_hashes(tip_hash)
            .into_iter()
            .find(|hash| {
                self.fork_choice
                    .get(hash)
                    .is_some_and(|node| node.height == first_height)
            })
            .ok_or(NodeError::ReorgRequired)?;
        let first = self
            .fork_choice
            .get(&first_hash)
            .ok_or(NodeError::ReorgRequired)?;
        let block_count = tip.height.0.saturating_sub(first.height.0);

        Ok(self.consensus.retarget_difficulty(
            tip.block.difficulty(),
            first.block.timestamp(),
            tip.block.timestamp(),
            block_count,
        )?)
    }

    pub fn flush_to_storage(&self) -> Result<(), NodeError> {
        self.storage.save_ledger(&self.ledger)?;
        Ok(())
    }

    pub fn tip_height(&self) -> Option<BlockHeight> {
        self.ledger.tip_height()
    }

    pub fn tip_hash(&self) -> Option<BlockHash> {
        self.ledger.tip_hash()
    }

    pub fn balance(&self, address: &Address) -> Option<Balance> {
        self.ledger.balance(address)
    }

    pub fn confirmed_balance(&self, address: &Address) -> Option<Balance> {
        self.ledger.confirmed_balance(address)
    }

    pub fn available_balance(&self, address: &Address) -> Option<Balance> {
        self.available_balance_with_depth(
            address,
            crate::runtime::params::CONFIRMATION_DEPTH as u64,
        )
    }

    pub fn available_balance_with_depth(
        &self,
        address: &Address,
        _finality_depth: u64,
    ) -> Option<Balance> {
        let tip_height = self.ledger.tip_height()?;
        self.ledger
            .account(address)
            .map(|account| account.available_balance_at(tip_height))
    }

    pub fn pending_balance(&self, address: &Address) -> PendingBalance {
        let mut pending = PendingBalance::default();
        for transaction in self.mempool.transactions() {
            if transaction.transaction.to == *address {
                pending.incoming.0 = pending
                    .incoming
                    .0
                    .saturating_add(transaction.transaction.amount.0);
            }
            if transaction.transaction.from == *address {
                let total = transaction
                    .transaction
                    .amount
                    .0
                    .saturating_add(transaction.transaction.fee.0);
                pending.outgoing.0 = pending.outgoing.0.saturating_add(total);
            }
        }
        pending
    }

    pub fn balance_summary(&self, address: &Address) -> Option<BalanceSummary> {
        Some(BalanceSummary {
            confirmed: self.confirmed_balance(address)?,
            available: self.available_balance(address)?,
            pending: self.pending_balance(address),
        })
    }

    pub fn account_view(&self, address: &Address) -> Option<AccountView> {
        let account = self.ledger.account(address)?;
        let tip_height = self.ledger.tip_height()?;
        Some(AccountView {
            balance: account.available_balance_at(tip_height),
            unspendable: account.unspendable_balance_at(tip_height),
            nonce: account.nonce,
        })
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
    use crate::runtime::storage::Storage;
    use paqus::block::{Block, Height, Nonce};
    use paqus::consensus::supply::Amount;
    use paqus::consensus::{Consensus, ConsensusConfig};
    use paqus::crypto::{Hash, address_from_public_key, generate_keypair, sign};
    use paqus::ledger::Ledger;
    use paqus::state::Account;
    use paqus::transaction::{SignedTransaction, Transaction};

    fn address(byte: u8) -> Address {
        Address([byte; 20])
    }

    #[test]
    fn invalid_state_block_does_not_enter_fork_choice() {
        let genesis = Block::new(
            Height(0),
            Hash([0; HASH_SIZE]),
            address(9),
            1_700_000_000,
            Nonce(0),
            vec![],
        );
        let keypair = generate_keypair();
        let sender = address_from_public_key(&keypair.public_key);
        let receiver = address(2);
        let mut genesis_accounts = BTreeMap::new();
        genesis_accounts.insert(sender, Account::new(sender, Amount(100)));
        genesis_accounts.insert(receiver, Account::new(receiver, Amount(0)));
        genesis_accounts.insert(address(9), Account::new(address(9), Amount(0)));
        let mut ledger = Ledger {
            accounts: genesis_accounts.clone(),
            chain: Chain::new(),
        };
        ledger.chain.insert_block(genesis.clone()).unwrap();
        let transaction = Transaction::new(
            sender,
            receiver,
            Amount(200),
            Amount(crate::runtime::params::DEFAULT_TRANSACTION_FEE),
            Nonce(0),
        );
        let signature = sign(&keypair.secret_key, &transaction.signing_bytes());
        let signed = SignedTransaction::new(transaction, keypair.public_key, signature);
        let block = Block::new(
            Height(1),
            genesis.hash(),
            address(9),
            1_700_000_001,
            Nonce(0),
            vec![signed],
        );
        let block_hash = block.hash();
        let mut node = Node::with_genesis_accounts(
            ledger,
            Storage::temporary().unwrap(),
            Consensus {
                config: ConsensusConfig { difficulty: 0 },
            },
            genesis_accounts,
        );

        let error = node.apply_block(block).unwrap_err();

        assert!(matches!(error, NodeError::Ledger(_)));
        assert!(!node.fork_choice.contains(&block_hash));
        assert_eq!(node.tip_hash(), Some(genesis.hash()));
    }

    #[test]
    fn reorgs_from_empty_genesis_accounts() {
        let genesis = Block::new(
            Height(0),
            Hash([0; HASH_SIZE]),
            address(9),
            1_700_000_000,
            Nonce(0),
            vec![],
        );
        let mut active = Block::with_difficulty(
            Height(1),
            genesis.hash(),
            address(9),
            1,
            1_700_000_001,
            Nonce(1),
            vec![],
        );
        let mut side = Block::with_difficulty(
            Height(1),
            genesis.hash(),
            address(8),
            4,
            1_700_000_001,
            Nonce(2),
            vec![],
        );
        let genesis_accounts = BTreeMap::new();
        let mut ledger = Ledger {
            accounts: genesis_accounts.clone(),
            chain: Chain::new(),
        };
        ledger.chain.insert_block(genesis).unwrap();
        active.set_state_root(ledger.state_root_after_block(&active).unwrap());
        side.set_state_root(ledger.state_root_after_block(&side).unwrap());
        let side_hash = side.hash();
        ledger.apply_block(active).unwrap();
        let mut node = Node::with_genesis_accounts(
            ledger,
            Storage::temporary().unwrap(),
            Consensus {
                config: ConsensusConfig { difficulty: 0 },
            },
            genesis_accounts,
        );

        node.apply_block(side).unwrap();

        assert_eq!(node.tip_hash(), Some(side_hash));
        assert_eq!(
            node.balance(&address(8)),
            node.ledger.account(&address(8)).map(|a| a.balance)
        );
    }

    #[test]
    fn rejects_non_genesis_block_with_zero_state_root() {
        let genesis = Block::new(
            Height(0),
            Hash([0; HASH_SIZE]),
            address(9),
            1_700_000_000,
            Nonce(0),
            vec![],
        );
        let mut ledger = Ledger::new();
        ledger.chain.insert_block(genesis.clone()).unwrap();
        let block = Block::with_difficulty(
            Height(1),
            genesis.hash(),
            address(9),
            1,
            1_700_000_001,
            Nonce(1),
            vec![],
        );
        let block_hash = block.hash();
        let mut node = Node::with_genesis_accounts(
            ledger,
            Storage::temporary().unwrap(),
            Consensus {
                config: ConsensusConfig { difficulty: 0 },
            },
            BTreeMap::new(),
        );

        let error = node.apply_block(block).unwrap_err();

        assert!(matches!(error, NodeError::Ledger(_)));
        assert!(!node.fork_choice.contains(&block_hash));
        assert_eq!(node.tip_hash(), Some(genesis.hash()));
    }

    #[test]
    fn branch_difficulty_uses_parent_branch_window() {
        let mut node = Node::temporary(
            Ledger::new(),
            Consensus {
                config: ConsensusConfig { difficulty: 1 },
            },
        )
        .unwrap();
        let genesis = Block::with_difficulty(
            Height(0),
            Hash([0; HASH_SIZE]),
            address(9),
            1,
            1_700_000_000,
            Nonce(0),
            vec![],
        );
        let mut previous_hash = node.fork_choice.insert_block(genesis).unwrap();

        for height in 1..=DIFFICULTY_ADJUSTMENT_INTERVAL {
            let block = Block::with_difficulty(
                Height(height),
                previous_hash,
                address(9),
                1,
                1_700_000_000 + height,
                Nonce(height),
                vec![],
            );
            previous_hash = node.fork_choice.insert_block(block).unwrap();
        }

        assert_eq!(
            node.next_difficulty_after_branch_tip(previous_hash)
                .unwrap(),
            5
        );
    }

    #[test]
    fn indexes_stored_side_blocks_into_fork_choice() {
        let genesis = Block::new(
            Height(0),
            Hash([0; HASH_SIZE]),
            address(9),
            1_700_000_000,
            Nonce(0),
            vec![],
        );
        let active = Block::with_difficulty(
            Height(1),
            genesis.hash(),
            address(9),
            1,
            1_700_000_001,
            Nonce(1),
            vec![],
        );
        let side = Block::with_difficulty(
            Height(1),
            genesis.hash(),
            address(8),
            1,
            1_700_000_002,
            Nonce(2),
            vec![],
        );
        let side_hash = side.hash();
        let mut ledger = Ledger::new();
        ledger.chain.insert_block(genesis).unwrap();
        ledger.chain.insert_block(active).unwrap();
        let storage = Storage::temporary().unwrap();
        storage.save_ledger(&ledger).unwrap();
        storage.save_side_block(&side).unwrap();
        let mut node = Node::new(
            ledger,
            storage,
            Consensus {
                config: ConsensusConfig { difficulty: 0 },
            },
        );

        node.index_stored_blocks().unwrap();

        assert!(node.fork_choice.contains(&side_hash));
    }

    #[test]
    fn caches_orphan_block_until_parent_arrives() {
        let genesis = Block::new(
            Height(0),
            Hash([0; HASH_SIZE]),
            address(9),
            1_700_000_000,
            Nonce(0),
            vec![],
        );
        let mut parent = Block::with_difficulty(
            Height(1),
            genesis.hash(),
            address(9),
            1,
            1_700_000_001,
            Nonce(1),
            vec![],
        );
        let mut ledger = Ledger::new();
        ledger.chain.insert_block(genesis.clone()).unwrap();
        parent.set_state_root(ledger.state_root_after_block(&parent).unwrap());
        let mut child_ledger = ledger.clone();
        child_ledger.apply_block(parent.clone()).unwrap();

        let mut child = Block::with_difficulty(
            Height(2),
            parent.hash(),
            address(9),
            1,
            1_700_000_002,
            Nonce(2),
            vec![],
        );
        child.set_state_root(child_ledger.state_root_after_block(&child).unwrap());
        let child_hash = child.hash();
        let mut node = Node::with_genesis_accounts(
            ledger,
            Storage::temporary().unwrap(),
            Consensus {
                config: ConsensusConfig { difficulty: 0 },
            },
            BTreeMap::new(),
        );

        node.apply_block(child).unwrap();

        assert_eq!(node.orphan_blocks.len(), 1);
        assert!(!node.fork_choice.contains(&child_hash));
        assert_eq!(node.tip_hash(), Some(genesis.hash()));

        node.apply_block(parent).unwrap();

        assert!(node.orphan_blocks.is_empty());
        assert!(node.fork_choice.contains(&child_hash));
        assert_eq!(node.tip_height(), Some(Height(2)));
    }

    #[test]
    fn prunes_expired_orphan_blocks() {
        let genesis = Block::new(
            Height(0),
            Hash([0; HASH_SIZE]),
            address(9),
            1_700_000_000,
            Nonce(0),
            vec![],
        );
        let missing_parent_hash = BlockHash([7; HASH_SIZE]);
        let child = Block::with_difficulty(
            Height(1),
            missing_parent_hash,
            address(9),
            1,
            1_700_000_001,
            Nonce(1),
            vec![],
        );
        let child_hash = child.hash();
        let mut ledger = Ledger::new();
        ledger.chain.insert_block(genesis).unwrap();
        let mut node = Node::with_genesis_accounts(
            ledger,
            Storage::temporary().unwrap(),
            Consensus {
                config: ConsensusConfig { difficulty: 0 },
            },
            BTreeMap::new(),
        );

        node.apply_block(child).unwrap();
        node.orphan_blocks.get_mut(&child_hash).unwrap().received_at = 1;
        node.prune_expired_orphans(ORPHAN_BLOCK_TTL_SECS + 2);

        assert!(node.orphan_blocks.is_empty());
        assert!(node.orphan_children_by_parent.is_empty());
    }

    #[test]
    fn queues_missing_parent_request_once_for_orphans() {
        let genesis = Block::new(
            Height(0),
            Hash([0; HASH_SIZE]),
            address(9),
            1_700_000_000,
            Nonce(0),
            vec![],
        );
        let missing_parent_hash = BlockHash([7; HASH_SIZE]);
        let first = Block::with_difficulty(
            Height(1),
            missing_parent_hash,
            address(9),
            1,
            1_700_000_001,
            Nonce(1),
            vec![],
        );
        let second = Block::with_difficulty(
            Height(1),
            missing_parent_hash,
            address(8),
            1,
            1_700_000_002,
            Nonce(2),
            vec![],
        );
        let mut ledger = Ledger::new();
        ledger.chain.insert_block(genesis).unwrap();
        let mut node = Node::with_genesis_accounts(
            ledger,
            Storage::temporary().unwrap(),
            Consensus {
                config: ConsensusConfig { difficulty: 0 },
            },
            BTreeMap::new(),
        );

        node.apply_block(first).unwrap();
        node.apply_block(second).unwrap();

        assert_eq!(
            node.drain_missing_parent_requests(),
            vec![missing_parent_hash]
        );
        assert!(node.drain_missing_parent_requests().is_empty());
    }

    #[test]
    fn retries_missing_parent_request_after_cooldown() {
        let genesis = Block::new(
            Height(0),
            Hash([0; HASH_SIZE]),
            address(9),
            1_700_000_000,
            Nonce(0),
            vec![],
        );
        let missing_parent_hash = BlockHash([7; HASH_SIZE]);
        let mut ledger = Ledger::new();
        ledger.chain.insert_block(genesis).unwrap();
        let mut node = Node::with_genesis_accounts(
            ledger,
            Storage::temporary().unwrap(),
            Consensus {
                config: ConsensusConfig { difficulty: 0 },
            },
            BTreeMap::new(),
        );

        node.queue_missing_parent_request_at(missing_parent_hash, 10);
        assert!(node.drain_missing_parent_requests_at(9).is_empty());
        assert_eq!(
            node.drain_missing_parent_requests_at(10),
            vec![missing_parent_hash]
        );

        node.retry_missing_parent_request(missing_parent_hash);
        assert!(
            node.drain_missing_parent_requests_at(current_unix_timestamp())
                .is_empty()
        );
    }

    #[test]
    fn ignores_orphan_blocks_too_far_ahead_of_tip() {
        let genesis = Block::new(
            Height(0),
            Hash([0; HASH_SIZE]),
            address(9),
            1_700_000_000,
            Nonce(0),
            vec![],
        );
        let far_orphan = Block::with_difficulty(
            Height(MAX_ORPHAN_HEIGHT_DISTANCE + 1),
            BlockHash([7; HASH_SIZE]),
            address(9),
            1,
            1_700_000_001,
            Nonce(1),
            vec![],
        );
        let mut ledger = Ledger::new();
        ledger.chain.insert_block(genesis).unwrap();
        let mut node = Node::with_genesis_accounts(
            ledger,
            Storage::temporary().unwrap(),
            Consensus {
                config: ConsensusConfig { difficulty: 0 },
            },
            BTreeMap::new(),
        );

        node.apply_block(far_orphan).unwrap();

        assert!(node.orphan_blocks.is_empty());
        assert!(node.orphan_children_by_parent.is_empty());
    }
}
