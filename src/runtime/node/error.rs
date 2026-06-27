use crate::runtime::mempool::MempoolError;
use crate::runtime::storage::StorageError;
use paqus::consensus::ConsensusError;
use paqus::genesis::GenesisError;
use paqus::ledger::LedgerError;
use paqus::ledger::fork_choice::ForkChoiceError;
use std::error::Error;
use std::fmt;

#[derive(Debug)]
pub enum NodeError {
    Consensus(ConsensusError),
    Genesis(GenesisError),
    ForkChoice(ForkChoiceError),
    Ledger(LedgerError),
    Mempool(MempoolError),
    Storage(StorageError),
    MiningExhausted,
    MissingGenesisState,
    CheckpointMismatch,
    ReorgRequired,
}

impl fmt::Display for NodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NodeError::Consensus(error) => write!(f, "consensus error: {error}"),
            NodeError::Genesis(error) => write!(f, "genesis error: {error}"),
            NodeError::ForkChoice(error) => write!(f, "fork choice error: {error:?}"),
            NodeError::Ledger(error) => write!(f, "ledger error: {error}"),
            NodeError::Mempool(error) => write!(f, "mempool error: {error}"),
            NodeError::Storage(error) => write!(f, "storage error: {error}"),
            NodeError::MiningExhausted => f.write_str("mining attempt budget was exhausted"),
            NodeError::MissingGenesisState => {
                f.write_str("node cannot reorg without a genesis state snapshot")
            }
            NodeError::CheckpointMismatch => {
                f.write_str("block does not match hardcoded checkpoint")
            }
            NodeError::ReorgRequired => f.write_str("fork choice selected a non-linear best tip"),
        }
    }
}

impl Error for NodeError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            NodeError::Consensus(error) => Some(error),
            NodeError::Genesis(error) => Some(error),
            NodeError::ForkChoice(_) => None,
            NodeError::Ledger(error) => Some(error),
            NodeError::Mempool(error) => Some(error),
            NodeError::Storage(error) => Some(error),
            NodeError::MiningExhausted => None,
            NodeError::MissingGenesisState => None,
            NodeError::CheckpointMismatch => None,
            NodeError::ReorgRequired => None,
        }
    }
}

impl From<ConsensusError> for NodeError {
    fn from(error: ConsensusError) -> Self {
        NodeError::Consensus(error)
    }
}

impl From<GenesisError> for NodeError {
    fn from(error: GenesisError) -> Self {
        NodeError::Genesis(error)
    }
}

impl From<ForkChoiceError> for NodeError {
    fn from(error: ForkChoiceError) -> Self {
        NodeError::ForkChoice(error)
    }
}

impl From<LedgerError> for NodeError {
    fn from(error: LedgerError) -> Self {
        NodeError::Ledger(error)
    }
}

impl From<MempoolError> for NodeError {
    fn from(error: MempoolError) -> Self {
        NodeError::Mempool(error)
    }
}

impl From<StorageError> for NodeError {
    fn from(error: StorageError) -> Self {
        NodeError::Storage(error)
    }
}
