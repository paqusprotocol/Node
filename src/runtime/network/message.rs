use crate::runtime::network::error::NetworkError;
use crate::runtime::params::{CURRENT_CHAIN_PARAMS, MAX_NETWORK_MESSAGE_SIZE};
use borsh::{BorshDeserialize, BorshSerialize};
use paqus::block::{Block, BlockHeader, BlockHeight};
use paqus::crypto::{BlockHash, TransactionHash};
use paqus::transaction::SignedProtocolTransaction;

#[derive(BorshSerialize, BorshDeserialize, Clone, Debug, PartialEq, Eq)]
pub struct PeerInfo {
    pub address: String,
}

#[derive(BorshSerialize, BorshDeserialize, Clone, Copy, Debug, PartialEq, Eq)]
pub struct TipInfo {
    pub height: BlockHeight,
    pub hash: BlockHash,
}

#[derive(BorshSerialize, BorshDeserialize, Clone, Debug, PartialEq, Eq)]
pub struct VersionInfo {
    pub protocol_version: u8,
    pub chain_id: u16,
    pub chain_name: String,
    pub protocol_stage: String,
    pub pow_algorithm: String,
    pub difficulty_algorithm: String,
    pub network_magic: [u8; 4],
    pub tip: Option<TipInfo>,
}

impl VersionInfo {
    pub fn local(tip: Option<TipInfo>) -> Self {
        Self {
            protocol_version: CURRENT_CHAIN_PARAMS.protocol_version,
            chain_id: CURRENT_CHAIN_PARAMS.chain_id,
            chain_name: CURRENT_CHAIN_PARAMS.chain_name.to_string(),
            protocol_stage: CURRENT_CHAIN_PARAMS.protocol_stage.to_string(),
            pow_algorithm: CURRENT_CHAIN_PARAMS.pow_algorithm.to_string(),
            difficulty_algorithm: CURRENT_CHAIN_PARAMS.difficulty_algorithm.to_string(),
            network_magic: CURRENT_CHAIN_PARAMS.network_magic,
            tip,
        }
    }

    pub fn validate_compatibility(&self) -> Result<(), RejectReason> {
        if self.network_magic != CURRENT_CHAIN_PARAMS.network_magic {
            return Err(RejectReason::NetworkMismatch);
        }
        if self.chain_id != CURRENT_CHAIN_PARAMS.chain_id
            || self.chain_name != CURRENT_CHAIN_PARAMS.chain_name
            || self.protocol_stage != CURRENT_CHAIN_PARAMS.protocol_stage
        {
            return Err(RejectReason::ChainMismatch);
        }
        if self.protocol_version != CURRENT_CHAIN_PARAMS.protocol_version {
            return Err(RejectReason::ProtocolVersionMismatch);
        }
        if self.pow_algorithm != CURRENT_CHAIN_PARAMS.pow_algorithm
            || self.difficulty_algorithm != CURRENT_CHAIN_PARAMS.difficulty_algorithm
        {
            return Err(RejectReason::ConsensusMismatch);
        }
        Ok(())
    }
}

#[derive(BorshSerialize, BorshDeserialize, Clone, Debug, PartialEq, Eq)]
pub enum RejectReason {
    ProtocolVersionMismatch,
    ChainMismatch,
    NetworkMismatch,
    ConsensusMismatch,
    InvalidMessage,
}

#[derive(BorshSerialize, BorshDeserialize, Clone, Debug, PartialEq, Eq)]
pub enum InventoryItem {
    Block(BlockHash),
    Transaction(TransactionHash),
}

#[derive(BorshSerialize, BorshDeserialize, Clone, Debug, PartialEq, Eq)]
#[allow(clippy::large_enum_variant)] // Boxing would alter the public message construction API.
pub enum NetworkMessage {
    Version(VersionInfo),
    VerAck(VersionInfo),
    Reject {
        reason: RejectReason,
        message: String,
    },
    Ping {
        nonce: u64,
    },
    Pong {
        nonce: u64,
    },
    GetTip,
    Tip(TipInfo),
    GetBlockByHeight {
        height: BlockHeight,
    },
    GetBlocksByHeightRange {
        start: BlockHeight,
        limit: u32,
    },
    GetBlockHeadersByHeightRange {
        start: BlockHeight,
        limit: u32,
    },
    GetCommonAncestor {
        locator: Vec<BlockHash>,
    },
    CommonAncestor(Option<TipInfo>),
    GetBlockByHash {
        hash: BlockHash,
    },
    Block(Block),
    Blocks(Vec<Block>),
    BlockHeaders(Vec<BlockHeader>),
    Inventory(Vec<InventoryItem>),
    GetData(Vec<InventoryItem>),
    Transaction(SignedProtocolTransaction),
    Transactions(Vec<SignedProtocolTransaction>),
    GetMempoolInventory,
    GetPeers,
    Peers(Vec<PeerInfo>),
}

#[derive(BorshSerialize, BorshDeserialize, Clone, Debug, PartialEq, Eq)]
pub struct NetworkEnvelope {
    pub magic: [u8; 4],
    pub message: NetworkMessage,
}

impl NetworkEnvelope {
    pub fn new(message: NetworkMessage) -> Self {
        Self {
            magic: CURRENT_CHAIN_PARAMS.network_magic,
            message,
        }
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, NetworkError> {
        let bytes = borsh::to_vec(self)?;
        if bytes.len() > MAX_NETWORK_MESSAGE_SIZE {
            return Err(NetworkError::MessageTooLarge);
        }
        Ok(bytes)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, NetworkError> {
        if bytes.len() > MAX_NETWORK_MESSAGE_SIZE {
            return Err(NetworkError::MessageTooLarge);
        }

        let envelope = Self::try_from_slice(bytes)?;
        if envelope.magic != CURRENT_CHAIN_PARAMS.network_magic {
            return Err(NetworkError::Serialization(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "network magic mismatch",
            )));
        }
        Ok(envelope)
    }
}

impl NetworkMessage {
    #[allow(clippy::wrong_self_convention)] // Conversion intentionally consumes the message.
    pub fn to_envelope(self) -> NetworkEnvelope {
        NetworkEnvelope::new(self)
    }
}
