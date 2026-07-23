pub mod error;
pub mod handler;
pub mod message;
pub mod transport;

pub use error::NetworkError;
pub use handler::handle_message;
pub use message::{InventoryItem, NetworkMessage, PeerInfo, TipInfo, VersionInfo};
#[cfg(test)]
pub use message::{NetworkEnvelope, RejectReason};
pub use transport::{read_message, write_message};

#[cfg(test)]
mod test;
