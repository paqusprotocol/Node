pub mod error;
pub mod extension;
#[allow(clippy::module_inception)] // Preserve the established public module path.
pub mod mempool;

pub use error::MempoolError;
pub use extension::ExtensionMempool;
pub use mempool::{Mempool, MempoolConfig};

#[cfg(test)]
mod test;
