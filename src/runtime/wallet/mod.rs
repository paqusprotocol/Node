pub mod error;
#[allow(clippy::module_inception)] // Preserve the established public module path.
pub mod wallet;

pub use error::WalletError;
pub use wallet::Wallet;

#[cfg(test)]
mod test;
