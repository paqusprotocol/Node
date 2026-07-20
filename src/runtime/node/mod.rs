pub mod error;
#[allow(clippy::module_inception)] // Preserve the established public module path.
pub mod node;

pub use error::NodeError;
#[cfg(test)]
pub use node::AccountView;
pub use node::Node;

#[cfg(test)]
mod test;
