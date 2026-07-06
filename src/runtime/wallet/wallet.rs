use crate::runtime::wallet::error::WalletError;
use paqus::crypto::{
    Address, PublicKey, SecretKey, address_from_public_key, address_to_string, derive_public_key,
    generate_keypair, sign,
};
use paqus::transaction::{SignedTransaction, Transaction};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Wallet {
    pub public_key: PublicKey,
    pub secret_key: SecretKey,
    pub address: Address,
}

impl Wallet {
    pub fn generate() -> Self {
        let keypair = generate_keypair();
        Self::from_keypair(keypair.public_key, keypair.secret_key)
    }

    pub fn from_secret_key(secret_key: SecretKey) -> Self {
        let public_key = derive_public_key(&secret_key);
        Self::from_keypair(public_key, secret_key)
    }

    pub fn from_keypair(public_key: PublicKey, secret_key: SecretKey) -> Self {
        let address = address_from_public_key(&public_key);
        Self {
            public_key,
            secret_key,
            address,
        }
    }

    pub fn wallet_address(&self) -> String {
        address_to_string(&self.address)
    }

    pub fn sign_transaction(
        &self,
        transaction: Transaction,
    ) -> Result<SignedTransaction, WalletError> {
        if transaction.from != self.address {
            return Err(WalletError::SenderAddressMismatch);
        }

        let signature = sign(&self.secret_key, &transaction.signing_bytes());
        Ok(SignedTransaction::new(
            transaction,
            self.public_key,
            signature,
        ))
    }
}
