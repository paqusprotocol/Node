use super::{Wallet, WalletError};
use crate::runtime::params::BASE_FEE;
use paqus::block::Nonce;
use paqus::consensus::supply::Amount;
use paqus::crypto::{Address, address_from_string};
use paqus::transaction::Transaction;

fn receiver() -> Address {
    Address([2; 20])
}

#[test]
fn generates_wallet_with_address() {
    let wallet = Wallet::generate();
    let wallet_address = wallet.wallet_address();

    assert_eq!(address_from_string(&wallet_address), Ok(wallet.address));
}

#[test]
fn restores_wallet_from_secret_key() {
    let wallet = Wallet::generate();
    let restored = Wallet::from_secret_key(wallet.secret_key);

    assert_eq!(restored.public_key, wallet.public_key);
    assert_eq!(restored.address, wallet.address);
}

#[test]
fn signs_transaction_from_wallet_address() {
    let wallet = Wallet::generate();
    let transaction = Transaction::new(
        wallet.address,
        receiver(),
        Amount(10),
        Amount(BASE_FEE),
        Nonce(0),
    );

    let signed = wallet.sign_transaction(transaction).unwrap();

    assert_eq!(signed.transaction.from, wallet.address);
    assert_eq!(signed.witness.public_key, wallet.public_key);
    assert_eq!(signed.validate_signed(), Ok(()));
}

#[test]
fn rejects_transaction_from_different_address() {
    let wallet = Wallet::generate();
    let transaction = Transaction::new(
        Address([9; 20]),
        receiver(),
        Amount(10),
        Amount(BASE_FEE),
        Nonce(0),
    );

    assert_eq!(
        wallet.sign_transaction(transaction),
        Err(WalletError::SenderAddressMismatch)
    );
}
