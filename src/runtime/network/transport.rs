use crate::runtime::network::error::NetworkError;
use crate::runtime::network::message::NetworkEnvelope;
use crate::runtime::params::MAX_NETWORK_MESSAGE_SIZE;
use std::io::{Read, Write};

const MESSAGE_LENGTH_SIZE: usize = 4;

pub fn write_message<W: Write>(
    writer: &mut W,
    envelope: &NetworkEnvelope,
) -> Result<(), NetworkError> {
    let bytes = envelope.to_bytes()?;
    let length = u32::try_from(bytes.len()).map_err(|_| NetworkError::MessageTooLarge)?;

    writer
        .write_all(&length.to_be_bytes())
        .map_err(NetworkError::Io)?;
    writer.write_all(&bytes).map_err(NetworkError::Io)?;
    Ok(())
}

pub fn read_message<R: Read>(reader: &mut R) -> Result<NetworkEnvelope, NetworkError> {
    let mut length_bytes = [0_u8; MESSAGE_LENGTH_SIZE];
    reader
        .read_exact(&mut length_bytes)
        .map_err(NetworkError::Io)?;
    let length = u32::from_be_bytes(length_bytes) as usize;

    if length > MAX_NETWORK_MESSAGE_SIZE {
        return Err(NetworkError::MessageTooLarge);
    }

    let mut bytes = vec![0_u8; length];
    reader.read_exact(&mut bytes).map_err(NetworkError::Io)?;
    NetworkEnvelope::from_bytes(&bytes)
}
