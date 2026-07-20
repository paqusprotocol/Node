//! Synchronous TCP helpers used by peer adapters.

use crate::runtime::network::{NetworkMessage, read_message, write_message};
use socket2::{Domain, Protocol, SockAddr, Socket, Type};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::time::Duration;

pub fn bind_nonblocking(addr: SocketAddr, label: &str) -> Result<TcpListener, String> {
    let listener = if addr.is_ipv6() {
        let socket = Socket::new(Domain::IPV6, Type::STREAM, Some(Protocol::TCP))
            .map_err(|error| format!("failed to create {label} IPv6 socket: {error}"))?;
        socket
            .set_only_v6(true)
            .map_err(|error| format!("failed to set {label} IPv6-only mode: {error}"))?;
        socket
            .bind(&SockAddr::from(addr))
            .map_err(|error| format!("failed to bind {label} {addr}: {error}"))?;
        socket
            .listen(1024)
            .map_err(|error| format!("failed to listen on {label} {addr}: {error}"))?;
        socket.into()
    } else {
        TcpListener::bind(addr)
            .map_err(|error| format!("failed to bind {label} {addr}: {error}"))?
    };
    listener
        .set_nonblocking(true)
        .map_err(|error| format!("failed to set {label} listener nonblocking: {error}"))?;
    Ok(listener)
}

pub fn configure_stream(stream: &TcpStream, timeout: Duration) -> Result<(), String> {
    stream
        .set_nonblocking(false)
        .map_err(|error| format!("failed to set stream blocking mode: {error}"))?;
    stream
        .set_read_timeout(Some(timeout))
        .map_err(|error| format!("failed to set read timeout: {error}"))?;
    stream
        .set_write_timeout(Some(timeout))
        .map_err(|error| format!("failed to set write timeout: {error}"))?;
    Ok(())
}

pub fn connect_peer(peer: SocketAddr) -> Result<TcpStream, String> {
    let stream = TcpStream::connect_timeout(&peer, Duration::from_secs(2))
        .map_err(|error| format!("connect failed: {error}"))?;
    configure_stream(&stream, Duration::from_secs(5))?;
    Ok(stream)
}

pub fn request_on_stream(
    stream: &mut TcpStream,
    message: NetworkMessage,
) -> Result<NetworkMessage, String> {
    write_message(stream, &message.to_envelope())
        .map_err(|error| format!("send failed: {error}"))?;
    read_message(stream)
        .map(|envelope| envelope.message)
        .map_err(|error| format!("read failed: {error}"))
}
