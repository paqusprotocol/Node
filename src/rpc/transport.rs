//! Synchronous HTTP/TCP helpers used by CLI and peer adapters.

use crate::runtime::network::{NetworkMessage, read_message, write_message};
use socket2::{Domain, Protocol, SockAddr, Socket, Type};
use std::io::{ErrorKind, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::time::Duration;

const RPC_HTTP_TIMEOUT: Duration = Duration::from_secs(60);

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

pub fn http_post_json(addr: &str, path: &str, body: &str) -> Result<String, String> {
    let addr = addr
        .parse::<SocketAddr>()
        .map_err(|error| format!("invalid rpc address: {error}"))?;
    let mut stream = TcpStream::connect_timeout(&addr, Duration::from_secs(3))
        .map_err(|error| format!("failed to connect rpc: {error}"))?;
    configure_stream(&stream, RPC_HTTP_TIMEOUT)?;
    let request = format!(
        "POST {path} HTTP/1.1\r\nhost: {addr}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
        body.len(),
        body
    );
    stream
        .write_all(request.as_bytes())
        .map_err(|error| format!("failed to write rpc request: {error}"))?;
    read_http_response(stream)
}

pub fn http_get(addr: &str, path: &str) -> Result<String, String> {
    let addr = addr
        .parse::<SocketAddr>()
        .map_err(|error| format!("invalid rpc address: {error}"))?;
    let mut stream = TcpStream::connect_timeout(&addr, Duration::from_secs(3))
        .map_err(|error| format!("failed to connect rpc: {error}"))?;
    configure_stream(&stream, RPC_HTTP_TIMEOUT)?;
    let request = format!("GET {path} HTTP/1.1\r\nhost: {addr}\r\nconnection: close\r\n\r\n");
    stream
        .write_all(request.as_bytes())
        .map_err(|error| format!("failed to write rpc request: {error}"))?;
    read_http_response(stream)
}

fn read_http_response(mut stream: TcpStream) -> Result<String, String> {
    let mut response = Vec::new();
    let mut buffer = [0u8; 4096];
    loop {
        match stream.read(&mut buffer) {
            Ok(0) => break,
            Ok(bytes_read) => {
                response.extend_from_slice(&buffer[..bytes_read]);
                if response_body_complete(&response)? {
                    break;
                }
            }
            Err(error) if matches!(error.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) => {
                if response_body_complete(&response)? {
                    break;
                }
                return Err(
                    "failed to read rpc response: timed out waiting for node response".to_string(),
                );
            }
            Err(error) => return Err(format!("failed to read rpc response: {error}")),
        }
    }
    let response = String::from_utf8(response)
        .map_err(|error| format!("failed to decode rpc response: {error}"))?;
    Ok(response
        .split_once("\r\n\r\n")
        .map(|(_, body)| body.to_string())
        .unwrap_or(response))
}

fn response_body_complete(response: &[u8]) -> Result<bool, String> {
    let Some(header_end) = response.windows(4).position(|window| window == b"\r\n\r\n") else {
        return Ok(false);
    };
    let headers = std::str::from_utf8(&response[..header_end])
        .map_err(|error| format!("failed to decode rpc response headers: {error}"))?;
    let Some(content_length) = headers.lines().find_map(content_length) else {
        return Ok(false);
    };
    Ok(response.len() >= header_end + 4 + content_length)
}

fn content_length(line: &str) -> Option<usize> {
    let (name, value) = line.split_once(':')?;
    name.eq_ignore_ascii_case("content-length")
        .then(|| value.trim().parse().ok())
        .flatten()
}
