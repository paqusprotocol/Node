use crate::paquscore::{NetworkMessage, read_message, write_message};
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::time::Duration;

pub fn bind_nonblocking(addr: SocketAddr, label: &str) -> Result<TcpListener, String> {
    let listener = TcpListener::bind(addr)
        .map_err(|error| format!("failed to bind {label} {addr}: {error}"))?;
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

pub fn roundtrip(peer: SocketAddr, message: NetworkMessage) -> Result<NetworkMessage, String> {
    let mut stream = TcpStream::connect_timeout(&peer, Duration::from_secs(2))
        .map_err(|error| format!("connect failed: {error}"))?;
    configure_stream(&stream, Duration::from_secs(5))?;
    write_message(&mut stream, &message.to_envelope())
        .map_err(|error| format!("send failed: {error}"))?;
    read_message(&mut stream)
        .map(|envelope| envelope.message)
        .map_err(|error| format!("read failed: {error}"))
}

pub fn send_message(peer: SocketAddr, message: NetworkMessage) -> Result<(), String> {
    let mut stream = TcpStream::connect_timeout(&peer, Duration::from_secs(2))
        .map_err(|error| format!("connect failed: {error}"))?;
    configure_stream(&stream, Duration::from_secs(5))?;
    write_message(&mut stream, &message.to_envelope())
        .map_err(|error| format!("send failed: {error}"))
}

pub fn http_post_json(addr: &str, path: &str, body: &str) -> Result<String, String> {
    let addr = addr
        .parse::<SocketAddr>()
        .map_err(|error| format!("invalid rpc address: {error}"))?;
    let mut stream = TcpStream::connect_timeout(&addr, Duration::from_secs(3))
        .map_err(|error| format!("failed to connect rpc: {error}"))?;
    configure_stream(&stream, Duration::from_secs(5))?;
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
    configure_stream(&stream, Duration::from_secs(5))?;
    let request = format!("GET {path} HTTP/1.1\r\nhost: {addr}\r\nconnection: close\r\n\r\n");
    stream
        .write_all(request.as_bytes())
        .map_err(|error| format!("failed to write rpc request: {error}"))?;
    read_http_response(stream)
}

fn read_http_response(mut stream: TcpStream) -> Result<String, String> {
    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .map_err(|error| format!("failed to read rpc response: {error}"))?;
    Ok(response
        .split_once("\r\n\r\n")
        .map(|(_, body)| body.to_string())
        .unwrap_or(response))
}
