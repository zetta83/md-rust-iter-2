use crate::clients::StreamManager;
use crate::errors::{ServerError, StreamError};
use quote_libs::error;
use std::io::{BufRead, BufReader, Write};
use std::net::TcpStream;
use std::sync::{Arc, Mutex};

pub fn handler(stream: TcpStream, manager: Arc<Mutex<StreamManager>>) -> Result<(), ServerError> {
    let mut writer = stream.try_clone().expect("failed to clone stream");
    let mut reader = BufReader::new(stream);

    let _ = writer.write_all(b"Welcome to the server!\n");
    let _ = writer.flush();

    let mut line = String::new();
    loop {
        line.clear();
        match reader.read_line(&mut line) {
            Ok(0) => {
                return Err(ServerError::Io(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "client disconnected",
                )));
            }
            Ok(_) => {
                let input = line.trim();
                if input.is_empty() {
                    let _ = writer.flush();
                    continue;
                }

                let response = handle_command(input, &manager).unwrap_or_else(|e| {
                    error!("{}", e);
                    e.to_string() + "\r\n"
                });

                let _ = writer.write_all(format!("{}\r\n", response).as_bytes());
                let _ = writer.flush();
            }
            Err(e) => {
                return Err(ServerError::Io(e));
            }
        }
    }
}

fn handle_command(input: &str, manager: &Arc<Mutex<StreamManager>>) -> Result<String, StreamError> {
    let mut parts = input.split_whitespace();

    match parts.next() {
        Some("STREAM") => Ok(handle_stream_command(parts, manager)?),
        _ => Err(StreamError::UnknownCommand),
    }
}

fn handle_stream_command(
    mut parts: std::str::SplitWhitespace,
    manager: &Arc<Mutex<StreamManager>>,
) -> Result<String, StreamError> {
    let addr_str = parts.next().ok_or(StreamError::MissingAddress)?;
    let udp_addr = parse_udp_address(addr_str)?;

    let tickers_str = parts.next().ok_or(StreamError::MissingTickers)?;
    let tickers: Vec<&str> = tickers_str.split(',').collect();

    if tickers.is_empty() {
        return Err(StreamError::EmptyTickers);
    }

    if let Ok(mut stream_manager) = manager.lock() {
        stream_manager.add_stream(udp_addr, &tickers)?;
        Ok(format!("Successfully added '{}'", udp_addr))
    } else {
        Err(StreamError::ManagerLockFailed)
    }
}

fn parse_udp_address(addr_str: &str) -> Result<std::net::SocketAddr, StreamError> {
    let clean_addr = if let Some(stripped) = addr_str.strip_prefix("udp://") {
        stripped
    } else {
        addr_str
    };

    clean_addr
        .parse::<std::net::SocketAddr>()
        .map_err(|_| StreamError::InvalidAddress(clean_addr.to_string()))
}
