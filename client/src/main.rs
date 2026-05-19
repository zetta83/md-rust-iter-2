use crate::client::QuoteClient;
use crate::errors::QuoteClientError;
use crate::receiver::{Receiver, TickerReceiver};
use crate::sender::PingSender;
use clap::Parser;
use quote_libs::{debug, error, filter_tickers, info, init_logger};
use std::io::{BufRead, BufReader};
use std::sync::{Arc, Mutex};
use std::{fs, thread};

mod client;
mod errors;
mod receiver;
mod sender;

#[derive(Parser)]
#[command(
    name = "quote_client",
    version = "0.0.1",
    about = "Client to query quotes"
)]
struct Args {
    /// Адрес и порт TCP-сервера
    #[arg(long)]
    server_addr: String,

    /// Порт для приёма UDP-данных
    #[arg(long)]
    udp_port: String,

    /// Путь к файлу со списком тикеров для подписки
    #[arg(long)]
    tickers_file: String,
}

// - Клиент принимает аргументы через clap: --server-addr, --udp-port, --tickers-file.
// - Файл тикеров читается построчно, игнорируются пустые строки и пробелы.
// - При отсутствии файла или ошибке чтения выводится понятная ошибка.
fn main() -> Result<(), QuoteClientError> {
    init_logger();
    info!("Starting quote client");

    let args = Args::parse();

    let file_h = fs::File::open(args.tickers_file)
        .map_err(|e| QuoteClientError::IOError(format!("tickers file not found: {}", e)))?;
    let reader = BufReader::new(file_h);

    let tickers: Vec<String> = filter_tickers(
        &reader
            .lines()
            .map(|l| l.map(|s| s.trim().to_string()))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| QuoteClientError::IOError(format!("error reading line: {}", e)))?,
    );

    let client = QuoteClient::new(args.server_addr.as_str(), args.udp_port.as_str(), &tickers);
    client.send_command_create_stream()?;

    info!("Starting quote client");

    let receiver: Arc<dyn Receiver> = Arc::new(TickerReceiver::new(&format!(
        "127.0.0.1:{}",
        args.udp_port
    ))?);
    let (receiver_handle, ticker_rx) = receiver.start_with_channel();
    let heartbeat = Arc::new(Mutex::new(PingSender::new(Arc::clone(&receiver))));

    let mut heartbeat_started = false;

    loop {
        match ticker_rx.recv() {
            Ok((ticker, src_addr)) => {
                debug!("Received ticker: {:?} from {:?}", ticker, src_addr);

                if !heartbeat_started {
                    heartbeat_started = true;
                    let src_addr_str = src_addr.to_string();
                    let heartbeat_clone = Arc::clone(&heartbeat);

                    thread::spawn(move || {
                        heartbeat_clone
                            .lock()
                            .map_err(|e| {
                                error!("heartbeat thread panicked: {:?}", e);
                                QuoteClientError::HeartbeatLockFailed
                            })
                            .and_then(|mut heartbeat_guard| {
                                heartbeat_guard.start_heartbeat(&src_addr_str).map_err(|e| {
                                    error!("error starting heartbeat thread: {:?}", e);
                                    QuoteClientError::HeartbeatLockFailed
                                })
                            })
                            .ok();
                    });
                }
            }
            Err(e) => {
                error!("Error receiving ticker: {}", e);
                break;
            }
        }
    }

    let _ = receiver_handle.join();

    Ok(())
}
