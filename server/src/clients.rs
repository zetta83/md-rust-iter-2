use crate::errors::StreamError;
use crate::sender::TickersSender;
use quote_libs::{QuoteGenerator, StockQuote, debug, error, info};
use std::collections::{HashMap, HashSet};
use std::net::{SocketAddr, UdpSocket};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

struct StreamParams {
    tickers: Vec<String>,
    last_seen: Instant,
    sender_handler: JoinHandle<()>,
}

pub struct StreamManager {
    socket: Arc<UdpSocket>,
    tickers_sender: Arc<TickersSender>,

    streams: Arc<Mutex<HashMap<SocketAddr, StreamParams>>>,
    tickers: Arc<Mutex<HashSet<String>>>,
    quotes: Arc<Mutex<HashMap<String, StockQuote>>>,
}

impl StreamManager {
    pub fn new() -> Result<Self, StreamError> {
        let socket = Arc::new(UdpSocket::bind("0.0.0.0:0").map_err(|e| {
            error!("Error occurred while binding socket: {}", e);
            StreamError::AddressAlreadyExists
        })?);
        let socket_clone = Arc::clone(&socket);

        let sm = StreamManager {
            socket,
            streams: Arc::new(Mutex::new(HashMap::new())),
            tickers: Arc::new(Mutex::new(HashSet::new())),
            tickers_sender: Arc::new(TickersSender::new(socket_clone).map_err(|e| {
                error!("{}", e);
                StreamError::AddressAlreadyExists
            })?),
            quotes: Arc::new(Mutex::new(HashMap::new())),
        };

        sm.starting_threads();

        Ok(sm)
    }

    fn starting_threads(&self) {
        let (tx, rx) = mpsc::channel();

        let tickers_mutex_copy = Arc::clone(&self.tickers);
        thread::spawn(move || {
            let quote_generator = QuoteGenerator::new();

            loop {
                if let Ok(tickers_mutex) = tickers_mutex_copy.try_lock() {
                    let ts: Vec<_> = tickers_mutex.iter().map(|v| v.as_str()).collect();
                    if let Err(e) = tx.send(quote_generator.generator_quotes(&ts)) {
                        error!("generator_quotes: {}", e);
                    }
                }
                thread::sleep(Duration::from_secs(1));
            }
        });

        let quotes_copy = Arc::clone(&self.quotes);
        thread::spawn(move || {
            loop {
                if let Ok(Some(quotes)) = rx.recv() {
                    match quotes_copy.lock() {
                        Ok(mut v) => {
                            for item in quotes {
                                v.insert(item.ticker.clone(), item);
                            }
                        }
                        Err(e) => {
                            error!("{}", e);
                        }
                    }
                }
            }
        });

        let socket_clone = Arc::clone(&self.socket);
        let streams_mutex_copy = Arc::clone(&self.streams);

        thread::spawn(move || {
            loop {
                let mut buf = [0u8; 1024];

                match socket_clone.recv_from(&mut buf) {
                    Ok((size, src_addr)) => {
                        debug!("Получено {} байт от {}", size, src_addr);

                        if let Ok(msg_raw) =
                            String::from_utf8(Vec::from(&buf[..size])).map_err(|e| {
                                error!("{}", e);
                            })
                            && "PING" == msg_raw.trim().to_uppercase()
                        {
                            match streams_mutex_copy
                                .lock()
                                .map_err(|_| StreamError::ManagerLockFailed)
                            {
                                Ok(mut streams) => {
                                    if let Some(stream) = streams.get_mut(&src_addr) {
                                        stream.last_seen = Instant::now();
                                        info!("success updated last_seen for stream {}", src_addr);
                                    } else {
                                        error!("error with stream {}", src_addr);
                                    }
                                }
                                Err(e) => {
                                    error!("{}", e);
                                }
                            }

                            debug!("has PING from {}", src_addr);
                        }
                    }
                    Err(e) => {
                        error!("{}", e);
                    }
                }
            }
        });
    }

    fn starting_broadcast(&self, target_addr: String, tickers: Vec<String>) -> JoinHandle<()> {
        let sender_clone = Arc::clone(&self.tickers_sender);
        let quotes_clone = Arc::clone(&self.quotes);

        thread::spawn(move || {
            if let Err(e) = sender_clone.start_broadcasting(
                target_addr.as_str(),
                tickers.as_ref(),
                &quotes_clone,
                1000,
            ) {
                error!("{}", e);
            }
        })
    }

    pub fn add_stream(&mut self, addr: SocketAddr, tickers: &[&str]) -> Result<(), StreamError> {
        let mut streams = self
            .streams
            .lock()
            .map_err(|_| StreamError::ManagerLockFailed)?;

        if streams.contains_key(&addr) {
            return Err(StreamError::AddressAlreadyExists);
        }

        self.tickers
            .lock()
            .map_err(|_| StreamError::ManagerLockFailed)?
            .extend(tickers.iter().map(|s| s.to_string()));

        let sender_handler = self.starting_broadcast(
            addr.to_string(),
            tickers.iter().map(|s| s.to_string()).collect(),
        );

        let client_tickers = tickers.iter().map(|s| s.to_string()).collect();
        streams.insert(
            addr,
            StreamParams {
                tickers: client_tickers,
                last_seen: Instant::now(),
                sender_handler,
            },
        );

        drop(streams);

        debug!(
            "Added stream {} for tickers {:?}",
            addr,
            self.get_stream_tickers(&addr)
        );

        Ok(())
    }

    pub fn cleanup_expired_streams(&mut self, timeout_seconds: u64) -> Result<(), StreamError> {
        let timeout = Duration::from_secs(timeout_seconds);
        let mut streams = self
            .streams
            .lock()
            .map_err(|_| StreamError::ManagerLockFailed)?;

        let expired_addrs: Vec<SocketAddr> = streams
            .iter()
            .filter(|(_, stream)| stream.last_seen.elapsed() >= timeout)
            .map(|(addr, _)| *addr)
            .collect();

        debug!("expired_addrs: {:?}", expired_addrs);

        for addr in expired_addrs {
            if let Some(stream) = streams.remove(&addr) {
                info!(
                    "Removing expired stream: {} (no PING for {} seconds)",
                    addr, timeout_seconds
                );

                for ticker in &stream.tickers {
                    let ticker_in_use = { streams.values().any(|t| t.tickers.contains(ticker)) };

                    if !ticker_in_use && let Ok(mut ts) = self.tickers.lock() {
                        info!("removing ticker {}", ticker);
                        ts.remove(ticker);
                    }
                }

                let sender_clone = Arc::clone(&self.tickers_sender);
                if let Err(e) = sender_clone.stop_broadcasting(addr.to_string().as_str()) {
                    error!("Failed send stop {}", e);
                }

                if let Err(e) = stream.sender_handler.join() {
                    error!("Failed to join stream handler for {}: {:?}", addr, e);
                }
            }
        }

        Ok(())
    }

    pub fn get_stream_tickers(&self, addr: &SocketAddr) -> Option<Vec<String>> {
        if let Ok(streams) = self
            .streams
            .lock()
            .map_err(|_| StreamError::ManagerLockFailed)
        {
            Some(streams.get(addr)?.tickers.to_vec())
        } else {
            None
        }
    }
}
