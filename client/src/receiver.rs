use crate::errors::QuoteClientError;
use quote_libs::{StockQuote, debug, error, trace, warn};
use std::net::{SocketAddr, UdpSocket};
use std::sync::{Arc, mpsc};
use std::thread;

pub struct TickerReceiver {
    socket: Arc<UdpSocket>,
}

impl TickerReceiver {
    pub fn new(bind_addr: &str) -> Result<Self, std::io::Error> {
        let socket = UdpSocket::bind(bind_addr)?;

        debug!("TickerReceiver bind to {}", bind_addr);

        Ok(TickerReceiver {
            socket: Arc::new(socket),
        })
    }

    pub fn start_with_channel(
        &self,
    ) -> (
        thread::JoinHandle<()>,
        mpsc::Receiver<(StockQuote, std::net::SocketAddr)>,
    ) {
        let (tx, rx) = mpsc::channel();
        let socket = Arc::clone(&self.socket);

        let handle = thread::spawn(move || {
            debug!("TickerReceiver start");
            if let Err(e) = Self::receive_loop_with_channel(socket, tx) {
                error!("Error occurred while receiving data: {}", e);
            }
        });

        (handle, rx)
    }

    pub fn receive_loop_with_channel(
        socket: Arc<UdpSocket>,
        tx: mpsc::Sender<(StockQuote, std::net::SocketAddr)>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut buf = [0u8; 1024];
        let socket = Arc::clone(&socket);

        loop {
            match socket.recv_from(&mut buf) {
                Ok((size, src_addr)) => {
                    trace!("Получено {} байт от {}", size, src_addr);

                    let stock_quote_raw = String::from_utf8(Vec::from(&buf[..size]))?;
                    if let Some(stock_quote) = StockQuote::from_string(&stock_quote_raw) {
                        trace!(">: {:?}", stock_quote);
                        if tx.send((stock_quote, src_addr)).is_err() {
                            warn!("channel closed unexpectedly");
                            return Err(Box::from(QuoteClientError::IOError("ssss".to_string())));
                        }
                    } else {
                        error!("Error: can't parse {}", stock_quote_raw);
                    }
                }
                Err(e) => {
                    error!("Error occurred while receiving data: {}", e);
                }
            }

            // self.socket.send(buf.as_ref())?;
        }
    }

    pub fn send_ping(&self, serv_addr: &str) -> Result<(), Box<dyn std::error::Error>> {
        self.socket.send_to(b"ping", serv_addr)?;
        Ok(())
    }
}

pub trait Receiver: Send + Sync {
    fn start_with_channel(
        &self,
    ) -> (
        thread::JoinHandle<()>,
        mpsc::Receiver<(StockQuote, SocketAddr)>,
    );

    fn send_ping(&self, serv_addr: &str) -> Result<(), Box<dyn std::error::Error>>;
}

impl Receiver for TickerReceiver {
    fn start_with_channel(
        &self,
    ) -> (
        thread::JoinHandle<()>,
        mpsc::Receiver<(StockQuote, std::net::SocketAddr)>,
    ) {
        TickerReceiver::start_with_channel(self)
    }

    fn send_ping(&self, serv_addr: &str) -> Result<(), Box<dyn std::error::Error>> {
        TickerReceiver::send_ping(self, serv_addr)
    }
}
