use quote_libs::{StockQuote, debug, error, info};
use std::collections::{HashMap, HashSet};
use std::net::UdpSocket;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

pub struct TickersSender {
    socket: Arc<UdpSocket>,
    should_be_stopped: Arc<Mutex<HashSet<String>>>,
}

impl TickersSender {
    pub fn new(socket: Arc<UdpSocket>) -> Result<Self, std::io::Error> {
        info!("TickerSender created");

        Ok(Self {
            socket,
            should_be_stopped: Arc::new(Mutex::new(HashSet::new())),
        })
    }

    pub fn send_to(
        &self,
        ticker_data: &StockQuote,
        target_addr: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.socket.send_to(&ticker_data.to_bytes(), target_addr)?;
        Ok(())
    }

    pub fn stop_broadcasting(
        &self,
        target_addr: &str,
    ) -> Result<(), Box<dyn std::error::Error + '_>> {
        let mut setter = self.should_be_stopped.lock()?;
        setter.insert(target_addr.to_string());
        Ok(())
    }

    pub fn start_broadcasting(
        &self,
        target_addr: &str,
        tickers: &[String],
        map_tickers: &Arc<Mutex<HashMap<String, StockQuote>>>,
        interval_ms: u64,
    ) -> Result<(), Box<dyn std::error::Error + '_>> {
        loop {
            {
                let mut setter = self.should_be_stopped.lock()?;
                if setter.get(target_addr).is_some() {
                    setter.remove(target_addr);
                    return Ok(());
                }
            }

            match map_tickers.lock() {
                Ok(map_tickers) => {
                    for ticker in tickers.iter() {
                        if let Some(v) = map_tickers.get(ticker) {
                            match self.send_to(v, target_addr) {
                                Ok(_) => {
                                    debug!("Ticker sent to {}", target_addr);
                                }
                                Err(err) => {
                                    error!("Failed to send ticker to {}: {}", target_addr, err);
                                }
                            }
                        }
                    }
                }
                Err(err) => {
                    error!("TickersSender::start_broadcasting error: {}", err);
                }
            }

            thread::sleep(Duration::from_millis(interval_ms));
        }
    }
}
