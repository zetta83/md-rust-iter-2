use std::collections::HashMap;
use std::fmt::Display;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone)]
pub struct StockQuote {
    pub ticker: String,
    pub price: f64,
    pub volume: u32,
    pub timestamp: u64,
}

// Методы для сериализации/десериализации
impl StockQuote {
    pub fn from_string(s: &str) -> Option<Self> {
        let parts: Vec<&str> = s.split("|").collect();

        if parts.len() == 4 {
            Some(StockQuote {
                ticker: parts[0].to_string(),
                price: parts[1].parse().ok()?,
                volume: parts[2].parse().ok()?,
                timestamp: parts[3].parse().ok()?,
            })
        } else {
            None
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        // note: плохо если вам нужна экстремальная производительность (миллионы операций в секунду)
        // self.to_string().into_bytes()
        // note: хорошо - ручная сборка байтов
        let mut bytes = Vec::new();
        bytes.extend_from_slice(self.ticker.as_bytes());
        bytes.push(b'|');
        bytes.extend_from_slice(self.price.to_string().as_bytes());
        bytes.push(b'|');
        bytes.extend_from_slice(self.volume.to_string().as_bytes());
        bytes.push(b'|');
        bytes.extend_from_slice(self.timestamp.to_string().as_bytes());
        bytes
    }
}

impl Display for StockQuote {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}|{}|{}|{}",
            self.ticker, self.price, self.volume, self.timestamp
        )
    }
}

pub struct QuoteGenerator {
    last_price: Arc<Mutex<HashMap<String, f64>>>,
}

impl Default for QuoteGenerator {
    fn default() -> Self {
        Self::new()
    }
}

impl QuoteGenerator {
    pub fn new() -> Self {
        Self {
            last_price: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn generator_quote(&self, ticker: &str) -> Option<StockQuote> {
        let last_price = {
            let mut last_prices = self.last_price.lock().ok()?;

            if let Some(&price) = last_prices.get(ticker) {
                let change_percent = (rand::random::<f64>() - 0.5) * 0.02;
                let new_price = price * (1.0 + change_percent);

                let rounded_price = (new_price * 100.0).round() / 100.0;

                last_prices.insert(ticker.to_string(), rounded_price);

                rounded_price
            } else {
                let initial_price = (rand::random::<f64>() * 990.0) + 10.0;
                let rounded_initial = (initial_price * 100.0).round() / 100.0;

                last_prices.insert(ticker.to_string(), rounded_initial);

                rounded_initial
            }
        };

        let volume = match ticker {
            // Популярные акции имеют больший объём
            "AAPL" | "MSFT" | "TSLA" => 1_000 + (rand::random::<f64>() * 5_000.0) as u32,
            // Обычные акции - средний объём
            _ => (rand::random::<f64>() * 1_000.0) as u32,
        };

        let timestamp = Self::get_current_timestamp()?;

        Some(StockQuote {
            ticker: ticker.to_string(),
            price: last_price,
            volume,
            timestamp,
        })
    }

    pub fn generator_quotes(&self, tickers: &[&str]) -> Option<Vec<StockQuote>> {
        tickers
            .iter()
            .map(|&ticker| self.generator_quote(ticker))
            .collect()
    }

    fn get_current_timestamp() -> Option<u64> {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .ok()
            .map(|d| d.as_millis() as u64)
    }
}
