use std::collections::HashSet;

const DEFAULT_TICKERS_RAW: &str = include_str!("data/tickers.txt");

static DEFAULT_TICKERS_SET: std::sync::OnceLock<HashSet<&'static str>> = std::sync::OnceLock::new();

fn get_default_tickers_set() -> &'static HashSet<&'static str> {
    DEFAULT_TICKERS_SET.get_or_init(|| get_default_tickers().collect())
}

pub fn get_default_tickers() -> impl Iterator<Item = &'static str> {
    DEFAULT_TICKERS_RAW.lines().filter(|line| !line.is_empty())
}

pub fn filter_tickers(tickers: &[String]) -> Vec<String> {
    let default_set = get_default_tickers_set();

    tickers
        .iter()
        .map(|t| t.to_uppercase())
        .filter(|t| default_set.contains(t.as_str()))
        .collect()
}

#[cfg(test)]
mod tests {
    use crate::default_tickers::{filter_tickers, get_default_tickers};

    #[test]
    fn test_get_default_tickers() {
        assert_eq!(get_default_tickers().next(), Some("AAPL"));
    }

    #[test]
    fn test_filter_tickers() {
        let tickers = vec![
            "aapl".to_string(),
            "INVALID".to_string(),
            "MSFT".to_string(),
        ];

        let filtered = filter_tickers(&tickers);

        assert_eq!(filtered.len(), 2);
        assert!(filtered.contains(&"AAPL".to_string()));
        assert!(filtered.contains(&"MSFT".to_string()));
        assert!(!filtered.contains(&"INVALID".to_string()));
    }
}
