const DEFAULT_TICKERS_RAW: &str = include_str!("data/tickers.txt");

pub fn get_default_tickers() -> impl Iterator<Item = &'static str> {
    DEFAULT_TICKERS_RAW.lines().filter(|line| !line.is_empty())
}

#[cfg(test)]
mod tests {
    use crate::default_tickers::get_default_tickers;

    #[test]
    fn test_get_default_tickers() {
        assert_eq!(get_default_tickers().next(), Some("AAPL"));
    }
}
