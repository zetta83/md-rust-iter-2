use std::fmt::{Display, Formatter};

#[derive(Debug, PartialEq)]
pub enum QuoteClientError {
    EmptyFile,
    IOError(String),
}

impl Display for QuoteClientError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmptyFile => write!(f, "Empty file"),
            Self::IOError(msg) => write!(f, "IO error: {}", msg),
        }
    }
}

impl std::error::Error for QuoteClientError {}

impl From<std::io::Error> for QuoteClientError {
    fn from(err: std::io::Error) -> Self {
        Self::IOError(err.to_string())
    }
}
