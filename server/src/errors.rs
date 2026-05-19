use std::fmt::Display;

#[derive(Debug)]
pub enum StreamError {
    MissingAddress,
    MissingTickers,
    EmptyTickers,
    InvalidAddress(String),
    AddressAlreadyExists,
    ManagerLockFailed,
    // DoesNotExist,
    UnknownCommand,
}

#[derive(Debug)]
pub enum ServerError {
    Io(std::io::Error),
    Stream(StreamError),
}

impl std::error::Error for StreamError {}

impl std::error::Error for ServerError {}

impl From<std::io::Error> for ServerError {
    fn from(err: std::io::Error) -> Self {
        ServerError::Io(err)
    }
}

impl From<StreamError> for ServerError {
    fn from(err: StreamError) -> Self {
        ServerError::Stream(err)
    }
}

impl Display for StreamError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let message = match self {
            StreamError::MissingAddress => "ERROR: missing UDP address",
            StreamError::MissingTickers => "ERROR: missing tickers list",
            StreamError::EmptyTickers => "ERROR: empty tickers list",
            StreamError::InvalidAddress(addr) => {
                return write!(f, "ERROR: invalid UDP address: {}", addr);
            }
            StreamError::AddressAlreadyExists => "ERROR: stream already exists for this address",
            StreamError::ManagerLockFailed => "ERROR: failed to lock manager",
            // StreamError::DoesNotExist => "ERROR: stream does not exist",
            StreamError::UnknownCommand => "ERROR: unknown command",
        };
        write!(f, "{}", message)
    }
}

impl Display for ServerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ServerError::Io(e) => write!(f, "ERROR: IO error: {}", e),
            ServerError::Stream(e) => write!(f, "{}", e),
        }
    }
}
