pub use log::{debug, error, info, trace, warn};

#[cfg(feature = "logging")]
pub fn init_logger() {
    use env_logger;

    env_logger::init();
    info!("Logger initialized");
}

#[cfg(not(feature = "logging"))]
pub fn init_logger() {}
