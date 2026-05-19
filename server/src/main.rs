mod clients;
mod errors;
mod handler;
mod sender;

use crate::clients::StreamManager;
use crate::errors::ServerError;
use crate::handler::handler;
use quote_libs::{error, info, init_logger};
use std::net::TcpListener;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

fn main() -> Result<(), ServerError> {
    init_logger();

    let listener = TcpListener::bind("127.0.0.1:7878")?;
    info!("Listening on 127.0.0.1:7878");

    let stream_manager = StreamManager::new().map_err(|e| {
        error!("{}", e);
        ServerError::from(e)
    })?;

    let manager = Arc::new(Mutex::new(stream_manager));

    let cleanup_manager = Arc::clone(&manager);
    thread::spawn(move || {
        let timeout_seconds = 5;
        let interval_seconds = 2;

        loop {
            thread::sleep(Duration::from_secs(interval_seconds));

            if let Ok(mut manager) = cleanup_manager.lock() {
                if let Err(e) = manager.cleanup_expired_streams(timeout_seconds) {
                    error!("Cleanup failed: {}", e);
                }
            } else {
                error!("Failed to lock stream manager for cleanup");
            }
        }
    });

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let manager = Arc::clone(&manager);

                thread::spawn(move || {
                    if let Err(e) = handler(stream, manager) {
                        error!("{}", e);
                    }
                });
            }
            Err(e) => {
                error!("Failed to accept client connection: {}", e);
            }
        }
    }

    Ok(())
}
