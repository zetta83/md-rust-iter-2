use crate::receiver::Receiver;
use quote_libs::{debug, error};
use std::error::Error;
use std::sync::Arc;
use std::{thread, time};

pub struct PingSender {
    socket: Arc<dyn Receiver>,
    heartbeat_is_running: bool,
}

impl PingSender {
    pub fn new(socket: Arc<dyn Receiver>) -> Self {
        Self {
            socket,
            heartbeat_is_running: false,
        }
    }

    pub fn start_heartbeat(&mut self, server_addr: &str) -> Result<(), Box<dyn Error>> {
        if self.heartbeat_is_running {
            return Ok(());
        }

        self.heartbeat_is_running = true;
        let socket = Arc::clone(&self.socket);
        let server_addr = server_addr.to_string();

        thread::spawn(move || {
            loop {
                match socket.send_ping(&server_addr) {
                    Ok(_) => {
                        debug!("PING send to {}", server_addr);
                    }
                    Err(e) => {
                        error!("Failed send PING: {}", e);
                        break;
                    }
                }
                thread::sleep(time::Duration::from_secs(2));
            }
        });

        Ok(())
    }

    // pub fn heartbeat_is_running(&self) -> bool {
    //     self.heartbeat_is_running
    // }
}
