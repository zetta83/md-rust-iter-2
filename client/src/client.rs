use quote_libs::{debug, info};
use std::io;
use std::io::{BufRead, BufReader, Write};
use std::net::TcpStream;

// pub fn connect() -> io::Result<(TcpStream, BufReader<TcpStream>)> {
//     let socket = Socket::new(Domain::IPV4, Type::STREAM, None)?;
// }

pub struct QuoteClient {
    server_tcp_addr: String,
    udp_port: String,
    tickers: Vec<String>,
}

impl QuoteClient {
    pub fn new(server_tcp_addr: &str, udp_port: &str, tickers: &[String]) -> Self {
        Self {
            server_tcp_addr: server_tcp_addr.to_string(),
            udp_port: udp_port.to_string(),
            tickers: tickers.to_vec(),
        }
    }

    pub fn send_command_create_stream(&self) -> io::Result<()> {
        let mut stream = TcpStream::connect(self.server_tcp_addr.clone())?;
        let mut reader = BufReader::new(stream.try_clone()?);

        for _ in 0..1 {
            let mut line = String::new();
            reader.read_line(&mut line)?;
            info!("{}", line);
        }

        debug!("Calling STREAM cmd");

        stream.write_all(
            format!(
                "STREAM udp://127.0.0.1:{} {}",
                self.udp_port,
                self.tickers.join(",")
            )
            .as_bytes(),
        )?;
        stream.write_all(b"\n")?;
        stream.flush()?;

        let mut buffer = String::new();
        let bytes = reader.read_line(&mut buffer)?;

        if bytes == 0 {
            info!("Server closed connection");
            return Ok(());
        }

        info!("{}", buffer);

        Ok(())
    }
}
