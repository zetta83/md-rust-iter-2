use clap::Parser;
use crate::errors::QuoteClientError;
use quote_libs::{info, init_logger};

mod errors;

#[derive(Parser)]
#[command(
    name = "quote_client",
    version = "0.0.1",
    about = "Client to query quotes"
)]
struct Args {
    /// Адрес и порт TCP-сервера
    #[arg(long)]
    server_addr: String,

    /// Порт для приёма UDP-данных
    #[arg(long)]
    udp_port: String,

    /// Путь к файлу со списком тикеров для подписки
    #[arg(long)]
    tickers_file: String,
}

// - Клиент принимает аргументы через clap: --server-addr, --udp-port, --tickers-file.
// - Файл тикеров читается построчно, игнорируются пустые строки и пробелы.
// - При отсутствии файла или ошибке чтения выводится понятная ошибка.
fn main() -> Result<(), QuoteClientError>{
    init_logger();
    info!("Starting quote client");
    
    let args = Args::parse();

    Ok(())
}
