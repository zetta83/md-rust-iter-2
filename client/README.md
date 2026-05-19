## Клиент котировок (Quote Client)

пример запуска:
```terminaloutput
RUST_LOG=debug cargo run --bin client --features logging -- --server-addr 127.0.0.1:7878 --udp-port 54323  --tickers-file ./tmp.txt
```
