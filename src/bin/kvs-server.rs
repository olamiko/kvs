use slog::*;
use std::{
    io::{BufReader, Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
};

use clap::Parser;
use kvs::{KvsError, Result};
#[derive(Parser)]
#[command(version, about, propagate_version = true)]
struct Cli {
    #[arg(long, value_name = "ENGINE-NAME")]
    engine: Option<String>,
    #[arg(long, value_name = "IP:PORT")]
    addr: Option<String>,
}

fn setup_logging() -> Logger {
    let decorator = slog_term::TermDecorator::new().stderr().build();
    let drain = slog_term::CompactFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();

    return slog::Logger::root(drain, o!());
}

pub fn main() -> Result<()> {
    let cli: Cli = Cli::parse();
    // set up logging
    let log = setup_logging();

    info!(log, "Server Startup"; "Server Version Number" => env!("CARGO_PKG_VERSION"));

    let mut ip_port: SocketAddr = "127.0.0.1:4000".parse()?;
    let mut engine_name = "kvs";

    if let Some(ipaddr) = cli.addr.as_deref() {
        ip_port = ipaddr.parse()?;
    }
    if let Some(eng_name) = cli.engine.as_deref() {
        if eng_name == "kvs" || eng_name == "sled" {
            engine_name = eng_name;
        } else {
            return Err(KvsError::UnknownEngineType(eng_name.to_string()));
        }
    }

    info!(log, "Received Configuration"; "Engine name" => engine_name, "Ip Address and Port" => ip_port);
    let listener = TcpListener::bind(ip_port)?;

    for stream in listener.incoming() {
        info!(log, "Received a Connection");
        handle_connection(stream?, &log)?;
    }

    Ok(())
}

fn handle_connection(mut stream: TcpStream, log: &Logger) -> Result<()> {
    let mut buf_reader = BufReader::new(std::io::Read::by_ref(&mut stream));
    let mut buf = String::new();
    buf_reader.read_to_string(&mut buf)?;

    // let content = String::from_utf8_lossy(&buf).to_string();
    info!(log, "Received Content"; "stream content" => &buf);
    if buf == "TCP Handshake" {
        info!(log, "Received TCP Handshake");
        stream.write_all(b"Welcome to KVS")?;
    }
    Ok(())
}
