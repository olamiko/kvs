use clap::Parser;
use kvs::{Commands, KvStore, KvsError, NetworkConnection, Result};
use slog::*;
use std::{
    net::{SocketAddr, TcpListener, TcpStream},
    path::Path,
};

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

    slog::Logger::root(drain, o!())
}

pub fn main() -> Result<()> {
    let cli: Cli = Cli::parse();
    // Open store
    let mut store: KvStore = KvStore::open(Path::new(".")).unwrap();

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
        handle_request(stream?, &mut store, &log)?;
    }

    Ok(())
}

fn handle_request(mut stream: TcpStream, store: &mut KvStore, log: &Logger) -> Result<()> {
    let buf = NetworkConnection::receive_network_message(&mut stream)?;

    let message = NetworkConnection::deserialize_message(buf)?;

    info!(log, "Parsing a network message");
    if let NetworkConnection::Request { command } = message {
        match command {
            Commands::Get { key } => {
                let value = store.get(key);
                match value {
                    Ok(val) => match val {
                        Some(val) => NetworkConnection::send_network_message(
                            NetworkConnection::Response { value: val },
                            &mut stream,
                        )?,
                        None => NetworkConnection::send_network_message(
                            NetworkConnection::Response {
                                value: KvsError::KeyDoesNotExist.to_string(),
                            },
                            &mut stream,
                        )?,
                    },
                    Err(err) => NetworkConnection::send_network_message(
                        NetworkConnection::Error {
                            error: err.to_string(),
                        },
                        &mut stream,
                    )?,
                }
            }
            Commands::Set { key, value } => {
                if let Err(err) = store.set(key, value) {
                    NetworkConnection::send_network_message(
                        NetworkConnection::Error {
                            error: err.to_string(),
                        },
                        &mut stream,
                    )?
                }
                NetworkConnection::send_network_message(NetworkConnection::Ok, &mut stream)?
            }
            Commands::Rm { key } => {
                if let Err(err) = store.remove(key) {
                    NetworkConnection::send_network_message(
                        NetworkConnection::Error {
                            error: err.to_string(),
                        },
                        &mut stream,
                    )?
                }
                NetworkConnection::send_network_message(NetworkConnection::Ok, &mut stream)?
            }
        }
    } // Drop any other network command type sent to server silently

    Ok(())
}
