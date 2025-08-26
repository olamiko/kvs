use clap::Parser;
use kvs::{Commands, KvStore, KvsError, NetworkCommand, Result};
use slog::*;
use std::{
    io::{BufReader, Read, Write},
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

    return slog::Logger::root(drain, o!());
}

fn server_response(network_command: NetworkCommand, stream: &mut TcpStream) -> Result<()> {
    stream.write_all(network_command.serialize_command()?.as_slice());
    Ok(())
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
    let mut buf_reader = BufReader::new(std::io::Read::by_ref(&mut stream));
    let mut buf = Vec::new();
    buf_reader.read_to_end(&mut buf)?;
    println!("{:?}", buf);

    let message = NetworkCommand::deserialize_command(buf)?;
    if let NetworkCommand::Request { command } = message {
        match command {
            Commands::Get { key } => {
                let value = store.get(key);
                match value {
                    Ok(val) => match val {
                        Some(val) => {
                            server_response(NetworkCommand::Response { value: val }, &mut stream)?
                        }
                        None => server_response(
                            NetworkCommand::Error {
                                error: KvsError::KeyDoesNotExist.to_string(),
                            },
                            &mut stream,
                        )?,
                    },
                    Err(err) => match err {
                        KvsError::KeyDoesNotExist => server_response(
                            NetworkCommand::Response {
                                value: "".to_string(),
                            },
                            &mut stream,
                        )?,
                        _ => server_response(
                            NetworkCommand::Error {
                                error: err.to_string(),
                            },
                            &mut stream,
                        )?,
                    },
                }
            }
            Commands::Set { key, value } => {
                if let Err(err) = store.set(key, value) {
                    server_response(
                        NetworkCommand::Error {
                            error: err.to_string(),
                        },
                        &mut stream,
                    )?
                } else {
                    server_response(
                        NetworkCommand::Response {
                            value: "".to_string(),
                        },
                        &mut stream,
                    )?
                }
            }
            Commands::Rm { key } => {
                if let Err(err) = store.remove(key) {
                    server_response(
                        NetworkCommand::Error {
                            error: err.to_string(),
                        },
                        &mut stream,
                    )?
                } else {
                    server_response(
                        NetworkCommand::Response {
                            value: "".to_string(),
                        },
                        &mut stream,
                    )?
                }
            }
        }
    } else {
        // Send server response that we don't know what is meant
    }

    // Do I need a connection statement to show that all went well?
    // Do I want to keep the client connection open for reuse? (No)
    Ok(())
}
