use clap::{Parser, Subcommand};
use kvs::{KvStore, KvsError, Result};
use std::{
    io::{Read, Write}, net::{SocketAddr, TcpStream}, path::Path
};

#[derive(Parser)]
#[command(version, about, propagate_version = true)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
    #[arg(long, value_name = "IP:PORT", global = true)]
    addr: Option<String>,
}

#[derive(Subcommand)]
enum Commands {
    Set { key: String, value: String },
    Get { key: String },
    Rm { key: String },
}

pub fn main() -> Result<()> {
    let cli: Cli = Cli::parse();
    let mut ip_port: SocketAddr = "127.0.0.1:4000".parse()?;

    if let Some(ipaddr) = cli.addr.as_deref() {
        ip_port = ipaddr.parse()?;
    }

    // Connect to server
    let mut stream = TcpStream::connect(ip_port)?;
    stream.write_all(b"TCP Handshake")?;

    let mut store: KvStore = KvStore::open(Path::new(".")).unwrap();

    match &cli.command {
        Commands::Set { key, value } => {
            if let Err(err) = store.set(key.to_string(), value.to_string()) {
                println!("{}", err);
                return Err(err);
            }
            Ok(())
        }
        Commands::Get { key } => {
            let value = store.get(key.to_string());
            match value {
                Ok(val) => match val {
                    Some(val) => println!("{}", val),
                    None => println!("{}", KvsError::KeyDoesNotExist),
                },
                Err(err) => match err {
                    KvsError::KeyDoesNotExist => {
                        println!("{}", KvsError::KeyDoesNotExist);
                    }
                    _ => return Err(err),
                },
            }
            Ok(())
        }
        Commands::Rm { key } => {
            if let Err(err) = store.remove(key.to_string()) {
                println!("{}", err);
                return Err(err);
            }
            Ok(())
        }
    }
}
