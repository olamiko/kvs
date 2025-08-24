use clap::{Parser, Subcommand};
use kvs::{KvStore, KvsError, Result};
use serde::Serialize;
use std::{
    io::{Read, Write},
    net::{SocketAddr, TcpStream},
    path::Path,
};

use kvs::NetworkCommand;

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

// struct NetworkMessage {
//     command: String,
//     arguments: Vec<String>,
// }

// fn construct_bulk_strings(value: &str) -> String {
//     let mut message = String::new();
//     message.push('$');
//     message.push_str(&value.len().to_string());
//     message.push_str(value);
//     message.push_str("/r/n");

//     message
// }

// fn construct_network_message(network_message: NetworkMessage) -> String {
//     let mut message = String::new();
//     message.push('*');
//     message.push_str(&(&network_message.arguments.len() + 1).to_string());
//     message.push_str("/r/n");

//     message.push_str(&construct_bulk_strings(&network_message.command));
//     for arg in &network_message.arguments {
//         message.push_str(&construct_bulk_strings(arg));
//     }

//     message
// }

pub fn main() -> Result<()> {
    let cli: Cli = Cli::parse();
    let mut ip_port: SocketAddr = "127.0.0.1:4000".parse()?;

    if let Some(ipaddr) = cli.addr.as_deref() {
        ip_port = ipaddr.parse()?;
    }

    // Connect to server
    let mut stream = TcpStream::connect(ip_port)?;
    let mut store: KvStore = KvStore::open(Path::new(".")).unwrap();

    match &cli.command {
        Commands::Set { key, value } => {
            let message = NetworkCommand::Request {
                command: kvs::CommandType::Set {
                    key: key.to_string(),
                    value: value.to_string(),
                },
            }
            .serialize_command()?;
            stream.write_all(message.as_slice())?;

            // let mut buf = Vec::new();
            // stream.read_to_end(&mut buf)?;
            // let response = NetworkCommand::deserialize_command(buf)?;

            // if let NetworkCommand::Error { error } = response {
            //     println!("{}", error);
            //     // return Err(error);
            // }

            // if let Err(err) = store.set(key.to_string(), value.to_string()) {
            // println!("{}", err);
            // return Err(err);
            // }
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
