use clap::Parser;
use kvs::Result;
use kvs::{Commands, NetworkCommand};
use std::{
    io::{Read, Write},
    net::{SocketAddr, TcpStream},
    process::exit,
};

#[derive(Parser)]
#[command(version, about, propagate_version = true)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
    #[arg(long, value_name = "IP:PORT", global = true)]
    addr: Option<String>,
}

pub fn main() -> Result<()> {
    let cli: Cli = Cli::parse();
    let mut ip_port: SocketAddr = "127.0.0.1:4000".parse()?;

    if let Some(ipaddr) = cli.addr.as_deref() {
        ip_port = ipaddr.parse()?;
    }

    // Connect to server
    let mut stream = TcpStream::connect(ip_port)?;

    let message = NetworkCommand::Request {
        command: cli.command,
    }
    .serialize_command()?;
    stream.write_all(message.as_slice())?;

    // Get response
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf)?;
    let response = NetworkCommand::deserialize_command(buf)?;

    match response {
        NetworkCommand::Response { value } => {
            println!("{}", value);
        }
        NetworkCommand::Error { error } => {
            println!("{}", error);
            exit(1);
        }
        _ => {
            println!("Unexpected from server: {:?}", response);
            exit(1);
        },
    }

    // How to check if there's a response and then read the response (Done)
    // Let the server always respond even if it is a simple OK :) (Done)
    Ok(())
    // match &cli.command {
    //     Commands::Set { key, value } => {
    //         let message = NetworkCommand::Request {
    //             command: Commands::Set {
    //                 key: key.to_string(),
    //                 value: value.to_string(),
    //             },
    //         }
    //         .serialize_command()?;
    //         stream.write_all(message.as_slice())?;

    //         // let mut buf = Vec::new();
    //         // stream.read_to_end(&mut buf)?;
    //         // let response = NetworkCommand::deserialize_command(buf)?;

    //         // if let NetworkCommand::Error { error } = response {
    //         //     println!("{}", error);
    //         //     // return Err(error);
    //         // }

    //         // if let Err(err) = store.set(key.to_string(), value.to_string()) {
    //         // println!("{}", err);
    //         // return Err(err);
    //         // }
    //         Ok(())
    //     }
    //     Commands::Get { key } => {
    //         let value = store.get(key.to_string());
    //         match value {
    //             Ok(val) => match val {
    //                 Some(val) => println!("{}", val),
    //                 None => println!("{}", KvsError::KeyDoesNotExist),
    //             },
    //             Err(err) => match err {
    //                 KvsError::KeyDoesNotExist => {
    //                     println!("{}", KvsError::KeyDoesNotExist);
    //                 }
    //                 _ => return Err(err),
    //             },
    //         }
    //         Ok(())
    //     }
    //     Commands::Rm { key } => {
    //         if let Err(err) = store.remove(key.to_string()) {
    //             println!("{}", err);
    //             return Err(err);
    //         }
    //         Ok(())
    //     }
    // }
}
