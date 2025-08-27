use clap::Parser;
use kvs::{receive_network_message, send_network_message, Result};
use kvs::{Commands, NetworkCommand};
use std::{
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

    send_network_message(
        NetworkCommand::Request {
            command: cli.command,
        },
        &mut stream,
    )?;

    // Get response
    let buf = receive_network_message(&mut stream)?;
    let response = NetworkCommand::deserialize_command(buf)?;

    match response {
        NetworkCommand::Response { value } => {
            println!("{}", value);
        }
        NetworkCommand::Error { error } => {
            println!("{}", error);
            exit(1);
        }
        NetworkCommand::Ok => (),
        _ => {
            println!("Unexpected from server: {:?}", response);
            exit(1);
        }
    }

    Ok(())
}
