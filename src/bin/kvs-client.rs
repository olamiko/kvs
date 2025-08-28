use clap::Parser;
use kvs::{Result};
use kvs::{Commands, NetworkConnection};
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

    NetworkConnection::send_network_message(
        NetworkConnection::Request {
            command: cli.command,
        },
        &mut stream,
    )?;

    // Get response
    let buf = NetworkConnection::receive_network_message(&mut stream)?;
    let response = NetworkConnection::deserialize_message(buf)?;

    match response {
        NetworkConnection::Response { value } => {
            println!("{}", value);
        }
        NetworkConnection::Error { error } => {
            eprintln!("{}", error);
            exit(1);
        }
        NetworkConnection::Ok => (),
        _ => {
            println!("Unexpected from server: {:?}", response);
            exit(1);
        }
    }

    Ok(())
}
