use std::net::{SocketAddr, TcpListener};

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
pub fn main() -> Result<()> {
    let cli: Cli = Cli::parse();
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

    let listener = TcpListener::bind(ip_port)?;

    for stream in listener.incoming(){
        // handle_connection(stream?);
    }

    Ok(())
}
