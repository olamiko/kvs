use clap::{Parser, Subcommand};
use kvs::KvStore;

#[derive(Parser)]
#[command(version, about, propagate_version = true)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Set { key: String, value: String },
    Get { key: String },
    Rm { key: String },
}

pub fn main() {
    let cli = Cli::parse();

    match &cli.command {
        Commands::Get { key } => panic!("unimplemented: exit code {}", 1),
        Commands::Set { key, value } => panic!("unimplemented: exit code {}", 1),
        Commands::Rm { key } => panic!("unimplemented: exit code {}", 1),
    }
}
