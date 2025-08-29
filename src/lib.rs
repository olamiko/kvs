#![warn(missing_docs)]

//! Implemtation for the kvs crate
pub use common::{get_current_engine,log_engine};
pub use common::{Commands, NetworkConnection};
pub use error::KvsError;
pub use kvs::{KvStore, KvsEngine, Result};

mod common;
mod engine;
mod error;
mod kvs;
