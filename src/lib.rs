#![warn(missing_docs)]

//! Implemtation for the kvs crate
pub use error::{KvsError};
pub use kvs::{KvStore, KvsEngine, Result};

mod error;
mod kvs;