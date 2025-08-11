use std::num::TryFromIntError;
use std::{fmt, error, io};

/// Error enum for kvs crate
#[derive(Debug)]
pub enum KvsError {
    /// IO variant for kvs crate
    Io(std::io::Error),
    /// Serialization error variant for kvs crate
    Serializer(flexbuffers::SerializationError),
    /// Deserialization error variant for kvs crate
    Deserializer(flexbuffers::DeserializationError),
    /// Reader error variant for kvs crate
    Reader(flexbuffers::ReaderError),
    /// Key does not exist error variant for kvs crate
    KeyDoesNotExist,
    /// Int conversion error variant for kvs crate
    TryFromInt(TryFromIntError),
}

impl fmt::Display for KvsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            KvsError::Io(ref err) => write!(f, "IO error: {}", err),
            KvsError::Serializer(ref err) => write!(f, "Serialization error: {}", err),
            KvsError::Reader(ref err) => write!(f, "Reader error: {}", err),
            KvsError::Deserializer(ref err) => write!(f, "Deserialization error: {}", err),
            KvsError::TryFromInt(ref err) => write!(f, "Deserialization error: {}", err),
            KvsError::KeyDoesNotExist => {
                write!(f, "Key not found")
            },
        }
    }
}

impl error::Error for KvsError {}

impl From<io::Error> for KvsError {
    fn from(err: io::Error) -> Self {
        KvsError::Io(err)
    }
}

impl From<flexbuffers::SerializationError> for KvsError {
    fn from(err: flexbuffers::SerializationError) -> Self {
        KvsError::Serializer(err)
    }
}

impl From<flexbuffers::DeserializationError> for KvsError {
    fn from(err: flexbuffers::DeserializationError) -> Self {
        KvsError::Deserializer(err)
    }
}

impl From<flexbuffers::ReaderError> for KvsError {
    fn from(err: flexbuffers::ReaderError) -> Self {
        KvsError::Reader(err)
    }
}

impl From<TryFromIntError> for KvsError {
    fn from(err: TryFromIntError) -> Self {
        KvsError::TryFromInt(err)
    }
}