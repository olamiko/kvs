// This module specifies common functions between server and clients. This has to do with the serialization protocol for the network system
// Our KVS supports only 3 commands i.e., set k v, get k, rm k; All the elements are strings. So we will use an enum to represent and then we can serialize / deserialize that

use std::{
    io::{BufRead, BufReader, Read, Write},
    net::TcpStream,
};

use crate::Result;
use clap::Subcommand;
use serde::{Deserialize, Serialize};

#[derive(Subcommand, Debug, Serialize, Deserialize)]
pub enum Commands {
    Set { key: String, value: String },
    Get { key: String },
    Rm { key: String },
}

#[derive(Debug, Serialize, Deserialize)]
pub enum NetworkCommand {
    Request { command: Commands },
    Response { value: String },
    Error { error: String },
    Ok,
}

// impl slog::Value for NetworkCommand {
//     fn serialize(
//         &self,
//         record: &slog::Record,
//         key: slog::Key,
//         serializer: &mut dyn slog::Serializer,
//     ) -> slog::Result {
//         unimplemented!()
//     }
// }

impl NetworkCommand {
    /// Returns the serialize command of this [`NetworkCommand`].
    ///
    /// # Errors
    ///
    /// This function will return an error if .
    pub fn serialize_command(&self) -> Result<Vec<u8>> {
        let mut s = flexbuffers::FlexbufferSerializer::new();
        self.serialize(&mut s)?;
        Ok(s.take_buffer())
    }

    /// .
    ///
    /// # Errors
    ///
    /// This function will return an error if .
    pub fn deserialize_command(buf: Vec<u8>) -> Result<NetworkCommand> {
        let r = flexbuffers::Reader::get_root(buf.as_slice())?;
        Ok(NetworkCommand::deserialize(r)?)
    }
}

/// .
///
/// # Errors
///
/// This function will return an error if .
pub fn send_network_message(network_command: NetworkCommand, stream: &mut TcpStream) -> Result<()> {
    let message = network_command.serialize_command()?;
    stream.write_all(&message.len().to_le_bytes())?;
    stream.write_all(b"\n")?;
    stream.write_all(network_command.serialize_command()?.as_slice())?;
    stream.flush()?;
    Ok(())
}

/// .
///
/// # Panics
///
/// Panics if .
///
/// # Errors
///
/// This function will return an error if .
pub fn receive_network_message(stream: &mut TcpStream) -> Result<Vec<u8>> {
    let mut buf_reader = BufReader::new(stream);
    let mut buf: Vec<u8> = Vec::new();
    buf_reader.read_until(b'\n', &mut buf)?;
    let content_size = usize::from_le_bytes(buf.trim_ascii().try_into().unwrap());
    let mut content_buf = vec![0u8; content_size];
    buf_reader.read_exact(&mut content_buf)?;
    Ok(content_buf)
}
