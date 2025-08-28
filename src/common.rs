// This module specifies common functions between server and clients. This has to do with the serialization protocol for the network system
// Our KVS supports only 3 commands i.e., set k v, get k, rm k; All the elements are strings. So we will use an enum to represent and then we can serialize / deserialize that

use std::{
    io::{BufRead, BufReader, Read, Write},
    net::TcpStream,
};

use crate::Result;
use clap::Subcommand;
use serde::{Deserialize, Serialize};

/// Enums describing the commands supported by the KVS
#[derive(Subcommand, Debug, Serialize, Deserialize)]
pub enum Commands {
    /// Sets the value of a key in the database
    Set { key: String, value: String },
    /// Gets the value of a key from the database
    Get { key: String },
    /// Removes the key from the database
    Rm { key: String },
}

/// Describes the type of message that can be sent or received from the stream
#[derive(Debug, Serialize, Deserialize)]
pub enum NetworkConnection {
    /// A message request usually sent by the client
    Request { command: Commands },
    /// A message response containing a `value`
    Response { value: String },
    /// A message signaling an error
    Error { error: String },
    /// A message response signalling that the request was handled  
    Ok,
}

impl NetworkConnection {
    /// Returns the serialized message of this [`NetworkConnection`].
    ///
    /// # Errors
    ///
    /// This function will return an error if the serialization fails
    pub fn serialize_message(&self) -> Result<Vec<u8>> {
        let mut s = flexbuffers::FlexbufferSerializer::new();
        self.serialize(&mut s)?;
        Ok(s.take_buffer())
    }

    /// Returns the NetworkConnection enum from a vector of bytes
    ///
    /// # Errors
    ///
    /// This function will return an error if deserialization fails
    pub fn deserialize_message(buf: Vec<u8>) -> Result<NetworkConnection> {
        let r = flexbuffers::Reader::get_root(buf.as_slice())?;
        Ok(NetworkConnection::deserialize(r)?)
    }

    /// Serializes a message and sends it into a stream
    ///
    /// # Errors
    ///
    /// This function will return an error if the serialization fails
    /// or writing to the TcpStream fails
    pub fn send_network_message(
        network_connection: NetworkConnection,
        stream: &mut TcpStream,
    ) -> Result<()> {
        let message = network_connection.serialize_message()?;
        stream.write_all(&message.len().to_le_bytes())?;
        stream.write_all(b"\n")?;
        stream.write_all(network_connection.serialize_message()?.as_slice())?;
        stream.flush()?;
        Ok(())
    }

    /// Receives a message from a TcpStream
    ///
    /// # Errors
    ///
    /// This function will return an error if reading from the buffer fails
    pub fn receive_network_message(stream: &mut TcpStream) -> Result<Vec<u8>> {
        let mut buf_reader = BufReader::new(stream);
        let mut buf: Vec<u8> = Vec::new();
        buf_reader.read_until(b'\n', &mut buf)?;
        let content_size = usize::from_le_bytes(buf.trim_ascii().try_into().unwrap());
        let mut content_buf = vec![0u8; content_size];
        buf_reader.read_exact(&mut content_buf)?;
        Ok(content_buf)
    }
}
