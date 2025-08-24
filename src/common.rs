// This module specifies common functions between server and clients. This has to do with the serialization protocol for the network system
// Our KVS supports only 3 commands i.e., set k v, get k, rm k; All the elements are strings. So we will use an enum to represent and then we can serialize / deserialize that

use crate::{KvsError, Result};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub enum CommandType {
    Get { key: String },
    Set { key: String, value: String },
    Rm { key: String },
}

#[derive(Debug, Serialize, Deserialize)]
pub enum NetworkCommand {
    Request { command: CommandType },
    Response { value: String },
    Error { error: String },
}

impl slog::Value for NetworkCommand {
    fn serialize(
        &self,
        record: &slog::Record,
        key: slog::Key,
        serializer: &mut dyn slog::Serializer,
    ) -> slog::Result {
        unimplemented!()
    }
}

impl NetworkCommand {
    pub fn serialize_command(&self) -> Result<Vec<u8>> {
        let mut s = flexbuffers::FlexbufferSerializer::new();
        self.serialize(&mut s)?;
        Ok(s.take_buffer())
    }

    pub fn deserialize_command(buf: Vec<u8>) -> Result<NetworkCommand> {
        let r = flexbuffers::Reader::get_root(buf.as_slice())?;
        Ok(NetworkCommand::deserialize(r)?)
    }
}
