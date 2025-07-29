#![warn(missing_docs)]

//! Implemtation for the kvs crate

use flexbuffers::SerializationError;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::fs::OpenOptions;
use std::io::prelude::*;
use std::io::BufReader;
use std::num::TryFromIntError;
use std::path::PathBuf;
use std::{collections::HashMap, path, result};
use std::{error, fmt, io};

/// Result type for the kvs crate
pub type Result<T> = result::Result<T, KvsError>;

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
            }
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

/// The store for kvs crate
pub struct KvStore {
    elements: HashMap<String, u64>,
    writeFileHandle: File,
    readFileHandle: BufReader<File>,
    lastWriteOffset: u64,
}

/// The command set for serialization and storage
#[derive(Debug, Serialize, Deserialize)]
enum KvsLogLine {
    Set { key: String, value: String },
    Rm { key: String },
}

impl KvStore {
    /// ```
    /// # use kvs::KvStore;
    /// #
    /// # fn main() {
    /// let mut store = KvStore::new();
    /// # }
    /// ```
    // pub fn new() -> Self {
    //     let f = File::open(path)
    //     KvStore {
    //         elements: HashMap::new(),
    //         fileHandle: None,
    //     }
    // }

    pub fn open(path: &path::Path) -> Result<Self> {
        let mut index: HashMap<String, u64> = HashMap::new();
        let mut filepath: PathBuf = PathBuf::from(path);

        // Open file handle for reading
        if path.is_dir() {
            filepath.push("kvs.log");
        }

        let f = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(&filepath)?;
        let mut buf_reader = BufReader::new(f);

        loop {
            if buf_reader.fill_buf()?.is_empty() {
                break;
            }
            
            let data_offset = buf_reader.stream_position()?;
            // println!("(open) data offset is {:?}", data_offset);
            
            let mut buffer = [0u8; 4];
            buf_reader.read_exact(&mut buffer)?;
            let size = u32::from_le_bytes(buffer).try_into()?;
            let mut logline = vec![0u8; size];
            buf_reader.read_exact(&mut logline)?;
            let r = flexbuffers::Reader::get_root(logline.as_slice())?;
            let kvslogline = KvsLogLine::deserialize(r)?;

            match kvslogline {
                KvsLogLine::Set { key,.. } => {
                    index.insert(key, data_offset);
                }
                KvsLogLine::Rm { key } => {
                    index.remove(&key);
                }
            }
        }

        let w = OpenOptions::new().append(true).open(&filepath)?;
        let last_offset = buf_reader.stream_position()?;
        buf_reader.rewind()?;
        // pass the file handle into the KvStore and return
        Ok(KvStore {
            elements: index,
            writeFileHandle: w,
            readFileHandle: buf_reader,
            lastWriteOffset: last_offset, 
        })
    }

    /// ```
    /// # use kvs::KvStore;
    /// #
    /// # fn main() {
    /// # let mut store = KvStore::new();
    /// # store.set("name".to_string(), "olamide".to_string());
    /// assert_eq!(store.get("name".to_string())?, Some("olamide".to_string()));
    /// # }
    /// ```
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        let data_offset = self.elements.get(&key);
        if let None = data_offset {
            return Ok(None);
        }

        // Unwrapping here since we can be sure `data_offset` isn't None
        let offset = data_offset.unwrap().clone(); 
        
        self.readFileHandle.seek(io::SeekFrom::Start(offset))?;
        let mut buffer = [0u8; 4];
        self.readFileHandle.read_exact(&mut buffer)?;

        let size = u32::from_le_bytes(buffer).try_into().unwrap();

        // println!("size in get is {}", size);
        let mut logline = vec![0u8; size];
        self.readFileHandle.read_exact(&mut logline)?;
        let r = flexbuffers::Reader::get_root(logline.as_slice())?;
        let kvslogline = KvsLogLine::deserialize(r)?;
        if let KvsLogLine::Set { key, value } = kvslogline {
            return Ok(Some(value));
        }
        return Ok(None);
    }

    /// ```
    /// # use kvs::KvStore;
    /// #
    /// # fn main() {
    /// # let mut store = KvStore::new();
    /// store.set("name".to_string(), "olamide".to_string());
    /// assert_eq!(store.get("name".to_string())?, Some("olamide".to_string()));
    /// # }
    /// ```
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let logline = KvsLogLine::Set {
            key: key.clone(),
            value: value.clone(),
        };
        let mut s = flexbuffers::FlexbufferSerializer::new();
        logline.serialize(&mut s)?;
        
        let data_offset = self.lastWriteOffset;
        self.writeFileHandle.seek(io::SeekFrom::Start(data_offset))?;
        // println!("(set) data offset is {:?}", data_offset);
        
        // serialize to the log
        let size: u32 = s.view().len().try_into()?;
        self.writeFileHandle.write_all(&(size.to_le_bytes()))?;
        self.writeFileHandle.write_all(s.take_buffer().as_slice())?;

        // place the element in the index
        self.elements.insert(key, data_offset);
        self.lastWriteOffset = self.writeFileHandle.stream_position()?;
        Ok(())
    }

    /// ```
    /// # use kvs::KvStore;
    /// #
    /// # fn main() {
    /// # let mut store = KvStore::new();
    /// # store.set("name".to_string(), "olamide".to_string());
    /// store.remove("name".to_string());
    /// # assert_eq!(store.get("name".to_string())?, None);
    /// # }
    /// ```
    pub fn remove(&mut self, key: String) -> Result<()> {
        // Assert the key is in the index
        if !self.elements.contains_key(&key) {
            return Err(KvsError::KeyDoesNotExist);
        }

        let logline = KvsLogLine::Rm { key: key.clone() };
        let mut s = flexbuffers::FlexbufferSerializer::new();

        logline.serialize(&mut s)?;

        // serialize to the log
        let size: u32 = s.view().len().try_into().unwrap();
        self.writeFileHandle.write_all(&(size.to_le_bytes()))?;
        self.writeFileHandle.write_all(s.take_buffer().as_slice())?;

        // remove the element from the index
        self.elements.remove(&key);
        self.lastWriteOffset = self.writeFileHandle.stream_position()?;
        Ok(())
    }
}
