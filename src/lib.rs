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
use walkdir::DirEntry;
use walkdir::WalkDir;

/// Compaction Threshold
const DEFRAGMENTATION_FACTOR: u8 = 3;

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
    /// WalkDir Error
    DirectoryWalk(walkdir::Error),
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
            KvsError::DirectoryWalk(ref err) => write!(f, "Walkdir error: {}", err),
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

impl From<walkdir::Error> for KvsError {
    fn from(err: walkdir::Error) -> Self {
        KvsError::DirectoryWalk(err)
    }
}

/// The store for kvs crate
pub struct KvStore {
    elements: HashMap<String, u64>,
    write_file_handle: File,
    read_file_handle: BufReader<File>,
    stale_entries: u64, //(stale entries, total entries). The division of total entries / stale entries must not be greater than defragmentation threshold. if it is, call a compaction!
                        // Add latest log number entry
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
        let mut stale_entries: u64 = 0;

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
            let kvslogline = KvStore::deserialize_from_log(&mut buf_reader)?;

            match kvslogline {
                KvsLogLine::Set { key, .. } => {
                    if index.contains_key(&key) {
                        stale_entries += 1;
                    }
                    index.insert(key, data_offset);
                }
                KvsLogLine::Rm { key } => {
                    index.remove(&key);
                    stale_entries += 2;
                }
            }
        }

        let w = OpenOptions::new().append(true).open(&filepath)?;
        buf_reader.rewind()?;

        // pass the file handle into the KvStore and return
        Ok(KvStore {
            elements: index,
            write_file_handle: w,
            read_file_handle: buf_reader,
            stale_entries: stale_entries,
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
        self.read_file_handle.seek(io::SeekFrom::Start(offset))?;

        let kvslogline = KvStore::deserialize_from_log(&mut self.read_file_handle)?;
        if let KvsLogLine::Set { key, value } = kvslogline {
            return Ok(Some(value));
        }
        Ok(None)
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

        let data_offset = self.serialize_to_log(logline)?;

        // place the element in the index
        if self.elements.contains_key(&key) {
            self.stale_entries += 1;
        }
        self.elements.insert(key, data_offset);
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

        let _ = self.serialize_to_log(logline);

        // remove the element from the index
        self.elements.remove(&key);
        self.stale_entries += 2;
        Ok(())
    }

    fn serialize_to_log(&mut self, logline: KvsLogLine) -> Result<u64> {
        self.write_file_handle.seek(io::SeekFrom::End(0))?;
        let data_offset = self.write_file_handle.stream_position()?;

        let mut s = flexbuffers::FlexbufferSerializer::new();
        logline.serialize(&mut s)?;

        // serialize to the log
        let size: u32 = s.view().len().try_into().unwrap();
        self.write_file_handle.write_all(&(size.to_le_bytes()))?;
        self.write_file_handle
            .write_all(s.take_buffer().as_slice())?;
        Ok(data_offset)
    }

    fn deserialize_from_log(buf_reader: &mut BufReader<File>) -> Result<KvsLogLine> {
        let mut buffer = [0u8; 4];
        buf_reader.read_exact(&mut buffer)?;
        let size = u32::from_le_bytes(buffer).try_into()?;
        let mut logline = vec![0u8; size];
        buf_reader.read_exact(&mut logline)?;
        let r = flexbuffers::Reader::get_root(logline.as_slice())?;
        let kvslogline = KvsLogLine::deserialize(r)?;
        Ok(kvslogline)
    }
}
