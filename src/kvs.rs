
use crate::error::KvsError;

use serde::{Deserialize, Serialize};
use std::fs;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::prelude::*;
use std::io::BufReader;
use std::io::BufWriter;
use std::path::Path;
use std::path::PathBuf;
use std::{collections::HashMap, path, result};
use std::{error, fmt, io};

/// Result type for the kvs crate
pub type Result<T> = result::Result<T, KvsError>;

/// DEFRAGMENTATION ALLOWED
const DEFRAGMENATION_SIZE: u32 = 1024;

/// The store for kvs crate
pub struct KvStore {
    elements: HashMap<String, u64>,
    write_file_handle: BufWriter<File>,
    read_file_handle: BufReader<File>,
    stale_entries: u64,
    directory_path: path::PathBuf,
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
    /// let mut store: KvStore = KvStore::open(Path::new(".")).unwrap();
    /// # }
    /// ```

    pub fn open(path: &path::Path) -> Result<Self> {
        let mut index: HashMap<String, u64> = HashMap::new();
        let mut filepath: PathBuf = PathBuf::from(path);
        let mut stale_entries: u64 = 0;

        // Open file handle for reading
        if path.is_dir() {
            filepath.push("kvs_log/kvs.log");
        }

        // Create non-existent directories
        fs::create_dir_all(filepath.parent().unwrap())?;

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
        let mut buf_writer = BufWriter::new(w);
        buf_reader.rewind()?;

        // pass the file handle into the KvStore and return
        Ok(KvStore {
            elements: index,
            write_file_handle: buf_writer,
            read_file_handle: buf_reader,
            stale_entries,
            directory_path: filepath,
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
        if data_offset.is_none() {
            return Ok(None);
        }

        // Unwrapping here since we can be sure `data_offset` isn't None
        let offset = *data_offset.unwrap();
        self.read_file_handle.seek(io::SeekFrom::Start(offset))?;

        let kvslogline = KvStore::deserialize_from_log(&mut self.read_file_handle)?;
        if let KvsLogLine::Set { key: _, value } = kvslogline {
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

        let data_offset = KvStore::serialize_to_log(&mut self.write_file_handle, logline)?;

        // place the element in the index
        if self.elements.contains_key(&key) {
            self.stale_entries += 1;
        }
        self.elements.insert(key, data_offset);

        // check for defragmentation
        if self.stale_entries > DEFRAGMENATION_SIZE.into() {
            self.compaction()?;
        }

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

        let _ = KvStore::serialize_to_log(&mut self.write_file_handle, logline);

        // remove the element from the index
        self.elements.remove(&key);
        self.stale_entries += 2;

        // check for defragmentation
        if self.stale_entries > DEFRAGMENATION_SIZE.into() {
            self.compaction()?;
        }
        Ok(())
    }

    fn serialize_to_log(write_handle: &mut BufWriter<File>, logline: KvsLogLine) -> Result<u64> {
        write_handle.seek(io::SeekFrom::End(0))?;
        let data_offset = write_handle.stream_position()?;

        let mut s = flexbuffers::FlexbufferSerializer::new();
        logline.serialize(&mut s)?;

        // serialize to the log
        let size: u32 = s.view().len().try_into().unwrap();
        write_handle.write_all(&(size.to_le_bytes()))?;
        write_handle.write_all(s.take_buffer().as_slice())?;
        write_handle.flush()?;
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

    fn compaction(&mut self) -> Result<()> {
        // create temporary file
        // can we get the directory from current file handle? Yes, done
        let dir_path = self.directory_path.parent().unwrap();
        let directory = File::open(dir_path)?;

        let temp_path = self.directory_path.clone().with_file_name("temp_log.log");
        let w = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&temp_path)?;

        let mut buf_writer = BufWriter::new(w);

        // create struct fields that need to be changed
        let r = OpenOptions::new().read(true).open(&temp_path)?;
        let buf_reader = BufReader::new(r);
        let mut elements: HashMap<String, u64> = HashMap::new();

        // write all current index to temp file
        for (key, &old_offset) in &self.elements {
            // deserialize to get the value from the old file
            self.read_file_handle
                .seek(io::SeekFrom::Start(old_offset))?;
            let kvslogline = KvStore::deserialize_from_log(&mut self.read_file_handle)?;

            // serialize to the new file
            let new_offset = KvStore::serialize_to_log(&mut buf_writer, kvslogline)?;
            elements.insert(key.to_string(), new_offset);
        }

        // mv temp file to the operating file
        // w.sync_all()?; //sync file
        buf_writer.flush()?;
        fs::rename(temp_path, &self.directory_path)?; // rename the file
        directory.sync_all()?; // sync the directory

        // set the new parameters into self
        self.elements = elements;
        self.write_file_handle = buf_writer;
        self.read_file_handle = buf_reader;
        self.stale_entries = 0;

        Ok(())
    }
}