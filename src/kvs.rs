use crate::error::KvsError;

use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::ffi::OsStr;
use std::fs::OpenOptions;
use std::fs::{self, File};
use std::io::BufReader;
use std::io::BufWriter;
use std::io::{prelude::*, SeekFrom};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::{io, result};

/// Result type for the kvs crate
pub type Result<T> = result::Result<T, KvsError>;

const COMPACTION_THRESHOLD: u64 = 1024 * 1024;

/// The store for kvs crate
pub struct KvStore {
    // directory for the log and other data
    path: PathBuf,
    // map generation number to the file reader
    readers: HashMap<u64, BufReaderWithPos<File>>,
    // writer of the current log
    writer: BufWriterWithPos<File>,
    current_gen: u64,
    index: BTreeMap<String, CommandPos>,
    // the number of bytes representing "stale" commands that could be
    // deleted during a compaction
    uncompacted: u64,
}

/// The command set for serialization and storage
#[derive(Debug, Serialize, Deserialize)]
enum KvsLogLine {
    Set { key: String, value: String },
    Rm { key: String },
}

/// Represents the position and length of a serialized command in the log
struct CommandPos {
    gen: u64,
    pos: u64,
    len: u64,
}

impl From<(u64, Range<u64>)> for CommandPos {
    fn from((gen, range): (u64, Range<u64>)) -> Self {
        CommandPos {
            gen,
            pos: range.start,
            len: range.end - range.start,
        }
    }
}

struct BufReaderWithPos<R: Read + Seek> {
    reader: BufReader<R>,
    pos: u64,
}

impl<R: Read + Seek> BufReaderWithPos<R> {
    fn new(mut inner: R) -> Result<Self> {
        let pos = inner.seek(io::SeekFrom::Current(0))?;
        Ok(BufReaderWithPos {
            reader: BufReader::new(inner),
            pos,
        })
    }
    fn is_empty(&mut self) -> Result<bool> {
        Ok(self.reader.fill_buf()?.is_empty())
    }
}

impl<R: Read + Seek> Read for BufReaderWithPos<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let len = self.reader.read(buf)?;
        self.pos += len as u64;
        Ok(len)
    }
    fn read_exact(&mut self, buf: &mut [u8]) -> io::Result<()> {
        self.reader.read_exact(buf);
        self.pos += buf.len() as u64;
        Ok(())
    }
}

impl<R: Read + Seek> Seek for BufReaderWithPos<R> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.pos = self.reader.seek(pos)?;
        Ok(self.pos)
    }
}

struct BufWriterWithPos<W: Write + Seek> {
    writer: BufWriter<W>,
    pos: u64,
}

impl<W: Write + Seek> BufWriterWithPos<W> {
    fn new(mut inner: W) -> Result<Self> {
        let pos = inner.seek(SeekFrom::Current(0))?;
        Ok(BufWriterWithPos {
            writer: BufWriter::new(inner),
            pos,
        })
    }
}

impl<W: Write + Seek> Write for BufWriterWithPos<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let len = self.writer.write(buf)?;
        self.pos += len as u64;
        Ok(len)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}

impl<W: Write + Seek> Seek for BufWriterWithPos<W> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.pos = self.writer.seek(pos)?;
        Ok(self.pos)
    }
}

impl KvStore {
    /// Opens a `KvStore` with the given path
    ///
    /// This will create a new directory if the given one does not exist
    ///
    /// # Errors
    ///
    /// It propagates I/O or deserialization errors during log replay
    ///
    /// ```
    /// # use kvs::KvStore;
    /// #
    /// # fn main() {
    /// let mut store: KvStore = KvStore::open(Path::new(".")).unwrap();
    /// # }
    /// ```
    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        fs::create_dir_all(&path)?;

        let mut index = BTreeMap::new();
        let mut readers = HashMap::new();

        let gen_list = sorted_gen_list(&path)?;
        let mut uncompacted = 0;

        for &gen in &gen_list {
            let mut reader = BufReaderWithPos::new(File::open(log_path(&path, gen))?)?;
            uncompacted += load(gen, &mut reader, &mut index)?;
            readers.insert(gen, reader);
        }

        let current_gen = gen_list.last().unwrap_or(&0) + 1;
        let writer = new_log_file(&path, current_gen, &mut readers)?;

        Ok(KvStore {
            path,
            readers,
            writer,
            current_gen,
            index,
            uncompacted,
        })
    }

    /// Gets the string value of a given string key
    ///
    /// Returns `None` if the given key does not exist
    ///
    /// # Errors
    ///
    /// It propagates I/O or deserialization errors during log replay.
    /// Also returns `KvsError::UnexpectedCommandType` if the given command type is unexpected
    ///
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
        if let Some(cmd_pos) = self.index.get(&key) {
            let reader = self
                .readers
                .get_mut(&cmd_pos.gen)
                .expect("Cannot find log reader");
            reader.seek(SeekFrom::Start(cmd_pos.pos));
            if let KvsLogLine::Set { key: _, value } = deserialize_from_log(reader)? {
                Ok(Some(value))
            } else {
                Err(KvsError::UnexpectedCommandType)
            }
        } else {
            Ok(None)
        }
    }

    /// Sets the value of a string key to a string
    ///
    /// If the key already exists, the previous value will be overwritten.
    ///
    /// # Errors
    ///
    /// It propagates I/O or serialization errors during writing the log
    ///
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

        let start_pos = self.writer.pos;
        serialize_to_log(&mut self.writer, logline)?;

        // place the element in the index
        if let Some(old_cmd) = self
            .index
            .insert(key, (self.current_gen, start_pos..self.writer.pos).into())
        {
            self.uncompacted += old_cmd.len;
        }

        // check for defragmentation
        if self.uncompacted > COMPACTION_THRESHOLD {
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
        if !self.index.contains_key(&key) {
            return Err(KvsError::KeyDoesNotExist);
        }
        let logline = KvsLogLine::Rm { key: key.clone() };
        serialize_to_log(&mut self.writer, logline);
        // remove the element from the index
        if let Some(old_cmd) = self.index.remove(&key) {
            self.uncompacted += old_cmd.len;
        }
        Ok(())
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

fn new_log_file(
    path: &Path,
    gen: u64,
    readers: &mut HashMap<u64, BufReaderWithPos<File>>,
) -> Result<BufWriterWithPos<File>> {
    let path = log_path(path, gen);
    let writer = BufWriterWithPos::new(
        OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(&path)?,
    )?;
    readers.insert(gen, BufReaderWithPos::new(File::open(&path)?)?);
    Ok(writer)
}

fn serialize_to_log(write_handle: &mut BufWriterWithPos<File>, logline: KvsLogLine) -> Result<()> {
    let mut s = flexbuffers::FlexbufferSerializer::new();
    logline.serialize(&mut s)?;
    // serialize to the log
    let size: u32 = s.view().len().try_into().unwrap();
    write_handle.write(&(size.to_le_bytes()))?;
    write_handle.write(s.take_buffer().as_slice())?;
    write_handle.flush()?;
    Ok(())
}

fn deserialize_from_log(reader: &mut BufReaderWithPos<File>) -> Result<KvsLogLine> {
    let mut buffer = [0u8; 4];
    reader.read_exact(&mut buffer)?;
    let size = u32::from_le_bytes(buffer).try_into()?;

    let mut logline = vec![0u8; size];
    reader.read_exact(&mut logline)?;
    let r = flexbuffers::Reader::get_root(logline.as_slice())?;
    let kvslogline = KvsLogLine::deserialize(r)?;
    Ok(kvslogline)
}

fn load(
    gen: u64,
    reader: &mut BufReaderWithPos<File>,
    index: &mut BTreeMap<String, CommandPos>,
) -> Result<u64> {
    let mut pos = reader.seek(SeekFrom::Start(0))?;
    let mut uncompacted = 0;
    while !reader.is_empty()? {
        let kvslogline = deserialize_from_log(reader)?;
        let new_pos = reader.pos;
        match kvslogline {
            KvsLogLine::Set { key, .. } => {
                if let Some(old_cmd) = index.insert(key, (gen, pos..new_pos).into()) {
                    uncompacted += old_cmd.len;
                }
            }
            KvsLogLine::Rm { key } => {
                if let Some(old_cmd) = index.remove(&key) {
                    uncompacted += old_cmd.len;
                }
                uncompacted += new_pos - pos;
            }
        }
        pos = new_pos;
    }
    Ok(uncompacted)
}

fn log_path(path: &Path, gen: u64) -> PathBuf {
    path.join(format!("{}.log", gen))
}

fn sorted_gen_list(path: &Path) -> Result<Vec<u64>> {
    let mut gen_list: Vec<u64> = fs::read_dir(&path)?
        .flat_map(|res| -> Result<_> { Ok(res?.path()) })
        .filter(|path| path.is_file() && path.extension() == Some("log".as_ref()))
        .flat_map(|path| {
            path.file_name()
                .and_then(OsStr::to_str)
                .map(|s| s.trim_end_matches(".log"))
                .map(str::parse::<u64>)
        })
        .flatten()
        .collect();
    gen_list.sort_unstable();
    Ok(gen_list)
}
