use serde_json::Deserializer;
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{self, prelude::*, BufReader, BufWriter};
use std::path::PathBuf;
use std::{collections::HashMap, fs::OpenOptions, path::Path};

use crate::client::Command;
use crate::error::{KvsError, Result};

struct CommandPos {
    walfile_num: u64,
    pos: u64,
    len: u64,
}

const MAX_WAL_SIZE_THRESHOLD: u64 = 1024 * 1024;

/// A key-value store for storing string pairs
pub struct KvStore {
    path: PathBuf,
    index: BTreeMap<String, CommandPos>,
    readers: HashMap<u64, BufReaderWithPos<File>>,
    writer: BufWriterWithPos<File>,
    current_walfile_num: u64,
    uncompacted_size: u64,
}

impl KvStore {
    pub fn open(path: &Path) -> Result<Self> {
        let mut index = BTreeMap::new();
        let mut readers = HashMap::new();
        let mut uncompacted_size: u64 = 0;
        let walfile_nums = sorted_walfile_nums(path)?;
        for walfile_num in &walfile_nums {
            let mut reader = BufReaderWithPos::new(File::open(log_path(path, *walfile_num))?)?;
            uncompacted_size += load(*walfile_num, &mut reader, &mut index)?;
            readers.insert(*walfile_num, reader);
        }
        let current_walfile_num = walfile_nums.last().unwrap_or(&0) + 1;
        let writer = new_log_file(path, current_walfile_num)?;
        readers.insert(
            current_walfile_num,
            BufReaderWithPos::new(File::open(log_path(path, current_walfile_num))?)?,
        );
        Ok(Self {
            path: path.into(),
            index,
            readers,
            writer,
            current_walfile_num,
            uncompacted_size,
        })
    }
    /// Retrieves the value associated with the given key
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        if let Some(cmd_pos) = self.index.remove(&key) {
            let reader = self
                .readers
                .get_mut(&cmd_pos.walfile_num)
                .expect("unable to find reader in readers");
            reader.seek(io::SeekFrom::Start(cmd_pos.pos))?;
            let cmd_reader = reader.take(cmd_pos.len);
            if let Command::Set { value, .. } = serde_json::from_reader(cmd_reader)? {
                return Ok(Some(value));
            } else {
                Err(KvsError::InvalidCommand)
            }
        } else {
            Ok(None)
        }
    }

    /// Sets a value for the given key
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let cmd = Command::Set {
            key: key.clone(),
            value,
        };
        let pos = self.writer.pos;
        serde_json::to_writer(&mut self.writer, &cmd)?;
        self.writer.flush()?;
        let new_pos = self.writer.pos;
        let cmd_pos = CommandPos {
            walfile_num: self.current_walfile_num,
            pos,
            len: new_pos - pos,
        };
        if let Some(old_cmd) = self.index.insert(key, cmd_pos) {
            self.uncompacted_size += old_cmd.len;
        }
        if self.uncompacted_size >= MAX_WAL_SIZE_THRESHOLD {
            self.run_compaction()?;
        }
        Ok(())
    }

    /// Removes a key and its associated value from the store
    pub fn remove(&mut self, key: String) -> Result<()> {
        if self.index.contains_key(&key) {
            let cmd = Command::Rm { key: key.clone() };
            serde_json::to_writer(&mut self.writer, &cmd)?;
            if let Some(old_cmd) = self.index.remove(&key) {
                // TODO: will this case every arrive? i don't think so, will see in future
                self.uncompacted_size += old_cmd.len;
            }
            return Ok(());
        }
        Err(KvsError::KeyNotFound)
    }

    fn run_compaction(&mut self) -> Result<()> {
        let compaction_walfile_num = self.current_walfile_num + 1;
        self.current_walfile_num += 1; // for new active wal
        self.writer = new_log_file(&self.path, self.current_walfile_num)?;
        let mut compaction_writer = new_log_file(&self.path, compaction_walfile_num)?;
        let pos: u64 = 0;
        for cmd_pos in self.index.values_mut() {
            let reader = self
                .readers
                .get_mut(&cmd_pos.walfile_num)
                .expect("reader not found for a command in readers");
            reader.seek(io::SeekFrom::Start(cmd_pos.pos))?;
            let mut cmd_reader = reader.take(cmd_pos.len);
            let len = io::copy(&mut cmd_reader, &mut compaction_writer)?;
            *cmd_pos = CommandPos {
                walfile_num: compaction_walfile_num,
                pos,
                len: len - pos,
            };
        }
        compaction_writer.flush()?;
        let stale_files: Vec<_> = self
            .readers
            .keys()
            .filter(|x| **x < self.current_walfile_num)
            .cloned()
            .collect();
        for stale_walfile_num in &stale_files {
            fs::remove_file(log_path(&self.path, *stale_walfile_num))?;
        }
        self.uncompacted_size = 0;
        Ok(())
    }
}

fn new_log_file(dir: &Path, walfile_num: u64) -> Result<BufWriterWithPos<File>> {
    let writer = BufWriterWithPos::new(
        OpenOptions::new()
            .create(true)
            .append(true)
            .open(log_path(dir, walfile_num))?,
    )?;
    Ok(writer)
}

fn load(
    walfile_num: u64,
    reader: &mut BufReaderWithPos<File>,
    index: &mut BTreeMap<String, CommandPos>,
) -> Result<u64> {
    let mut pos = reader.seek(io::SeekFrom::Start(0))?;
    let mut stream = Deserializer::from_reader(reader).into_iter::<Command>();
    let mut uncompacted_size = 0;
    while let Some(cmd) = stream.next() {
        let new_pos = stream.byte_offset() as u64;
        match cmd? {
            Command::Set { key, .. } => {
                if let Some(old_cmd) = index.insert(
                    key,
                    CommandPos {
                        walfile_num,
                        pos,
                        len: new_pos - pos,
                    },
                ) {
                    uncompacted_size += old_cmd.len
                }
            }
            Command::Rm { key } => {
                if let Some(old_cmd) = index.remove(&key) {
                    uncompacted_size += old_cmd.len;
                } else {
                    uncompacted_size += new_pos - pos;
                }
            }
            _ => {}
        }
        pos = new_pos;
    }
    Ok(uncompacted_size)
}

fn sorted_walfile_nums(path: &Path) -> Result<Vec<u64>> {
    let mut walfile_nums: Vec<_> = fs::read_dir(path)?
        .flat_map(|res| -> Result<_> { Ok(res?.path()) })
        .filter(|path| path.is_file() && path.extension() == Some("log".as_ref()))
        .flat_map(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .map(|s| s.trim_start_matches("wal_"))
                .map(|s| s.trim_end_matches(".log"))
                .map(str::parse::<u64>)
        })
        .flatten()
        .collect();
    walfile_nums.sort_unstable();
    Ok(walfile_nums)
}

fn log_path(dir: &Path, walfile_num: u64) -> PathBuf {
    dir.join(format!("wal_{}.log", walfile_num))
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
}

impl<R: Read + Seek> Read for BufReaderWithPos<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let len = self.reader.read(buf)?;
        self.pos += len as u64;
        Ok(len)
    }
}

impl<R: Read + Seek> Seek for BufReaderWithPos<R> {
    fn seek(&mut self, pos: io::SeekFrom) -> io::Result<u64> {
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
        let pos = inner.seek(io::SeekFrom::Current(0))?;
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
        self.writer.flush()?;
        Ok(())
    }
}

impl<W: Write + Seek> Seek for BufWriterWithPos<W> {
    fn seek(&mut self, pos: io::SeekFrom) -> io::Result<u64> {
        self.pos = self.writer.seek(pos)?;
        Ok(self.pos)
    }
}
