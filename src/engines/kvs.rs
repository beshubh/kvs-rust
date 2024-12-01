use crate::client::Command;
use crate::error::{KvsError, Result};
use serde_json::Deserializer;
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{self, prelude::*, BufReader, BufWriter};
use std::path::PathBuf;
use std::sync::atomic;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex};
use std::{collections::HashMap, fs::OpenOptions, path::Path};

use super::KvsEngine;

struct CommandPos {
    walfile_num: u64,
    pos: u64,
    len: u64,
}

const MAX_WAL_SIZE_THRESHOLD: u64 = 1024 * 1024;

/// A key-value store for storing string pairs
///
pub struct KvStore(Arc<SharedKvStore>);

impl Clone for KvStore {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

pub struct SharedKvStore {
    path: PathBuf,
    index: Mutex<BTreeMap<String, CommandPos>>,
    readers: Mutex<HashMap<u64, BufReaderWithPos<File>>>,
    writer: Mutex<BufWriterWithPos<File>>,
    current_walfile_num: AtomicU64,
    uncompacted_size: AtomicU64,
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
        Ok(Self(Arc::new(SharedKvStore {
            path: path.into(),
            index: Mutex::new(index),
            readers: Mutex::new(readers),
            writer: Mutex::new(writer),
            current_walfile_num: current_walfile_num.into(),
            uncompacted_size: uncompacted_size.into(),
        })))
    }

    fn run_compaction(&self) -> Result<()> {
        let compaction_walfile_num = self
            .0
            .current_walfile_num
            .fetch_add(1, atomic::Ordering::SeqCst) // increment for new active wal
            + 1;

        *self.0.writer.lock().unwrap() = new_log_file(
            &self.0.path,
            self.0.current_walfile_num.load(atomic::Ordering::SeqCst),
        )?;

        let mut compaction_writer = new_log_file(&self.0.path, compaction_walfile_num)?;
        let pos: u64 = 0;
        // DEADLOCK probably
        let mut index = self.0.index.lock().unwrap();
        for cmd_pos in index.values_mut() {
            let mut readers = self.0.readers.lock().unwrap();

            let reader = readers
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
            .0
            .readers
            .lock()
            .unwrap()
            .keys()
            .filter(|x| **x < self.0.current_walfile_num.load(atomic::Ordering::SeqCst))
            .cloned()
            .collect();
        for stale_walfile_num in &stale_files {
            fs::remove_file(log_path(&self.0.path, *stale_walfile_num))?;
        }
        self.0.uncompacted_size.store(0, atomic::Ordering::SeqCst);
        Ok(())
    }
}

impl KvsEngine for KvStore {
    /// Retrieves the value associated with the given key
    fn get(&self, key: String) -> Result<Option<String>> {
        if let Some(cmd_pos) = self.0.index.lock().unwrap().get(&key) {
            let mut readers = self.0.readers.lock().unwrap();
            let reader = readers
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
    fn set(&self, key: String, value: String) -> Result<()> {
        let cmd = Command::Set {
            key: key.clone(),
            value,
        };

        let mut writer = self.0.writer.lock().unwrap();
        let pos = writer.pos;
        serde_json::to_writer(&mut *writer, &cmd)?;
        writer.flush()?;

        let new_pos = writer.pos;
        let cmd_pos = CommandPos {
            walfile_num: self.0.current_walfile_num.load(atomic::Ordering::SeqCst),
            pos,
            len: new_pos - pos,
        };
        if let Some(old_cmd) = self.0.index.lock().unwrap().insert(key, cmd_pos) {
            self.0
                .uncompacted_size
                .fetch_add(old_cmd.len, atomic::Ordering::SeqCst);
        }
        if self.0.uncompacted_size.load(atomic::Ordering::SeqCst) >= MAX_WAL_SIZE_THRESHOLD {
            self.run_compaction()?;
        }
        Ok(())
    }

    /// Removes a key and its associated value from the store
    fn remove(&self, key: String) -> Result<()> {
        let mut index = self.0.index.lock().unwrap();
        if index.contains_key(&key) {
            let cmd = Command::Rm { key: key.clone() };
            serde_json::to_writer(&mut *self.0.writer.lock().unwrap(), &cmd)?;
            if let Some(old_cmd) = index.remove(&key) {
                // TODO: will this case every arrive? i don't think so, will see in future
                self.0
                    .uncompacted_size
                    .fetch_add(old_cmd.len, atomic::Ordering::SeqCst);
            }
            return Ok(());
        }
        Err(KvsError::KeyNotFound)
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
