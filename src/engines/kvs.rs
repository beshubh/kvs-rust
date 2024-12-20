use crate::client::Command;
use crate::error::{KvsError, Result};
use dashmap::DashMap;
use serde_json::Deserializer;
use std::fs::{self, File};
use std::io::{self, prelude::*, BufReader, BufWriter};
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::{atomic, Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;
use std::{fs::OpenOptions, path::Path};

use super::KvsEngine;

struct CommandPos {
    walfile_num: u64,
    pos: u64,
    len: u64,
}

const MAX_WAL_SIZE_THRESHOLD: u64 = 1024 * 1024;

/// A key-value store for storing string pairs
#[derive(Clone)]
pub struct KvStore {
    index: Arc<DashMap<String, CommandPos>>,
    reader: Arc<KvStoreReader>,
    writer: Arc<Mutex<KvStoreWriter>>,
    running: Arc<AtomicBool>,
    compaction_thread: Arc<Mutex<Option<JoinHandle<()>>>>,
}

impl KvStore {
    pub fn open(path: &Path) -> Result<Self> {
        let mut index = DashMap::new();

        let walfile_nums = sorted_walfile_nums(path)?;
        let reader = Arc::new(KvStoreReader::from_walfiles(
            path,
            walfile_nums.clone(),
            &mut index,
        )?);
        let current_walfile_num = walfile_nums.last().unwrap_or(&0) + 1;
        let index = Arc::new(index);

        let writer = KvStoreWriter::new(
            path,
            current_walfile_num,
            Arc::clone(&reader),
            index.clone(),
        )?;
        let writer = Arc::new(Mutex::new(writer));
        reader.add_reader(current_walfile_num)?;

        let running = Arc::new(AtomicBool::new(true));
        let running_clone = running.clone();
        let writer_clone = writer.clone();

        let compaction_thread = thread::spawn(move || {
            while running_clone.load(atomic::Ordering::Relaxed) {
                if let Ok(mut writer_guard) = writer_clone.lock() {
                    if writer_guard.uncompacted > MAX_WAL_SIZE_THRESHOLD {
                        if let Err(e) = writer_guard.run_compaction() {
                            println!("Error compacting: {:?}", e);
                        }
                    }
                }
                thread::sleep(Duration::from_secs(2));
            }
        });

        Ok(Self {
            index,
            reader,
            writer,
            running,
            compaction_thread: Arc::new(Mutex::new(Some(compaction_thread))),
        })
    }
}

impl Drop for KvStore {
    fn drop(&mut self) {
        if Arc::strong_count(&self.compaction_thread) == 1 {
            self.running.store(false, atomic::Ordering::Relaxed);
            if let Ok(mut guard) = self.compaction_thread.lock() {
                if let Some(handle) = guard.take() {
                    handle.join().unwrap();
                }
            }
        }
    }
}

impl KvsEngine for KvStore {
    /// Retrieves the value associated with the given key
    fn get(&self, key: String) -> Result<Option<String>> {
        if let Some(val) = self.index.get(&key) {
            return Ok(self.reader.get(&*val)?);
        }
        Ok(None)
    }

    /// Sets a value for the given key
    fn set(&self, key: String, value: String) -> Result<()> {
        let mut writer = self.writer.lock().unwrap();
        writer.set(key, value)?;
        Ok(())
    }

    /// Removes a key and its associated value from the store
    fn remove(&self, key: String) -> Result<()> {
        self.writer.lock().unwrap().remove(key)?;
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
    index: &DashMap<String, CommandPos>,
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
                    uncompacted_size += old_cmd.1.len;
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

#[derive(Debug)]
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

#[derive(Debug)]
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

struct KvStoreReader {
    path: PathBuf,
    readers: DashMap<u64, BufReaderWithPos<File>>,
}

impl KvStoreReader {
    fn get(&self, cmd_pos: &CommandPos) -> Result<Option<String>> {
        let reader = self.readers.get_mut(&cmd_pos.walfile_num);
        if reader.is_none() {
            return Err(KvsError::Message("KvStoreReader: Reader not found".into()));
        }
        let mut reader = reader.unwrap();
        reader.seek(io::SeekFrom::Start(cmd_pos.pos))?;
        let cmd_reader = reader.by_ref().take(cmd_pos.len);
        if let Command::Set { value, .. } = serde_json::from_reader(cmd_reader)? {
            return Ok(Some(value));
        }
        return Err(KvsError::InvalidCommand);
    }

    fn from_walfiles(
        path: &Path,
        walfile_nums: Vec<u64>,
        index: &DashMap<String, CommandPos>,
    ) -> Result<Self> {
        let readers = DashMap::new();
        for walfile_num in walfile_nums {
            let mut reader =
                BufReaderWithPos::new(File::open(log_path(path, walfile_num)).unwrap())?;
            load(walfile_num, &mut reader, index)?;
            readers.insert(walfile_num, reader);
        }
        Ok(Self {
            path: path.into(),
            readers,
        })
    }

    fn add_reader(&self, walfile_num: u64) -> Result<()> {
        if self.readers.contains_key(&walfile_num) {
            return Err(KvsError::Message(
                "KvStoreReader: Reader already exists".into(),
            ));
        }
        self.readers.insert(
            walfile_num,
            BufReaderWithPos::new(File::open(log_path(&self.path, walfile_num)).unwrap()).unwrap(),
        );
        Ok(())
    }

    fn close_stale_handles(&self, compaction_walfile_num: u64) -> Result<()> {
        let keys: Vec<u64> = self.readers.iter().map(|pair| *pair.key()).collect();
        let stale_files: Vec<_> = keys
            .iter()
            .filter(|x| **x < compaction_walfile_num)
            .cloned()
            .collect();
        for stale_walfile_num in &stale_files {
            let path = log_path(&self.path, *stale_walfile_num);
            fs::remove_file(&path)?;
            self.readers.remove(&stale_walfile_num);
        }
        Ok(())
    }
}

struct KvStoreWriter {
    reader: Arc<KvStoreReader>,
    writer: BufWriterWithPos<File>,
    active_wal: u64,
    // number of bytes that can be saved by compaction
    uncompacted: u64,
    path: Arc<PathBuf>,
    index: Arc<DashMap<String, CommandPos>>,
}

impl KvStoreWriter {
    fn new(
        path: &Path,
        active_wal: u64,
        reader: Arc<KvStoreReader>,
        index: Arc<DashMap<String, CommandPos>>,
    ) -> Result<Self> {
        Ok(Self {
            reader,
            writer: new_log_file(path, active_wal)?,
            active_wal,
            uncompacted: 0,
            path: Arc::new(path.into()),
            index,
        })
    }

    fn set(&mut self, key: String, value: String) -> Result<()> {
        let cmd = Command::Set {
            key: key.clone(),
            value,
        };
        let pos = self.writer.pos;
        serde_json::to_writer(&mut self.writer, &cmd)?;
        self.writer.flush()?;

        let new_pos = self.writer.pos;
        let cmd_pos = CommandPos {
            walfile_num: self.active_wal,
            pos,
            len: new_pos - pos,
        };
        if let Some(old_cmd) = self.index.insert(key, cmd_pos) {
            self.uncompacted += old_cmd.len;
        }
        Ok(())
    }

    fn remove(&mut self, key: String) -> Result<()> {
        let cmd = Command::Rm { key: key.clone() };
        serde_json::to_writer(&mut self.writer, &cmd)?;
        if let Some((_, cmd)) = self.index.remove(&key) {
            self.uncompacted += cmd.len;
            return Ok(());
        } else {
            return Err(KvsError::KeyNotFound);
        }
    }

    fn run_compaction(&mut self) -> Result<()> {
        let active_wal = self.active_wal;
        let compaction_walfile_num = active_wal + 1;
        self.active_wal = active_wal + 2;
        let mut compaction_writer = new_log_file(&self.path, compaction_walfile_num)?;

        // new active wal file
        self.writer = new_log_file(&self.path, self.active_wal)?;
        self.reader.add_reader(self.active_wal)?;

        let mut pos: u64 = 0;

        for mut cmd_pos in self.index.iter_mut() {
            if cmd_pos.walfile_num >= compaction_walfile_num {
                continue;
            }
            // println!("compacting walfile num: {}", cmd_pos.walfile_num);
            let reader = self.reader.readers.get_mut(&cmd_pos.walfile_num);
            if reader.is_none() {
                panic!("reader not found for the command that was in the index?");
            }
            let mut reader = reader.unwrap();
            reader
                .seek(io::SeekFrom::Start(cmd_pos.pos))
                .expect("unable to seek reader");

            let mut cmd_reader = reader.by_ref().take(cmd_pos.len);
            let len = io::copy(&mut cmd_reader, &mut compaction_writer)?;
            *cmd_pos.value_mut() = CommandPos {
                walfile_num: compaction_walfile_num,
                pos,
                len,
            };
            pos += len;
        }

        compaction_writer.flush().unwrap();
        self.reader.add_reader(compaction_walfile_num)?;
        self.reader.close_stale_handles(compaction_walfile_num)?;
        self.uncompacted = 0;

        Ok(())
    }
}
