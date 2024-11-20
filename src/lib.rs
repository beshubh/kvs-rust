//! A simple key-value store implementation.
//!
//! This crate provides a basic in-memory key-value store for storing
//! string pairs. It supports basic operations like get, set, and remove

// TODO: remove this while publishing
// #![deny(missing_docs)]
use anyhow::Context;
use clap::Subcommand;
use serde::{Deserialize, Serialize};
use std::fs::{self, File};
use std::io::{self, prelude::*};
use std::path::PathBuf;
use std::time::UNIX_EPOCH;
use std::{collections::HashMap, fs::OpenOptions, path::Path};

#[derive(Subcommand, Deserialize, Serialize, Debug, Clone)]
#[serde(rename_all = "lowercase")]
pub enum Command {
    Get {
        #[serde(rename = "k")]
        key: String,
    },
    Set {
        #[serde(rename = "k")]
        key: String,
        #[serde(rename = "v")]
        value: String,
    },
    Rm {
        #[serde(rename = "k")]
        key: String,
    },
    #[command(name = "-V")]
    Version,
}

const MAX_WAL_SIZE_THRESHOLD: u64 = 2000;

/// A key-value store for storing string pairs
pub struct KvStore {
    map: HashMap<String, u64>,
    active_wal: WAL,
    path: PathBuf,
}

pub type Result<T> = anyhow::Result<T>;

impl KvStore {
    /// Open the path & builds a KvStore
    pub fn open(path: &Path) -> Result<Self> {
        let map = WAL::restore_state(path.into())?;
        let active_wal = WAL::new(path.into(), false)?;
        Ok(Self {
            map,
            active_wal,
            path: path.into(),
        })
    }
    /// Retrieves the value associated with the given key
    ///
    /// # Arguments
    ///
    /// * `key` - The key to look up
    ///
    /// # Returns
    ///
    /// Returns `Some(String)` if the key exists, `None` otherwise
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use kvs::KvStore;
    /// # use tempfile::TempDir;
    /// # let temp_dir = TempDir::new().expect("unable to create temp dir");
    /// # let mut store = KvStore::open(temp_dir.path()).unwrap();
    /// # store.set("key".to_string(), "value".to_string()).unwrap();
    /// # assert_eq!(store.get("key".to_owned()).unwrap(), Some("value".to_string()));
    /// ```
    pub fn get(&mut self, k: String) -> Result<Option<String>> {
        if let Some(offset) = self.map.get(&k) {
            self.active_wal
                .reader
                .seek(io::SeekFrom::Start(offset.to_owned()))?;
            let mut length_buffer = [0u8; 4];
            self.active_wal
                .reader
                .read_exact(&mut length_buffer)
                .context("unable to read length frame")?;
            let length = u32::from_le_bytes(length_buffer) as usize;
            let mut buf = vec![0u8; length];
            self.active_wal.reader.read_exact(&mut buf)?;
            let cmd: Command =
                bson::from_reader(&mut buf.as_slice()).expect("BSON deserialzation failed");
            match &cmd {
                Command::Set { value, .. } => return Ok(Some(value.to_owned())),
                _ => return Ok(None),
            }
        }
        Ok(None)
    }

    /// Sets a value for the given key
    ///
    /// If the key already exists, the value will be updated
    ///
    /// # Arguments
    ///
    /// * `key` - The key to set
    /// * `val` - The value to associate with the key
    /// # Examples
    /// ```rust
    /// # use kvs::KvStore;
    /// # use tempfile::TempDir;
    /// # let temp_dir = TempDir::new().expect("unable to create temp dir");
    /// # let mut store = KvStore::open(temp_dir.path()).unwrap();
    /// # store.set("SomeKey".to_owned(), "Val".to_owned()).unwrap();
    /// ```
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let cmd = Command::Set {
            key: key.clone(),
            value: value.clone(),
        };
        let offset = self.active_wal.write_log(cmd)?;
        self.map.insert(key, offset);
        if self.active_wal.size()? >= MAX_WAL_SIZE_THRESHOLD {
            self.run_compaction().context("Failed to run compaction")?;
        }
        Ok(())
    }

    /// Removes a key and its associated value from the store
    ///
    /// # Arguments
    ///
    /// * `key` - The key to remove
    /// ```rust
    /// # use kvs::KvStore;
    /// # use tempfile::TempDir;
    /// # let temp_dir = TempDir::new().expect("unable to create temp dir");
    /// # let mut store = KvStore::open(temp_dir.path()).unwrap();
    /// # store.set("key".to_string(), "value".to_string()).unwrap();
    /// # store.remove("key".to_owned()).unwrap();
    /// # assert_eq!(store.get("key".to_owned()).unwrap(), None);
    /// ```
    pub fn remove(&mut self, key: String) -> Result<()> {
        let cmd = Command::Rm { key: key.clone() };
        if let Some(_) = self.map.remove(&key) {
            self.active_wal.write_log(cmd).unwrap();
            return Ok(());
        }
        Err(anyhow::anyhow!("Key not found"))
    }

    fn run_compaction(&mut self) -> Result<()> {
        let mut new_map = HashMap::new();
        let mut new_wal = WAL::new(self.path.clone(), true)?;
        // FIXME: find a rust way to do this, rn I am just cloning
        for (key, _) in self.map.clone().into_iter() {
            let value = self.get(key.to_owned())?;
            if value.is_none() {
                continue;
            }
            let cmd = Command::Set {
                key: key.to_owned(),
                value: value.unwrap(),
            };
            let write_offset = new_wal.write_log(cmd)?;
            new_map.insert(key.to_owned(), write_offset);
        }
        fs::remove_file::<PathBuf>(self.active_wal.path.clone())
            .context("failed to remove old wal file")?;
        self.active_wal = new_wal;
        self.map = new_map;
        Ok(())
    }
}

struct WAL {
    reader: File,
    writer: File,
    current_write_offset: u64,
    path: PathBuf,
}

impl WAL {
    fn latest_wal_file(wal_dir: PathBuf) -> Result<Option<PathBuf>> {
        let mut entries: Vec<_> = fs::read_dir(wal_dir)
            .context("unable to read dir for restoring logs")?
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| {
                p.file_name()
                    .and_then(|name| name.to_str())
                    .map(|name| name.starts_with("wal_"))
                    .unwrap_or(false)
            })
            .collect();
        entries.sort();
        Ok(entries.last().cloned())
    }

    fn size(&self) -> Result<u64> {
        Ok(self.reader.metadata()?.len())
    }

    fn new(wal_dir: PathBuf, force_new: bool) -> Result<Self> {
        let file_name = format!(
            "wal_{}",
            std::time::SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis()
        );
        let mut log_file_path = wal_dir.join(&file_name);
        let latest_log_file_path = Self::latest_wal_file(wal_dir.clone())?;
        if latest_log_file_path.is_some() && !force_new {
            log_file_path = latest_log_file_path.unwrap()
        }

        let mut writer = OpenOptions::new()
            .append(true)
            .create(true)
            .open(&log_file_path)
            .context("Failed to create new file")?;
        let current_write_offset = writer
            .seek(io::SeekFrom::End(0))
            .context("failed to move file pointer to end of the file")?;
        Ok(Self {
            reader: File::open(&log_file_path).unwrap(),
            writer,
            current_write_offset,
            path: log_file_path,
        })
    }

    fn restore_state(wal_dir: PathBuf) -> Result<HashMap<String, u64>> {
        let mut map: HashMap<String, u64> = HashMap::new();
        let latest_log_file_path = Self::latest_wal_file(wal_dir)?;
        if latest_log_file_path.is_none() {
            return Ok(map);
        }

        let latest_log_file_path = latest_log_file_path.unwrap();

        let mut log_file =
            File::open(&latest_log_file_path).context("failed to open file for restoring state")?;
        let mut offset = log_file
            .stream_position()
            .context("failed to get the initial offset")?;
        let mut length_buffer = [0u8; 4];

        while let Ok(()) = log_file.read_exact(&mut length_buffer) {
            let length = u32::from_le_bytes(length_buffer) as usize;
            let mut bson_buffer = vec![0u8; length];
            log_file
                .read_exact(&mut bson_buffer)
                .context("failed to read into bson buffer while restoring state")?;
            let cmd: Command = bson::from_reader(&mut bson_buffer.as_slice())
                .expect("BSON deserialization failed");
            match &cmd {
                Command::Set { key, .. } => {
                    map.insert(key.into(), offset);
                }
                Command::Rm { key } => {
                    map.remove(key.into());
                }
                _ => panic!("Invalid log"),
            }
            offset = log_file
                .stream_position()
                .context("failed to get the file pointer offset")?;
        }
        Ok(map)
    }

    fn write_log(&mut self, cmd: Command) -> Result<u64> {
        let current_offset = self.current_write_offset;
        let bytes = bson::to_vec(&cmd).expect("BSON serialization failed");
        self.writer
            .write(&(bytes.len() as u32).to_le_bytes())
            .context("failed to write the frame")?;
        self.writer
            .write(&bytes)
            .context("failed to write command to log")?;
        self.current_write_offset = self
            .writer
            .stream_position()
            .context("failed to get the updated file pointer")?;
        Ok(current_offset)
    }
}

#[cfg(test)]
mod tests {

    use crate::KvStore;
    use assert_cmd::prelude::*;
    use predicates::ord::eq;
    use predicates::str::PredicateStrExt;
    use std::process::Command;
    use tempfile::TempDir;

    #[test]
    fn test_set() {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let mut kv = KvStore::open(temp_dir.path()).unwrap();
        kv.set("test".into(), "567".into()).unwrap();
        kv.set("test".into(), "67".into()).unwrap();
        kv.set("test".into(), "5567".into()).unwrap();
        kv.set("test".into(), "shubh".into()).unwrap();
        assert_eq!(kv.get("test".into()).unwrap(), Some("shubh".to_string()));
        kv.set("name".into(), "xyz".into()).unwrap();
        kv.set("age".into(), "12".into()).unwrap();
        kv.set("city".into(), "Bengaluru".into()).unwrap();
        assert_eq!(kv.get("name".into()).unwrap(), Some("xyz".to_string()));
        assert_eq!(kv.get("age".into()).unwrap(), Some("12".to_string()));
        assert_eq!(
            kv.get("city".into()).unwrap(),
            Some("Bengaluru".to_string())
        );
    }
    #[test]
    fn cli_get_stored() {
        // let temp_dir = TempDir::new().expect("unable to create temporary working directory");

        let temp_dir = std::env::current_dir().unwrap();
        let mut store = KvStore::open(temp_dir.as_path()).unwrap();
        store.set("key1".to_owned(), "value1".to_owned()).unwrap();
        store.set("key2".to_owned(), "value2".to_owned()).unwrap();
        drop(store);

        Command::cargo_bin("kvs")
            .unwrap()
            .args(&["get", "key1"])
            .current_dir(&temp_dir)
            .assert()
            .success()
            .stdout(eq("value1").trim());

        Command::cargo_bin("kvs")
            .unwrap()
            .args(&["get", "key2"])
            .current_dir(&temp_dir)
            .assert()
            .success()
            .stdout(eq("value2").trim());
    }
}
