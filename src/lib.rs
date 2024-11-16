//! A simple key-value store implementation.
//!
//! This crate provides a basic in-memory key-value store for storing
//! string pairs. It supports basic operations like get, set, and remove

// TODO: remove this while publishing
// #![deny(missing_docs)]
use clap::Subcommand;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::{self, prelude::*, BufWriter};
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

/// A key-value store for storing string pairs
pub struct KvStore {
    map: HashMap<String, String>,
    writer: BufWriter<File>,
}

pub type Result<T> = anyhow::Result<T>;

impl KvStore {
    /// Open the path & builds a KvStore
    pub fn open(path: &Path) -> Result<Self> {
        let path = path.join("wal");

        let mut map: HashMap<String, String> = HashMap::new();
        let f = OpenOptions::new().create(true).append(true).open(&path)?;
        let writer = BufWriter::with_capacity(1024 * 10 * 10, f);
        if Path::new(&path).exists() {
            let mut log_file = File::open(&path)?;
            let mut length_buffer = [0u8; 4];
            while let Ok(()) = log_file.read_exact(&mut length_buffer) {
                let length = u32::from_le_bytes(length_buffer) as usize;
                let mut bson_buffer = vec![0u8; length];
                log_file.read_exact(&mut bson_buffer)?;
                let cmd: Command = bson::from_reader(&mut bson_buffer.as_slice())
                    .expect("BSON deserialization failed");
                match &cmd {
                    Command::Set { key, value } => {
                        map.insert(key.into(), value.into());
                    }
                    Command::Rm { key } => {
                        map.remove(key.into());
                    }
                    _ => panic!("Invalid log"),
                }
            }
        }
        Ok(Self { map, writer })
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
    /// # let mut store = KvStore::new();
    /// # store.set("key".to_string(), "value".to_string());
    /// # assert_eq!(store.get("key".to_owned()), Some("value".to_string()));
    /// ```
    pub fn get(&mut self, k: String) -> Result<Option<String>> {
        // FIXME: map would only contain the file pointer this needs to be modified to read from the file pointer and get the value
        if let Some(val) = self.map.get(&k) {
            return Ok(Some(val.to_owned()));
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
    /// # let mut store = KvStore::new();
    /// # store.set("SomeKey".to_owned(), "Val".to_owned());
    /// ```
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let cmd = Command::Set {
            key: key.clone(),
            value: value.clone(),
        };
        self.write_log(cmd).unwrap();
        self.map.insert(key, value);
        Ok(())
    }

    fn write_log(&mut self, cmd: Command) -> io::Result<()> {
        let bytes = bson::to_vec(&cmd).expect("Bson Serialization failed");
        self.writer.write(&(bytes.len() as u32).to_le_bytes())?;
        self.writer.write(&bytes)?;
        Ok(())
    }

    /// Removes a key and its associated value from the store
    ///
    /// # Arguments
    ///
    /// * `key` - The key to remove
    /// ```rust
    /// # use kvs::KvStore;
    /// # let mut store = KvStore::new();
    /// # store.set("key".to_string(), "value".to_string());
    /// # store.remove("key".to_owned());
    /// # assert_eq!(store.get("key".to_owned()), None);
    /// ```
    pub fn remove(&mut self, key: String) -> Result<()> {
        let cmd = Command::Rm { key: key.clone() };
        if let Some(_) = self.map.remove(&key) {
            self.write_log(cmd).unwrap();
            return Ok(());
        }
        Err(anyhow::anyhow!("Key not found"))
    }
}

#[cfg(test)]
mod tests {

    use crate::KvStore;

    #[test]
    fn test_set() {
        let mut kv = KvStore::open(&std::env::current_dir().unwrap().as_path()).unwrap();
        kv.set("test".into(), "567".into()).unwrap();
        kv.set("test".into(), "67".into()).unwrap();
        kv.set("test".into(), "5567".into()).unwrap();
        kv.set("test".into(), "shubh".into()).unwrap();
        assert_eq!(kv.get("test".into()).unwrap(), Some("shubh".to_string()));
    }
}
