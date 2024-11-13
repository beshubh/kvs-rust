//! A simple key-value store implementation.
//!
//! This crate provides a basic in-memory key-value store for storing
//! string pairs. It supports basic operations like get, set, and remove

#![deny(missing_docs)]
use failure::Fail;
use std::{collections::HashMap, path::Path};

/// Custom Error type
#[derive(Fail, Debug)]
pub enum KVError {
    /// for any IO Error
    #[fail(display = "{}", _0)]
    Io(#[cause] std::io::Error),
}

/// Custom Result Type
pub type Result<T> = std::result::Result<T, KVError>;

/// A key-value store for storing string pairs
pub struct KvStore {
    map: HashMap<String, String>,
}

impl KvStore {
    ///Creates a new empty KvStore instance
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use kvs::KvStore;
    /// let store = KvStore::new();
    /// ```
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
        }
    }

    /// Open the path & builds a KvStore
    pub fn open(_path: &Path) -> Result<Self> {
        panic!("unimplemented")
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
    pub fn get(&self, key: String) -> Result<Option<String>> {
        Ok(self.map.get(&key).cloned())
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
    pub fn set(&mut self, key: String, val: String) -> Result<()> {
        self.map.insert(key, val);
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
        self.map.remove(&key);
        Ok(())
    }
}
