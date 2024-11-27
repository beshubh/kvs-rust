pub use crate::Result;
pub trait KvsEngine {
    /// Get the corresponding value for a key
    /// It returns an option that will be none
    /// if key does not exists
    fn get(&mut self, key: String) -> Result<Option<String>>;

    /// Set the value at key, like HashMap
    /// If previous value was there it will be overwritten
    fn set(&mut self, key: String, value: String) -> Result<()>;

    /// Remove the key, value pair at key
    /// # Errors
    /// KeyNotFound if key is not there in the map
    fn remove(&mut self, key: String) -> Result<()>;
}

mod kvs;
mod sled;
pub use self::kvs::KvStore;
pub use self::sled::SledStore;
