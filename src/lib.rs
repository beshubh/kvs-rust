//! A simple key-value store implementation.

pub mod client;
pub mod common;
pub mod engines;
pub mod error;
pub mod server;

pub use engines::{KvStore, KvsEngine};
pub use error::{KvsError, Result};
