use std::io;

#[derive(Debug)]
pub enum KvsError {
    Message(String),
    KeyNotFound,
    InvalidCommand,
    Io(io::Error),
    Serde(serde_json::Error),
}

impl From<io::Error> for KvsError {
    fn from(value: io::Error) -> Self {
        KvsError::Io(value)
    }
}

impl From<serde_json::Error> for KvsError {
    fn from(value: serde_json::Error) -> Self {
        KvsError::Serde(value)
    }
}

pub type Result<T> = std::result::Result<T, KvsError>;
