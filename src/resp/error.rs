use std;
use std::fmt::{self, Display};

use serde::{de, ser};

pub type Result<T> = std::result::Result<T, RespError>;

#[derive(Debug)]
pub enum RespError {
    Message(String),

    Eof,
    Syntax,
    ExpectedCRLF,
    ExpectedArray,
    ExpectedInteger,
    ExpectedSimpleString,
    ExpectedBulkString,
    ExpectedBoolean,
    TrailingCharacters,
    ExpectedNull,
}

impl ser::Error for RespError {
    fn custom<T>(msg: T) -> Self
    where
        T: Display,
    {
        RespError::Message(msg.to_string())
    }
}

impl de::Error for RespError {
    fn custom<T>(msg: T) -> Self
    where
        T: Display,
    {
        RespError::Message(msg.to_string())
    }
}

impl Display for RespError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RespError::Message(msg) => f.write_str(&msg),
            RespError::Eof => f.write_str("unexpected end of input"),
            RespError::Syntax => f.write_str("syntax does not follow RESP"),
            RespError::ExpectedCRLF => f.write_str("expected (CRLF)/\r/\n in the end"),
            RespError::ExpectedArray => f.write_str("invalid content expected an array"),
            RespError::ExpectedInteger => f.write_str("invalid content expected an integer"),
            RespError::ExpectedSimpleString => {
                f.write_str("invalid content expected simple strings")
            }
            RespError::TrailingCharacters => {
                f.write_str("trailing characaters left in input while deserializing")
            }
            RespError::ExpectedBoolean => f.write_str("expected boolean"),
            RespError::ExpectedBulkString => f.write_str("expted bulkstring"),
            RespError::ExpectedNull => f.write_str("expected null"),
        }
    }
}

impl std::error::Error for RespError {}
