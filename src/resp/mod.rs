mod de;
mod error;
mod ser;

// pub use de::{from_string, DeSerializer};
pub use crate::resp::de::{from_str, Deserializer};
pub use crate::resp::ser::{to_string, Serializer};
use serde::{ser::SerializeSeq, Deserialize, Serialize};

#[derive(Deserialize)]
pub enum RespValue {
    SimpleString(String),        // tuple variant
    Err(String),                 // tuple variant
    Integer(u64),                // tuple variant
    BulkString(Option<Vec<u8>>), // tuple variant
    Array(Option<Vec<RespValue>>),
}

impl Serialize for RespValue {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            RespValue::SimpleString(s) => serializer.serialize_str(s),
            RespValue::Err(e) => serializer.serialize_str(e),
            RespValue::Integer(i) => serializer.serialize_u64(*i),
            RespValue::BulkString(opt) => match opt {
                None => serializer.serialize_str("$-1\r\n"),
                Some(bytes) => serializer.serialize_bytes(&bytes),
            },
            RespValue::Array(opt) => match opt {
                None => serializer.serialize_str("*-1\r\n"),
                Some(arr) => {
                    let mut seq = serializer.serialize_seq(Some(arr.len()))?;
                    for value in arr {
                        seq.serialize_element(value)?;
                    }
                    seq.end()
                }
            },
        }
    }
}
