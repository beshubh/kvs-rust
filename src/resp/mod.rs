mod de;
mod error;
mod ser;

// pub use de::{from_string, DeSerializer};
pub use crate::resp::de::{Deserializer, SeqAccess};
pub use crate::resp::ser::{to_string, Serializer};
use serde::{ser::SerializeSeq, Deserialize, Serialize};

#[derive(Deserialize, Debug)]
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

pub fn from_str<'a>(s: &'a str) -> error::Result<RespValue> {
    let mut deserializer = Deserializer { input: &s };
    match deserializer.peek_char()? {
        ':' => Ok(RespValue::Integer(
            deserializer.parse_unsigned::<u64>().unwrap(),
        )),
        '$' => Ok(RespValue::BulkString(Some(
            deserializer.parse_bytes().unwrap(),
        ))),
        '+' => Ok(RespValue::SimpleString(
            deserializer.parse_string().unwrap().to_string(),
        )),
        '*' => {
            if deserializer.next_char()? != '$' {
                return Err(error::RespError::ExpectedArray);
            }
            let mut len = match deserializer.next_char()? {
                ch @ '0'..='9' => u64::from(ch as u8 - b'0'),
                _ => return Err(error::RespError::ExpectedInteger),
            };
            loop {
                match deserializer.peek_char()? {
                    ch @ '0'..='9' => {
                        deserializer.next_char()?;
                        len = len * 10 + u64::from(ch as u8 - b'0');
                    }
                    '\r' => {
                        // Consume \r\n
                        deserializer.next_char()?; // consume \r
                        deserializer.next_char()?; // consume \n
                        break;
                    }
                    _ => return Err(error::RespError::ExpectedInteger),
                }
            }
            let mut output: Vec<RespValue> = Vec::new();
            loop {
                match deserializer.peek_char()? {
                    ':' => output.push(RespValue::Integer(
                        deserializer.parse_unsigned::<u64>().unwrap(),
                    )),
                    '$' => output.push(RespValue::BulkString(Some(
                        deserializer.parse_bytes().unwrap(),
                    ))),
                    '+' => output.push(RespValue::SimpleString(
                        deserializer.parse_string().unwrap().to_string(),
                    )),
                    _ => break,
                }
            }

            Ok(RespValue::Array(Some(output)))
        }
        _ => Err(error::RespError::Syntax),
    }
}
