use std::ops::AddAssign;
use std::ops::MulAssign;

use crate::resp::error::RespError;
use crate::resp::error::Result;
use log::debug;
use serde::{de, Deserialize};

const ARRAY_PREFIX: char = '*';
const CRLF: &str = "\r\n";

pub struct SeqAccess<'a, 'de: 'a> {
    de: &'a mut Deserializer<'de>,
    len: usize,
    current: usize,
}

impl<'a, 'de> SeqAccess<'a, 'de> {
    pub fn new(de: &'a mut Deserializer<'de>, len: usize) -> Self {
        SeqAccess {
            de,
            len,
            current: 0,
        }
    }
}

impl<'de, 'a> de::SeqAccess<'de> for SeqAccess<'a, 'de> {
    type Error = RespError;

    fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>>
    where
        T: de::DeserializeSeed<'de>,
    {
        if self.current >= self.len {
            return Ok(None);
        }
        self.current += 1;
        seed.deserialize(&mut *self.de).map(Some)
    }
}
pub struct Deserializer<'de> {
    pub input: &'de str,
}

impl<'de> Deserializer<'de> {
    pub fn from_str(input: &'de str) -> Self {
        Deserializer { input }
    }
}

pub fn from_str<'a, T>(s: &'a str) -> Result<T>
where
    T: Deserialize<'a>,
{
    let mut deserializer = Deserializer::from_str(s);
    let t = T::deserialize(&mut deserializer)?;
    if deserializer.input.is_empty() {
        Ok(t)
    } else {
        Err(RespError::TrailingCharacters)
    }
}

impl<'de> Deserializer<'de> {
    pub fn peek_char(&mut self) -> Result<char> {
        self.input.chars().next().ok_or(RespError::Eof)
    }

    pub fn next_char(&mut self) -> Result<char> {
        let ch = self.peek_char()?;
        self.input = &self.input[ch.len_utf8()..];
        Ok(ch)
    }

    pub fn parse_bool(&mut self) -> Result<bool> {
        if self.input.starts_with("#t\r\n") {
            self.input = &self.input["#t\r\n".len()..];
            return Ok(true);
        } else if self.input.starts_with("#f\r\n") {
            self.input = &self.input["#f\r\n".len()..];
            return Ok(false);
        }
        Err(RespError::ExpectedBoolean)
    }

    pub fn parse_unsigned<T>(&mut self) -> Result<T>
    where
        T: AddAssign<T> + MulAssign + From<u8>,
    {
        if self.next_char()? != ':' {
            return Err(RespError::ExpectedInteger);
        }
        let sign = self.peek_char()?;
        if sign == '+' {
            self.next_char()?;
        }

        let mut int = match self.next_char()? {
            ch @ '0'..='9' => T::from(ch as u8 - b'0'),
            _ => {
                return Err(RespError::ExpectedInteger);
            }
        };
        loop {
            match self.input.chars().next() {
                Some(ch @ '0'..='9') => {
                    self.input = &self.input[1..];
                    int *= T::from(10);
                    int += T::from(ch as u8 - b'0');
                }
                _ => {
                    return Ok(int);
                }
            }
        }
    }

    pub fn parse_signed<T>(&mut self) -> Result<T>
    where
        T: AddAssign<T> + MulAssign + From<i8>,
    {
        unimplemented!();
    }

    pub fn parse_string(&mut self) -> Result<&'de str> {
        if self.next_char()? != '+' {
            return Err(RespError::ExpectedSimpleString);
        }
        match self.input.find(CRLF) {
            Some(len) => {
                let s = &self.input[..len];
                self.input = &self.input[len + 1..];
                Ok(s)
            }
            None => Err(RespError::Eof),
        }
    }

    pub fn parse_bytes(&mut self) -> Result<Vec<u8>> {
        if self.next_char()? != '$' {
            return Err(RespError::ExpectedBulkString);
        }
        let mut bulk_str_len = match self.next_char()? {
            ch @ '0'..='9' => u64::from(ch as u8 - b'0'),
            _ => {
                return Err(RespError::Message(
                    "bulk strings should start with unsigned integer length".into(),
                ))
            }
        };

        loop {
            match self.peek_char()? {
                ch @ '0'..='9' => {
                    self.next_char()?;
                    bulk_str_len = bulk_str_len * 10 + u64::from(ch as u8 - b'0');
                }
                '\r' => {
                    self.next_char()?; // consume \r
                    self.next_char()?; // consume \n
                    break;
                }
                _ => return Err(RespError::ExpectedInteger),
            }
        }
        let mut output: Vec<u8> = Vec::new();
        while bulk_str_len > 0 {
            output.push(self.next_char()? as u8);
            bulk_str_len -= 1;
        }
        self.next_char()?;
        self.next_char()?;
        Ok(output)
    }
}

impl<'de, 'a> de::Deserializer<'de> for &'a mut Deserializer<'de> {
    type Error = RespError;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        match self.peek_char()? {
            ':' => self.deserialize_i64(visitor),
            '#' => self.deserialize_bool(visitor),
            '$' => self.deserialize_bytes(visitor),
            '+' => self.deserialize_str(visitor),
            '*' => self.deserialize_seq(visitor),
            _ => Err(RespError::Syntax),
        }
    }

    fn deserialize_enum<V>(
        self,
        _name: &'static str,
        _variants: &'static [&'static str],
        _visitor: V,
    ) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        unimplemented!()
    }

    fn deserialize_i64<V>(self, visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        visitor.visit_i64(self.parse_signed()?)
    }

    fn deserialize_i32<V>(self, visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        visitor.visit_i32(self.parse_signed()?)
    }
    fn deserialize_i16<V>(self, visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        visitor.visit_i16(self.parse_signed()?)
    }

    fn deserialize_i8<V>(self, visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        visitor.visit_i8(self.parse_signed()?)
    }

    fn deserialize_u64<V>(self, visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        visitor.visit_u64(self.parse_unsigned()?)
    }

    fn deserialize_u32<V>(self, visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        visitor.visit_u32(self.parse_unsigned()?)
    }
    fn deserialize_u16<V>(self, visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        visitor.visit_u16(self.parse_unsigned()?)
    }

    fn deserialize_u8<V>(self, visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        visitor.visit_u8(self.parse_unsigned()?)
    }

    fn deserialize_bool<V>(self, visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        visitor.visit_bool(self.parse_bool()?)
    }

    // Float parsing is stupidly hard.
    fn deserialize_f32<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        unimplemented!()
    }

    // Float parsing is stupidly hard.
    fn deserialize_f64<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        unimplemented!()
    }

    // The `Serializer` implementation on the previous page serialized chars as
    // single-character strings so handle that representation here.
    fn deserialize_char<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        // Parse a string, check that it is one character, call `visit_char`.
        unimplemented!()
    }

    // Refer to the "Understanding deserializer lifetimes" page for information
    // about the three deserialization flavors of strings in Serde.
    fn deserialize_str<V>(self, visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        visitor.visit_borrowed_str(self.parse_string()?)
    }

    fn deserialize_string<V>(self, visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        self.deserialize_str(visitor)
    }

    fn deserialize_bytes<V>(self, visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        visitor.visit_bytes(&self.parse_bytes()?)
    }

    fn deserialize_byte_buf<V>(self, _visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        unimplemented!()
    }
    fn deserialize_option<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        if self.input.starts_with("_\r\n") {
            self.input = &self.input["_\r\n".len()..];
            visitor.visit_none()
        } else {
            visitor.visit_some(self)
        }
    }

    // In Serde, unit means an anonymous value containing no data.
    fn deserialize_unit<V>(self, visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        if self.input.starts_with("_\r\n") {
            self.input = &self.input["_\r\n".len()..];
            visitor.visit_unit()
        } else {
            Err(RespError::ExpectedNull)
        }
    }

    // Unit struct means a named value containing no data.
    fn deserialize_unit_struct<V>(self, _name: &'static str, visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        self.deserialize_unit(visitor)
    }

    fn deserialize_newtype_struct<V>(
        self,
        _name: &'static str,
        _visitor: V,
    ) -> std::result::Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        unimplemented!()
    }

    fn deserialize_seq<V>(self, visitor: V) -> Result<V::Value>
    where
        V: de::Visitor<'de>,
    {
        if self.next_char()? != ARRAY_PREFIX {
            return Err(RespError::ExpectedArray);
        }
        let mut len = match self.next_char()? {
            ch @ '0'..='9' => u64::from(ch as u8 - b'0'),
            _ => return Err(RespError::ExpectedInteger),
        };
        loop {
            match self.peek_char()? {
                ch @ '0'..='9' => {
                    self.next_char()?;
                    len = len * 10 + u64::from(ch as u8 - b'0');
                }
                '\r' => {
                    // Consume \r\n
                    self.next_char()?; // consume \r
                    self.next_char()?; // consume \n
                    break;
                }
                _ => return Err(RespError::ExpectedInteger),
            }
        }
        let seq = SeqAccess::new(self, len as usize);
        visitor.visit_seq(seq)
    }

    fn deserialize_identifier<V>(self, _visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        unimplemented!()
    }
    fn deserialize_map<V>(self, _visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        unimplemented!()
    }
    fn deserialize_tuple_struct<V>(
        self,
        _name: &'static str,
        _len: usize,
        _visitor: V,
    ) -> std::result::Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        unimplemented!()
    }
    fn deserialize_struct<V>(
        self,
        _name: &'static str,
        _fields: &'static [&'static str],
        _visitor: V,
    ) -> std::result::Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        unimplemented!()
    }

    fn deserialize_ignored_any<V>(self, _visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        unimplemented!()
    }

    fn deserialize_tuple<V>(
        self,
        _len: usize,
        _visitor: V,
    ) -> std::result::Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        unimplemented!()
    }
}
