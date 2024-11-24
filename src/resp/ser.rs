use serde::{ser, Serialize};

use crate::resp::error::{RespError, Result};

pub struct Serializer {
    output: String,
}

pub fn to_string<T>(value: &T) -> Result<String>
where
    T: Serialize,
{
    let mut serializer = Serializer {
        output: String::new(),
    };
    let output = value.serialize(&mut serializer)?;
    Ok(output)
}

impl<'a> ser::Serializer for &'a mut Serializer {
    type Ok = String;
    type Error = RespError;

    type SerializeSeq = SeqSerializer;
    type SerializeTuple = ser::Impossible<String, RespError>;
    type SerializeTupleStruct = ser::Impossible<String, RespError>;
    type SerializeTupleVariant = ser::Impossible<String, RespError>;
    type SerializeMap = ser::Impossible<String, RespError>;
    type SerializeStruct = ser::Impossible<String, RespError>;
    type SerializeStructVariant = ser::Impossible<String, RespError>;

    fn serialize_char(self, _v: char) -> Result<String> {
        Err(RespError::Message("RESP does not support char".into()))
    }

    fn serialize_f64(self, _v: f64) -> Result<String> {
        Err(RespError::Message("RESP does not support float".into()))
    }

    fn serialize_f32(self, _v: f32) -> Result<String> {
        Err(RespError::Message("RESP does not support float".into()))
    }

    fn serialize_u64(self, v: u64) -> Result<String> {
        let mut output = String::from(":");
        output += &v.to_string();
        output += "\r\n";
        Ok(output)
    }

    fn serialize_u32(self, v: u32) -> Result<String> {
        self.serialize_u64(u64::from(v))
    }

    fn serialize_u16(self, v: u16) -> Result<String> {
        self.serialize_u64(u64::from(v))
    }

    fn serialize_i16(self, v: i16) -> Result<String> {
        self.serialize_i64(i64::from(v))
    }

    fn serialize_i32(self, v: i32) -> Result<String> {
        self.serialize_i64(i64::from(v))
    }

    fn serialize_i64(self, v: i64) -> Result<String> {
        let output = format!(":{}\r\n", v);
        Ok(output)
    }

    fn serialize_u8(self, v: u8) -> Result<String> {
        self.serialize_u64(u64::from(v))
    }

    fn serialize_i8(self, v: i8) -> Result<String> {
        self.serialize_i64(i64::from(v))
    }

    fn serialize_bool(self, v: bool) -> Result<String> {
        let b = if v { "t" } else { "f" };
        let output = format!("#{}\r\n", b);
        Ok(output)
    }

    fn serialize_str(self, v: &str) -> Result<String> {
        println!("Okay I am comming here: {}", v);
        let output = format!("+{}\r\n", v);
        println!("Ok, this is the output: {}", output);
        Ok(output)
    }

    fn serialize_bytes(self, v: &[u8]) -> Result<String> {
        let mut output = format!("${}\r\n", v.len());
        for &byte in v {
            output.push(byte as char);
        }
        output.push_str("\r\n");
        Ok(output)
    }

    fn serialize_none(self) -> Result<String> {
        Ok("_\r\n".into())
    }

    fn serialize_unit(self) -> Result<String> {
        self.serialize_none()
    }

    fn serialize_some<T>(self, value: &T) -> Result<String>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(self)
    }

    fn serialize_unit_struct(self, _name: &'static str) -> Result<String> {
        self.serialize_unit()
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
    ) -> Result<String> {
        self.serialize_str(variant)
    }

    fn serialize_newtype_struct<T>(self, _name: &'static str, value: &T) -> Result<String>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(self)
    }

    fn serialize_newtype_variant<T>(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        value: &T,
    ) -> Result<String>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(self)
    }

    fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq> {
        match len {
            None => Ok(SeqSerializer {
                output: "*-\r\n".into(),
                elements: Vec::new(),
            }),
            Some(_) => Ok(SeqSerializer {
                output: String::new(),
                elements: Vec::new(),
            }),
        }
    }

    fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple> {
        Err(RespError::Message("RESP doesn't support tuples".into()))
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleStruct> {
        Err(RespError::Message(
            "RESP doesn't support tuple struct".into(),
        ))
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleVariant> {
        Err(RespError::Message(
            "RESP doesn't support tuple variants".into(),
        ))
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant> {
        Err(RespError::Message(
            "RESP doesn't support tuple variants".into(),
        ))
    }

    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap> {
        Err(RespError::Message(
            "RESP doesn't support tuple variants".into(),
        ))
    }

    fn serialize_struct(self, _name: &'static str, _len: usize) -> Result<Self::SerializeStruct> {
        Err(RespError::Message(
            "RESP doesn't support tuple variants".into(),
        ))
    }
}

pub struct SeqSerializer {
    pub output: String,
    pub elements: Vec<String>,
}

impl ser::SerializeSeq for SeqSerializer {
    type Ok = String;
    type Error = RespError;

    fn serialize_element<T>(&mut self, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        let mut ser = Serializer {
            output: String::new(),
        };
        let element = value.serialize(&mut ser)?;
        self.elements.push(element);
        Ok(())
    }

    fn end(self) -> Result<Self::Ok> {
        let mut output = format!("*{}\r\n", self.elements.len());
        for element in self.elements {
            output += &element;
        }

        Ok(output)
    }
}

#[test]
fn test_enum() -> Result<()> {
    use crate::resp::ser::to_string;
    use crate::resp::RespValue;

    let x = RespValue::Array(Some(vec![
        RespValue::Integer(69),
        RespValue::SimpleString("OK".into()),
    ]));
    let resp_string = to_string(&x)?;
    println!("{:?}", resp_string);
    Ok(())
}
