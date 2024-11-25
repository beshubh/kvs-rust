use std::net::TcpStream;

use crate::common::tcp_read_message;
use crate::common::tcp_send_message;
use crate::resp;
use crate::KvsError;
use crate::Result;
use clap::Subcommand;
use log::{debug, info};
use serde::{Deserialize, Serialize};

#[derive(Subcommand, Deserialize, Serialize, Debug, Clone)]
#[serde(rename_all = "lowercase")]
pub enum Command {
    #[command[name="PING"]]
    Ping,
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

pub fn parse_address(address: String) -> Result<String> {
    let parts: Vec<&str> = address.split(":").collect();

    if parts.len() > 2 {
        eprintln!("invalid address");
        return Err(KvsError::Message("Invalid address attribute".into()));
    }
    let addr = parts[0];
    let port = parts[1].to_string().parse::<u32>().unwrap();
    Ok(format!("{}:{}", addr, port))
}

pub fn handle_command(cmd: &Command, stream: &mut TcpStream) -> Result<()> {
    let resp_value = match &cmd {
        Command::Ping => resp::RespValue::SimpleString("PING".into()),
        Command::Set { key, value } => resp::RespValue::Array(Some(vec![
            resp::RespValue::BulkString(Some(b"set".into())),
            resp::RespValue::BulkString(Some(key.as_bytes().into())),
            resp::RespValue::BulkString(Some(value.as_bytes().into())),
        ])),
        Command::Get { key } => resp::RespValue::Array(Some(vec![
            resp::RespValue::BulkString(Some(b"get".into())),
            resp::RespValue::BulkString(Some(key.as_bytes().into())),
        ])),
        Command::Rm { key } => resp::RespValue::Array(Some(vec![
            resp::RespValue::BulkString(Some(b"rm".into())),
            resp::RespValue::BulkString(Some(key.as_bytes().into())),
        ])),
        Command::Version => resp::RespValue::SimpleString("V".into()),
    };
    let message = resp::to_string(&resp_value).unwrap();
    debug!("Message to send: {}", message);
    tcp_send_message(&stream, message)?;
    debug!("message sent to server");
    Ok(())
}
