use std::net::TcpStream;

use clap::Subcommand;
use log::debug;
use serde::{Deserialize, Serialize};

use crate::common::tcp_send_message;
use crate::common::KvsCommand;
use crate::KvStore;
use crate::KvsError;
use crate::Result;
use log::error;

#[derive(Subcommand, Deserialize, Serialize, Debug, Clone)]
pub enum Command {
    #[command(name = "-V")]
    Version,
}

pub fn handle_command(
    command: &KvsCommand,
    stream: &mut TcpStream,
    store: &mut KvStore,
) -> Result<()> {
    match command {
        KvsCommand::Ping => {
            if let Err(e) = tcp_send_message(&stream, "+PONG\r\n".into()) {
                error!("error sending message: {:?}", e);
            }
        }
        KvsCommand::Set(key, value) => {
            store.set(key.into(), value.into())?;
            if let Err(e) = tcp_send_message(&stream, "+OK\r\n".into()) {
                error!("error sending message: {:?}", e);
            }
        }
        KvsCommand::Get(key) => {
            let mut message = String::from("-1\r\n");
            if let Some(value) = store.get(key.into())? {
                message = format!("${}\r\n{}\r\n", value.len(), value);
            }
            if let Err(e) = tcp_send_message(&stream, message) {
                error!("error sending message: {:?}", e);
            }
        }
        KvsCommand::Rm(key) => {
            let mut message = String::from("+OK\r\n");
            if let Err(e) = store.remove(key.into()) {
                match e {
                    KvsError::KeyNotFound => {
                        message = String::from("-Key not found\r\n");
                    }
                    e => {
                        debug!("Something went wrong on key remove: {:?}", e)
                    }
                }
            }
            if let Err(e) = tcp_send_message(&stream, message) {
                error!("error sending tcp message: {:?}", e);
            }
        }
    }
    Ok(())
}
