use std::env;
use std::net::TcpStream;

use crate::common::tcp_send_message;
use crate::common::KvsCommand;
use crate::KvStore;
use crate::KvsError;
use crate::Result;
use clap::Subcommand;
use log::debug;
use log::error;
use serde::{Deserialize, Serialize};

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
    let message: String = match command {
        KvsCommand::Ping => "+OK\r\n".into(),
        KvsCommand::Set(key, value) => {
            store.set(key.into(), value.into())?;
            "+OK\r\n".into()
        }
        KvsCommand::Get(key) => {
            let mut m = String::from("-1\r\n");
            if let Some(value) = store.get(key.into())? {
                m = format!("${}\r\n{}\r\n", value.len(), value);
            }
            m
        }
        KvsCommand::Rm(key) => {
            let mut m = String::from("+OK\r\n");
            if let Err(e) = store.remove(key.into()) {
                match e {
                    KvsError::KeyNotFound => {
                        m = String::from("-Key not found\r\n");
                    }
                    e => {
                        debug!("Something went wrong on key remove: {:?}", e)
                    }
                }
            }
            m
        }
        KvsCommand::Version => env!("CARGO_PKG_VERSION").into(),
    };
    debug!("message to send: {}", message);
    if let Err(e) = tcp_send_message(&stream, message) {
        error!("error sending message: {:?}", e);
    }
    Ok(())
}
