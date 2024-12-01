use core::str;
use std::env;
use std::io::BufReader;
use std::io::Read;
use std::net::TcpListener;
use std::net::TcpStream;
use std::net::ToSocketAddrs;

use clap::Subcommand;
use log::debug;
use log::error;
use serde::{Deserialize, Serialize};

use crate::common;
use crate::common::tcp_send_message;
use crate::common::KvsCommand;
use crate::KvsEngine;
use crate::{KvsError, Result};

#[derive(Subcommand, Deserialize, Serialize, Debug, Clone)]
pub enum Command {
    #[command(name = "-V")]
    Version,
}

pub fn handle_command(
    command: &KvsCommand,
    stream: &mut TcpStream,
    store: impl KvsEngine,
) -> Result<()> {
    let message: String = match command {
        KvsCommand::Ping => "+PONG\r\n".into(),
        KvsCommand::Set(key, value) => {
            store.set(key.into(), value.into())?;
            "+OK\r\n".into()
        }
        KvsCommand::Get(key) => {
            let mut m = String::from("-Key not found\r\n");
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
    if let Err(e) = tcp_send_message(&stream, &message) {
        error!("error sending message: {:?}", e);
    }

    Ok(())
}

pub struct KvsServer<E: KvsEngine> {
    engine: E,
}

impl<E: KvsEngine> KvsServer<E> {
    pub fn new(engine: E) -> Self {
        KvsServer { engine }
    }

    pub fn run<A: ToSocketAddrs>(&mut self, addr: A) -> Result<()> {
        let listener = TcpListener::bind(addr)?;
        for stream in listener.incoming() {
            match stream {
                Err(e) => error!("could not bind to addres, err:{}", e),
                Ok(stream) => {
                    if let Err(e) = self.serve(stream) {
                        error!("Error handling client: {:?}", e);
                    }
                }
            }
        }
        Ok(())
    }

    fn serve(&mut self, tcp: TcpStream) -> Result<()> {
        let mut reader = BufReader::new(&tcp);
        loop {
            let mut buf: Vec<u8> = vec![0; 1024];
            match reader.read(&mut buf) {
                Ok(0) => {
                    log::info!("connection closed");
                    break;
                }
                Ok(size) => {
                    let s = std::str::from_utf8(&buf[..size]).unwrap();
                    let resp = common::parse_resp(s).unwrap().1;
                    let command = common::parse_command(&resp).unwrap();
                    self.handle_command(&command, &tcp).unwrap();
                }
                Err(e) => {
                    error!("Error reading from client: {}", e);
                    break;
                }
            }
        }
        Ok(())
    }

    fn handle_command(&self, command: &KvsCommand, stream: &TcpStream) -> Result<()> {
        let message: String = match command {
            KvsCommand::Ping => "+PONG\r\n".into(),
            KvsCommand::Set(key, value) => {
                self.engine.set(key.into(), value.into())?;
                "+OK\r\n".into()
            }
            KvsCommand::Get(key) => {
                let mut m = String::from("-Key not found\r\n");
                if let Some(value) = self.engine.get(key.into())? {
                    m = format!("${}\r\n{}\r\n", value.len(), value);
                }
                m
            }
            KvsCommand::Rm(key) => {
                let mut m = String::from("+OK\r\n");
                if let Err(e) = self.engine.remove(key.into()) {
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
        if let Err(e) = tcp_send_message(&stream, &message) {
            log::error!("error sending message: {:?}", e);
        } else {
            log::debug!("message sent: {}", message);
        }
        Ok(())
    }
}
