use clap::Parser;
use env_logger;
use kvs::client;
use kvs::common;
use kvs::resp;
use kvs::Result;
use log::{error, info};
use std::env;
use std::io::Write;
use std::net::TcpStream;

#[derive(Parser, Debug, Clone)]
#[command(author = "Shubh")]
#[command(version = env!("CARGO_PKG_VERSION"))]
#[command(name= env!("CARGO_PKG_NAME"))]
#[command(about = env!("CARGO_PKG_DESCRIPTION"))]
struct Cli {
    #[command(subcommand)]
    cmd: client::Command,

    #[arg(long = "addr", global = true, default_value = "127.0.0.1:6969")]
    address: Option<String>,
}

fn main() -> Result<()> {
    dotenv::dotenv().ok();
    env_logger::init();
    let cli = Cli::parse();
    let addr = common::parse_address(cli.address.unwrap())?;
    let stream = TcpStream::connect(&addr);
    match stream {
        Err(e) => error!("count not connect to server at: {}, err: {}", addr, e),
        Ok(mut stream) => {
            info!("connected to server at: {}", addr);
            info!("connect to cli cmd: {:?}", cli.cmd);
            match &cli.cmd {
                client::Command::Ping => {
                    let ping = resp::RespValue::SimpleString("PING".into());
                    let value = resp::to_string(&ping).unwrap();
                    stream.write(value.as_bytes()).unwrap();
                }
                _ => {
                    info!("command: {:?}", cli.cmd);
                }
            }
        }
    }
    Ok(())
}
