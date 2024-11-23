use clap::Parser;
use kvs::client;
use kvs::common;
use kvs::Result;
use std::env;
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
    let cli = Cli::parse();
    let addr = common::parse_address(cli.address.unwrap())?;
    let stream = TcpStream::connect(&addr);
    match stream {
        Err(e) => eprintln!("count not connect to server at: {}, err: {}", addr, e),
        Ok(stream) => {
            println!("connected to server at: {}", addr);
        }
    }
    Ok(())
}