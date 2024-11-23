use std::net::TcpListener;

use log::{error, info};

use clap::Parser;
use kvs::{common, server, Result};

#[derive(Parser, Debug, Clone)]
#[command(author = "Shubh")]
#[command(version = env!("CARGO_PKG_VERSION"))]
#[command(name= env!("CARGO_PKG_NAME"))]
#[command(about = env!("CARGO_PKG_DESCRIPTION"))]
struct Cli {
    #[command(subcommand)]
    cmd: Option<server::Command>,
    #[arg(long = "addr", global = true, default_value = "127.0.0.1:6969")]
    address: Option<String>,

    #[arg(long = "engine", global = true, default_value = "kvs")]
    engine: Option<String>,
}

fn main() -> Result<()> {
    dotenv::dotenv().ok();
    env_logger::init();
    let cli = Cli::parse();
    let addr = common::parse_address(cli.address.unwrap())?;
    info!("{}", env!("CARGO_PKG_VERSION"));
    info!("{}", addr);
    let listener = TcpListener::bind(&addr);
    if let Err(e) = &listener {
        error!("could not bind to address: {}, error: {}", addr, e);
        return Err(kvs::KvsError::Message(" could not bind to address".into()));
    }
    let listener = listener.unwrap();
    match listener.accept() {
        Err(e) => eprintln!("could not bind to address: {}, err:{}", addr, e),
        Ok(stream) => {
            info!("new connection");
        }
    }
    Ok(())
}
