use std::{
    env,
    io::{self, BufReader, Read},
    net::{TcpListener, TcpStream},
};

use log::{error, info};

use clap::Parser;
use kvs::{common, server, Result};
use kvs::{resp, KvStore};

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

fn handle_client(mut stream: TcpStream, store: &mut KvStore) -> io::Result<()> {
    let mut reader = BufReader::new(stream.try_clone()?);
    loop {
        let mut buf: Vec<u8> = Vec::new();
        match reader.read_to_end(&mut buf) {
            Ok(0) => {
                info!("connection closed");
                break;
            }
            Ok(size) => {
                let s = std::str::from_utf8(&buf[..size]).unwrap();
                let resp = common::parse_resp(s).unwrap().1;
                let command = common::parse_command(&resp).unwrap();
                server::handle_command(&command, &mut stream, store).unwrap();
            }
            Err(e) => {
                error!("Error reading from client: {}", e);
                break;
            }
        }
    }
    Ok(())
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
    let mut store = KvStore::open(&env::current_dir().unwrap()).unwrap();
    let listener = listener.unwrap();
    for stream in listener.incoming() {
        match stream {
            Err(e) => error!("could not bind to address: {}, err:{}", addr, e),
            Ok(stream) => {
                if let Err(e) = handle_client(stream, &mut store) {
                    error!("error handling client: {}", e);
                }
            }
        }
    }
    Ok(())
}
