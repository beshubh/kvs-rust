use std::env;

use clap::Parser;
use kvs::{client, KvStore};

#[derive(Parser, Debug, Clone)]
#[command(author = "Shubh")]
#[command(version = env!("CARGO_PKG_VERSION"))]
#[command(name= env!("CARGO_PKG_NAME"))]
#[command(about = env!("CARGO_PKG_DESCRIPTION"))]
struct Cli {
    #[command(subcommand)]
    cmd: client::Command,
}

fn main() -> kvs::Result<()> {
    let cli = Cli::parse();
    let mut store = KvStore::open(std::env::current_dir().unwrap().as_path()).unwrap();
    match &cli.cmd {
        client::Command::Get { key } => {
            let val = store.get(key.into());
            if val.is_err() {
                println!("Error: {:?}", val);
            }
            let val = val.unwrap();
            if let Some(value) = val {
                println!("{}", value);
            } else {
                print!("Key not found");
            }
        }
        client::Command::Set { key, value } => store.set(key.into(), value.into())?,
        client::Command::Rm { key } => {
            let val = store.remove(key.into());
            if let Err(_) = val {
                print!("Key not found");
                std::process::exit(1)
            }
        }
        client::Command::Version => {
            println!("{}", env!("CARGO_PKG_VERSION"))
        }
    }
    Ok(())
}
