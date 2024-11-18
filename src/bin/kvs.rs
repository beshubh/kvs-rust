use std::env;

use clap::Parser;
use kvs::KvStore;
use tempfile::TempDir;

#[derive(Parser, Debug, Clone)]
#[command(author = "Shubh")]
#[command(version = env!("CARGO_PKG_VERSION"))]
#[command(name= env!("CARGO_PKG_NAME"))]
#[command(about = env!("CARGO_PKG_DESCRIPTION"))]
struct Cli {
    #[command(subcommand)]
    cmd: kvs::Command,
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    // let temp_dir = TempDir::new().unwrap();
    let mut store = KvStore::open(std::env::current_dir().unwrap().as_path()).unwrap();
    match &cli.cmd {
        kvs::Command::Get { key } => {
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
        kvs::Command::Set { key, value } => store.set(key.into(), value.into())?,
        kvs::Command::Rm { key } => {
            let val = store.remove(key.into());
            if let Err(_) = val {
                print!("Key not found");
                std::process::exit(1)
            }
        }
        kvs::Command::Version => {
            println!("{}", env!("CARGO_PKG_VERSION"))
        }
    }
    Ok(())
}
