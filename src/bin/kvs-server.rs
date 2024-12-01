use clap::{Parser, ValueEnum};
use env_logger::Builder;
use kvs::engines::SledStore;
use kvs::server::{self, KvsServer};
use kvs::Result;
use kvs::{KvStore, KvsEngine};
use log::{error, info, LevelFilter};
use std::env::current_dir;
use std::net::SocketAddr;
use std::{env, net::TcpListener};

#[derive(Debug, Clone, ValueEnum)]
#[value(rename_all = "lowercase")]
enum Engine {
    Kvs,
    Sled,
}

#[derive(Parser, Debug, Clone)]
#[command(author = "Shubh")]
#[command(version = env!("CARGO_PKG_VERSION"))]
#[command(name= env!("CARGO_PKG_NAME"))]
#[command(about = env!("CARGO_PKG_DESCRIPTION"))]
struct Opt {
    #[command(subcommand)]
    cmd: Option<server::Command>,
    #[arg(long = "addr", global = true, default_value = "127.0.0.1:6969")]
    address: SocketAddr,
    #[arg(long = "engine", global = true, value_enum ,default_value_t = Engine::Kvs)]
    engine: Engine,
}

fn handle_command(cmd: &server::Command) {
    match cmd {
        server::Command::Version => {
            println!("{}", env!("CARGO_PKG_VERSION"))
        }
    }
}

fn main() -> Result<()> {
    dotenv::dotenv().ok();
    Builder::new()
        .filter(None, LevelFilter::Info)
        .write_style(env_logger::WriteStyle::Always)
        .target(env_logger::Target::Stderr)
        .init();
    let opt = Opt::parse();
    if let Some(cmd) = &opt.cmd {
        handle_command(cmd);
    }

    run(&opt)?;

    Ok(())
}

fn run(opt: &Opt) -> Result<()> {
    let addr = opt.address;
    let engine = &opt.engine;
    info!("kvs-server {}", env!("CARGO_PKG_VERSION"));
    info!("Listening on: {}", addr);
    info!("Storage engine: {:?}", engine);
    match opt.engine {
        Engine::Kvs => run_with_engine(KvStore::open(&current_dir()?)?, addr),
        Engine::Sled => run_with_engine(SledStore::open(&current_dir()?)?, addr),
    }
}

fn run_with_engine<E: KvsEngine>(engine: E, addr: SocketAddr) -> Result<()> {
    let mut server = KvsServer::new(engine);
    server.run(addr)?;
    Ok(())
}
