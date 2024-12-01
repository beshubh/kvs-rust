use clap::{Parser, ValueEnum};
use env_logger::Builder;
use kvs::engines::SledStore;
use kvs::server::{self, KvsServer};
use kvs::thread_pool::{NaiveThreadPool, RayonThreadPool, SharedQueueThreadPool, ThreadPool};
use kvs::Result;
use kvs::{KvStore, KvsEngine};
use log::{info, LevelFilter};
use std::env;
use std::env::current_dir;
use std::net::SocketAddr;

#[derive(Debug, Clone, ValueEnum)]
#[value(rename_all = "lowercase")]
enum Engine {
    Kvs,
    Sled,
}

#[derive(Debug, Clone, ValueEnum)]
#[value(rename_all = "lowercase")]
enum Pool {
    Naive,
    Rayon,
    SharedQueue,
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
    #[arg(long = "pool", global = true, value_enum, default_value_t = Pool::SharedQueue)]
    pool: Pool,
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

    match (&opt.engine, &opt.pool) {
        (Engine::Kvs, Pool::Naive) => run_with_engine(
            KvStore::open(&current_dir()?)?,
            NaiveThreadPool::new(1)?,
            addr,
        ),
        (Engine::Kvs, Pool::Rayon) => run_with_engine(
            KvStore::open(&current_dir()?)?,
            RayonThreadPool::new(1)?,
            addr,
        ),
        (Engine::Kvs, Pool::SharedQueue) => run_with_engine(
            KvStore::open(&current_dir()?)?,
            SharedQueueThreadPool::new(1)?,
            addr,
        ),
        (Engine::Sled, Pool::Naive) => run_with_engine(
            SledStore::open(&current_dir()?)?,
            NaiveThreadPool::new(1)?,
            addr,
        ),
        (Engine::Sled, Pool::Rayon) => run_with_engine(
            SledStore::open(&current_dir()?)?,
            RayonThreadPool::new(1)?,
            addr,
        ),
        (Engine::Sled, Pool::SharedQueue) => run_with_engine(
            SledStore::open(&current_dir()?)?,
            SharedQueueThreadPool::new(1)?,
            addr,
        ),
    }
}

fn run_with_engine<E: KvsEngine, P: ThreadPool>(
    engine: E,
    pool: P,
    addr: SocketAddr,
) -> Result<()> {
    let mut server = KvsServer::new(engine, pool);
    server.run(addr)?;
    Ok(())
}
