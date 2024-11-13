use clap::{Parser, Subcommand};

#[derive(Parser, Debug, Clone)]
#[command(author = "Shubh")]
#[command(version = env!("CARGO_PKG_VERSION"))]
#[command(name= env!("CARGO_PKG_NAME"))]
#[command(about = env!("CARGO_PKG_DESCRIPTION"))]
struct Cli {
    #[command(subcommand)]
    cmd: Command,
}

#[derive(Subcommand, Debug, Clone)]
enum Command {
    Get {
        key: String,
    },
    Set {
        key: String,
        value: String,
    },
    Rm {
        key: String,
    },
    #[command(name = "-V")]
    Version,
}

fn main() {
    let cli = Cli::parse();
    match &cli.cmd {
        Command::Get { key } => {
            println!("args: {:?}", &cli.cmd);
            panic!("unimplemented");
        }
        Command::Set { key, value } => {
            println!("args: {:?}", &cli.cmd);
            panic!("unimplemented");
        }
        Command::Rm { key } => {
            println!("args: {:?}", &cli.cmd);
            panic!("unimplemented")
        }
        Command::Version => {
            println!("{}", env!("CARGO_PKG_VERSION"))
        }
    }
}
