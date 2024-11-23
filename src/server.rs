use clap::Subcommand;
use serde::{Deserialize, Serialize};

#[derive(Subcommand, Deserialize, Serialize, Debug, Clone)]
pub enum Command {
    #[command(name = "-V")]
    Version,
}

