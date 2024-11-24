use crate::KvsError;
use crate::Result;
use clap::Subcommand;
use serde::{Deserialize, Serialize};

#[derive(Subcommand, Deserialize, Serialize, Debug, Clone)]
#[serde(rename_all = "lowercase")]
pub enum Command {
    #[command[name="PING"]]
    Ping,
    Get {
        #[serde(rename = "k")]
        key: String,
    },
    Set {
        #[serde(rename = "k")]
        key: String,
        #[serde(rename = "v")]
        value: String,
    },
    Rm {
        #[serde(rename = "k")]
        key: String,
    },
    #[command(name = "-V")]
    Version,
}

pub fn parse_address(address: String) -> Result<String> {
    let parts: Vec<&str> = address.split(":").collect();

    if parts.len() > 2 {
        eprintln!("invalid address");
        return Err(KvsError::Message("Invalid address attribute".into()));
    }
    let addr = parts[0];
    let port = parts[1].to_string().parse::<u32>().unwrap();
    Ok(format!("{}:{}", addr, port))
}
