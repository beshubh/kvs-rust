use crate::{KvsError, Result};

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
