// cargo thinks everything in here is unused even though it is used in the integration tests
#![allow(dead_code)]

use std::process::Command;
pub mod c;
pub mod js;

pub fn git_root() -> Result<String, Box<dyn std::error::Error>> {
    let x = Command::new("sh")
        .arg("-c")
        .arg("git rev-parse --show-toplevel")
        .output()?;
    Ok(String::from_utf8(x.stdout)?.trim().to_string())
}
