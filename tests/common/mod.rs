// cargo thinks everything in here is unused even though it is used in the integration tests
#![allow(dead_code)]

use std::process::{Command, Output};
pub mod c;
pub mod js;

pub static PATH_TO_DATA_DIR: &str = "tests/common/js/data";

macro_rules! join_paths {
    ( $path:expr$(,)?) => {
        $path
    };
    ( $p1:expr,  $p2:expr) => {{
        let p = std::path::Path::new(&*$p1).join($p2);
        p.display().to_string()
    }};
    ( $p1:expr,  $p2:expr, $($tail:tt)+) => {{
        let p = std::path::Path::new($p1).join($p2);
        join_paths!(p.display().to_string(), $($tail)*)
    }};
}

pub(crate) use join_paths;

pub fn git_root() -> Result<String, Box<dyn std::error::Error>> {
    let x = Command::new("sh")
        .arg("-c")
        .arg("git rev-parse --show-toplevel")
        .output()?;
    Ok(String::from_utf8(x.stdout)?.trim().to_string())
}

pub fn get_data_dir() -> Result<String, Box<dyn std::error::Error>> {
    Ok(join_paths!(git_root()?, &PATH_TO_DATA_DIR))
}

pub fn run_script_relative_to_git_root(script: &str) -> Result<Output, Box<dyn std::error::Error>> {
    Ok(Command::new("sh")
        .arg("-c")
        .arg(format!("cd {} && {}", git_root()?, script))
        .output()?)
}
