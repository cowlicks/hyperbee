use super::git_root;
use std::process::{Command, Output};

pub static REL_PATH_TO_C_DIR: &str = "./tests/common/c";

pub fn run_make_with(make_arg: &str) -> Result<Output, Box<dyn std::error::Error>> {
    let git_root = git_root()?;
    let cmd = format!("cd {git_root} && cd {REL_PATH_TO_C_DIR} && make {make_arg}");
    Ok(Command::new("sh").arg("-c").arg(cmd).output()?)
}
