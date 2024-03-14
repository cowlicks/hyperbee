// cargo thinks everything in here is unused even though it is used in the integration tests
#![allow(dead_code)]

use std::{
    fs::File,
    io::Write,
    process::{Command, Output},
};
pub mod c;
pub mod js;
pub mod python;

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
use tempfile::TempDir;

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

pub fn run_code(
    storage_dir: &TempDir,
    build_pre_script: impl FnOnce(&str) -> String,
    script: &str,
    post_script: &str,
    script_file_name: &str,
    build_command: impl FnOnce(&str, &str) -> String,
    copy_dirs: Vec<String>,
) -> Result<Output, Box<dyn std::error::Error>> {
    let storage_dir_name = format!("{}", storage_dir.path().display());
    let pre_script = build_pre_script(&storage_dir_name);

    let working_dir = tempfile::tempdir()?;

    let code = format!(
        "{pre_script}
{script}
{post_script}
"
    );
    println!("{code}");
    let script_path = working_dir.path().join(script_file_name);
    let script_file = File::create(&script_path)?;
    write!(&script_file, "{}", &code)?;

    let working_dir_path = working_dir.path().display().to_string();
    // copy dirs into working dir
    for dir in copy_dirs {
        let _ = Command::new("cp")
            .arg("-r")
            .arg(dir)
            .arg(&working_dir_path)
            .output()?;
    }
    let script_path_str = script_path.display().to_string();
    let command_str = build_command(&working_dir_path, &script_path_str);
    println!("{command_str}");
    let out = Command::new("sh").arg("-c").arg(command_str).output()?;
    dbg!(&out);
    Ok(out)
}
