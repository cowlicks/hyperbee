use std::{path::PathBuf, process::Output};

use tempfile::tempdir;

use super::{git_root, join_paths, path_to_c_lib, run_code, run_make_from_with};

static REL_PATH_TO_HERE: &str = "./tests/common/python";
static PRE_SCRIPT: &str = "
import asyncio
from target.hyperbee import *
";

static POST_SCRIPT: &str = "
if __name__ == '__main__':
    asyncio.run(main())
";

fn build_command(working_dir: &str, script_path: &str) -> String {
    format!("cd {working_dir} && python {script_path}")
}

pub fn path_to_python_target() -> Result<PathBuf, Box<dyn std::error::Error>> {
    let p = join_paths!(&git_root()?, &REL_PATH_TO_HERE, "target");
    Ok(p.into())
}

pub fn run_python(script: &str) -> Result<Output, Box<dyn std::error::Error>> {
    let storage_dir = tempdir()?;
    let target_path = path_to_python_target()?.display().to_string();
    run_code(
        &storage_dir,
        |_| PRE_SCRIPT.to_string(),
        script,
        POST_SCRIPT,
        "script.py",
        build_command,
        vec![target_path, path_to_c_lib()?],
    )
}

pub fn require_python() -> Result<Output, Box<dyn std::error::Error>> {
    run_make_from_with(REL_PATH_TO_HERE, "")
}
