use std::{path::PathBuf, process::Output};

use tempfile::tempdir;

use super::{
    build_whole_script, git_root, join_paths, path_to_c_lib, run_code, run_make_from_with,
};

static REL_PATH_TO_HERE: &str = "./tests/common/python";
static PRE_SCRIPT: &str = "
import asyncio
from hyperbee import *
";

static POST_SCRIPT: &str = "
if __name__ == '__main__':
    asyncio.run(main())
";

fn build_command(working_dir: &str, script_path: &str) -> String {
    format!("cd {working_dir} && python {script_path}")
}

pub fn path_to_python_target() -> Result<PathBuf, Box<dyn std::error::Error>> {
    let p = join_paths!(&git_root()?, "target/debug/hyperbee.py");
    Ok(p.into())
}

/// Run the provided python `code` within a context where the Python Hyperbee library is imported.
/// Code must be written within a `async main(): ...` function.
pub fn run_python(code: &str) -> Result<Output, Box<dyn std::error::Error>> {
    run_code(
        &build_whole_script(PRE_SCRIPT, code, POST_SCRIPT),
        "script.py",
        build_command,
        vec![path_to_python_target()?, path_to_c_lib()?],
    )
}

pub fn require_python() -> Result<Output, Box<dyn std::error::Error>> {
    run_make_from_with(REL_PATH_TO_HERE, "")
}
