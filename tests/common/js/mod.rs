use super::{git_root, join_paths, run_code};
use std::{
    fs::File,
    io::Write,
    path::PathBuf,
    process::{Command, Output},
};
use tempfile::TempDir;
pub static REL_PATH_TO_NODE_MODULES: &str = "./tests/common/js/node_modules";
pub static REL_PATH_TO_JS_DIR: &str = "./tests/common/JS";

pub fn run_make_with(make_arg: &str) -> Result<Output, Box<dyn std::error::Error>> {
    let git_root = git_root()?;
    let cmd = format!("cd {git_root} && cd {REL_PATH_TO_JS_DIR} && make {make_arg}");
    Ok(Command::new("sh").arg("-c").arg(cmd).output()?)
}

pub fn require_js_data() -> Result<(), Box<dyn std::error::Error>> {
    let _ = run_make_with("")?;
    Ok(())
}

pub fn path_to_node_modules() -> Result<PathBuf, Box<dyn std::error::Error>> {
    let p = join_paths!(git_root()?, &REL_PATH_TO_NODE_MODULES);
    Ok(p.into())
}

static POST_SCRIPT: &str = "
})()
";

static SCRIPT_FILE_NAME: &str = "script.js";

fn build_command(script_path: &str) -> String {
    format!(
        "NODE_PATH={} node {}",
        path_to_node_modules().unwrap().display(),
        script_path
    )
}

fn build_pre_script(storage_dir: &str) -> String {
    format!(
        "
const Hypercore = require('hypercore');
const Hyperbee = require('hyperbee');
const storageDir = \'{storage_dir}\';
const core = new Hypercore(storageDir, {{writable: false}});
const hb = new Hyperbee(core);
(async() => {{
    const write = (x) => process.stdout.write(x);
    await hb.ready();"
    )
}

pub fn run_js_2(storage_dir: &TempDir, script: &str) -> Result<Output, Box<dyn std::error::Error>> {
    run_code(
        storage_dir,
        build_pre_script,
        script,
        POST_SCRIPT,
        SCRIPT_FILE_NAME,
        build_command,
    )
}

pub fn run_js(storage_dir: &TempDir, script: &str) -> Result<Output, Box<dyn std::error::Error>> {
    let js_dir = tempfile::tempdir()?;
    let js_code = format!(
        "
const Hypercore = require('hypercore');
const Hyperbee = require('hyperbee');
const storageDir = \'{}\';
const core = new Hypercore(storageDir, {{writable: false}});
const hb = new Hyperbee(core);
(async() => {{
    const write = (x) => process.stdout.write(x);
    await hb.ready();
    {};
}})()
",
        storage_dir.path().display(),
        script,
    );
    let js_file_path = js_dir.path().join("script.js");
    let js_file = File::create(&js_file_path)?;
    write!(&js_file, "{}", &js_code)?;

    Ok(Command::new("sh")
        .arg("-c")
        .arg(format!(
            "NODE_PATH={} node {}",
            path_to_node_modules()?.display(),
            js_file_path.display()
        ))
        .output()?)
}
