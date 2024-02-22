use std::{
    fs::File,
    io::Write,
    path::{Path, PathBuf},
    process::{Command, Output},
};
use tempfile::TempDir;
pub static REL_PATH_TO_NODE_MODULES: &str = "./tests/js/node_modules";

pub fn git_root() -> Result<String, Box<dyn std::error::Error>> {
    let x = Command::new("sh")
        .arg("-c")
        .arg("git rev-parse --show-toplevel")
        .output()?;
    Ok(String::from_utf8(x.stdout)?.trim().to_string())
}

pub fn path_to_node_modules() -> Result<PathBuf, Box<dyn std::error::Error>> {
    let gr = git_root()?;
    let gr = Path::new(&gr);
    Ok(gr.join(REL_PATH_TO_NODE_MODULES))
}

pub fn run_js(storage_dir: &TempDir, script: &str) -> Result<Output, Box<dyn std::error::Error>> {
    let js_dir = tempfile::tempdir()?;
    //std::io::File::create(js_script)?;
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
