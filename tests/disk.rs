mod common;

use std::process::{Command, Output};
use tempfile::{tempdir, TempDir};

use common::{check_cmd_output, js::run_js_writable, Result};

use hyperbee::Hyperbee;

fn run_command(cmd: &str) -> Result<Output> {
    println!("Running command: {cmd}");
    Ok(check_cmd_output(
        Command::new("sh").arg("-c").arg(cmd).output()?,
    )?)
}

fn create_initialized_storage_dir(dir_str: &str) -> Result<Output> {
    Ok(run_js_writable(dir_str, "")?)
}

fn cp_dirs(a: &str, b: &str) -> Result<Output> {
    let cmd = format!("cp -r {a}/. {b}/.");
    Ok(run_command(&cmd)?)
}

fn diff_dirs(a: &str, b: &str) -> Result<Output> {
    let cmd = format!("diff {a} {b}");
    Ok(run_command(&cmd)?)
}

fn create_storage_dirs_with_same_keys() -> Result<(TempDir, TempDir)> {
    let rdir = tempdir()?;
    let jsdir = tempdir()?;

    let jsdir_str = jsdir.path().display().to_string();
    let rdir_str = rdir.path().display().to_string();

    let _ = create_initialized_storage_dir(&jsdir_str)?;
    let _ = cp_dirs(&jsdir_str, &rdir_str)?;

    Ok((jsdir, rdir))
}

#[tokio::test]
async fn copy_after_init() -> Result<()> {
    // set up the initial directories
    let (jsdir, rdir) = create_storage_dirs_with_same_keys()?;
    let jsdir_str = jsdir.path().display().to_string();
    let rdir_str = rdir.path().display().to_string();

    // add hello world to rust
    let hb = Hyperbee::from_storage_dir(&rdir_str).await?;
    let key = b"hello";
    let value = b"world";
    hb.put(key, Some(value)).await?;

    // add hello world to js
    let _ = run_js_writable(
        &jsdir_str,
        "
    await hb.put('hello', 'world');
    ",
    )?;

    // compare directies
    diff_dirs(&jsdir_str, &rdir_str)?;
    Ok(())
}
