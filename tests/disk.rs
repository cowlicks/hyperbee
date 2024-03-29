mod common;

use std::{
    ffi::OsStr,
    path::Path,
    process::{Command, Output},
};
use tempfile::{tempdir, TempDir};

use common::{check_cmd_output, js::run_js_writable, Result};

use hyperbee::Hyperbee;

use crate::common::{i32_key_vec, setup_logs, write_range_to_hb, Rand};

fn run_command(cmd: impl AsRef<str>) -> Result<Output> {
    let s = cmd.as_ref();
    check_cmd_output(Command::new("sh").arg("-c").arg(s).output()?)
}

fn create_initialized_storage_dir<T: AsRef<Path>>(dir: T) -> Result<Output> {
    run_js_writable(dir, "")
}

fn cp_dirs<T: AsRef<Path>>(a: T, b: T) -> Result<Output> {
    let a = a.as_ref().to_string_lossy();
    let b = b.as_ref().to_string_lossy();
    let cmd = format!("cp -r {a}/. {b}/.");
    run_command(&cmd)
}

fn diff_dirs<T: AsRef<Path>>(a: T, b: T) -> Result<Output> {
    let astr = a.as_ref().to_string_lossy();
    let bstr = b.as_ref().to_string_lossy();
    let cmd = format!("diff {astr} {bstr}");
    run_command(&cmd)
}

fn create_storage_dirs_with_same_keys() -> Result<(TempDir, TempDir)> {
    let rdir = tempdir()?;
    let jsdir = tempdir()?;

    let _ = create_initialized_storage_dir(&jsdir)?;
    let _ = cp_dirs(&jsdir, &rdir)?;

    Ok((jsdir, rdir))
}

#[tokio::test]
async fn compare_disk_hello_world() -> Result<()> {
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

#[tokio::test]
async fn compare_trees_of_some_ranges() -> Result<()> {
    for n_keys in (8..12).chain(47..53) {
        let (jsdir, rdir) = create_storage_dirs_with_same_keys()?;
        let jsdir_str = jsdir.path().display().to_string();
        let rdir_str = rdir.path().display().to_string();

        let hb = Hyperbee::from_storage_dir(&rdir_str).await?;
        let _keys = write_range_to_hb!(&hb, n_keys);
        let code = format!(
            "
        for (let i = 0; i < {n_keys}; i++) {{
            const k = String(i);
            await hb.put(k, k);
        }}
        "
        );
        let _output = run_js_writable(&jsdir_str, &code)?;
        if n_keys == 101 {
            println!("{}", hb.print().await?);
        }
        diff_dirs(&jsdir_str, &rdir_str)?;
    }
    Ok(())
}

#[tokio::test]
async fn foo_compare_disk_put_and_del() -> Result<()> {
    // set up the initial directories
    let (jsdir, rdir) = create_storage_dirs_with_same_keys()?;
    let jsdir_str = jsdir.path().display().to_string();
    let rdir_str = rdir.path().display().to_string();

    // add hello world to rust
    let rand = Rand::default();
    let hb = Hyperbee::from_storage_dir(&rdir_str).await?;

    let keys = (0..5).collect();
    let keys = rand.shuffle(keys);

    for key in keys.iter() {
        let key = i32_key_vec(*key);
        hb.put(&key, Some(&key)).await?;
    }

    // add keys
    let code = format!(
        "
    const keys = {};
    for (const ikey of keys) {{
        const key = String(ikey);
        await hb.put(key, key)
    }}
",
        serde_json::to_string(&keys)?
    );
    let _ = run_js_writable(&jsdir_str, &code)?;
    // compare directies
    diff_dirs(&jsdir_str, &rdir_str)?;

    // shuffle keys again and delete
    let keys = rand.shuffle(keys);
    for key in keys.iter() {
        let key = i32_key_vec(*key);
        hb.del(&key).await?;
    }
    let code = format!(
        "
    const keys = {};
    for (const ikey of keys) {{
        const key = String(ikey);
        await hb.del(key);
    }}
",
        serde_json::to_string(&keys)?
    );
    let _ = run_js_writable(&jsdir_str, &code)?;
    // compare directies
    diff_dirs(&jsdir_str, &rdir_str)?;
    Ok(())
}
