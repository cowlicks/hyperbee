mod common;

use std::{
    path::Path,
    process::{Command, Output},
};
use tempfile::{tempdir, TempDir};

use common::{check_cmd_output, js::run_js_writable, Result};

use hyperbee::Hyperbee;

use crate::common::{i32_key_vec, write_range_to_hb, Rand};

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
    run_command(cmd)
}

fn diff_dirs<T: AsRef<Path>>(a: T, b: T) -> Result<Output> {
    let astr = a.as_ref().to_string_lossy();
    let bstr = b.as_ref().to_string_lossy();
    let cmd = format!("diff {astr} {bstr}");
    run_command(cmd)
}

fn create_storage_dirs_with_same_keys() -> Result<(TempDir, TempDir)> {
    let rdir = tempdir()?;
    let jsdir = tempdir()?;

    let _ = create_initialized_storage_dir(&jsdir)?;
    let _ = cp_dirs(&jsdir, &rdir)?;

    Ok((jsdir, rdir))
}

macro_rules! put_rs_and_js_range {
    ($range:expr, $extra_js:expr) => {{
        let (jsdir, rdir) = create_storage_dirs_with_same_keys()?;
        let keys: Vec<i32> = $range.clone().collect();
        let hb = Hyperbee::from_storage_dir(&rdir).await?;
        write_range_to_hb!(&hb, $range);
        let js_code = format!(
            "
const keys = {};
for (const ikey of keys) {{
    const key = String(ikey);
    await hb.put(key, key);
}}
{}",
            serde_json::to_string(&keys)?,
            $extra_js,
        );
        println!("{js_code}");

        let _ = run_js_writable(&jsdir, &js_code)?;
        (hb, jsdir, rdir)
    }};
    ($range:expr) => {{
        put_rs_and_js_range!($range, "")
    }};
}

#[tokio::test]
async fn compare_disk_hello_world() -> Result<()> {
    // set up the initial directories
    let (jsdir, rdir) = create_storage_dirs_with_same_keys()?;

    // add hello world to rust
    let hb = Hyperbee::from_storage_dir(&rdir).await?;
    let key = b"hello";
    let value = b"world";
    hb.put(key, Some(value)).await?;

    // add hello world to js
    let _ = run_js_writable(
        &jsdir,
        "
    await hb.put('hello', 'world');
    ",
    )?;

    // compare directies
    diff_dirs(&jsdir, &rdir)?;
    Ok(())
}

#[tokio::test]
async fn compare_trees_of_some_ranges() -> Result<()> {
    for n_keys in (8..12).chain(47..53) {
        let (_hb, jsdir, rdir) = put_rs_and_js_range!(0..n_keys);
        diff_dirs(&jsdir, &rdir)?;
    }
    Ok(())
}

#[tokio::test]
async fn rotate_from_right_the_same() -> Result<()> {
    let delete_me = 37;
    let (hb, jsdir, rdir) = put_rs_and_js_range!(0..48, format!("await hb.del('{}')", delete_me));
    hb.del(&i32_key_vec(delete_me)).await?;
    diff_dirs(&jsdir, &rdir)?;
    Ok(())
}
#[tokio::test]
async fn rotate_from_left_the_same() -> Result<()> {
    let delete_me = 6;
    let (hb, jsdir, rdir) = put_rs_and_js_range!(0..48, format!("await hb.del('{}')", delete_me));
    hb.del(&i32_key_vec(delete_me)).await?;
    diff_dirs(&jsdir, &rdir)?;
    Ok(())
}

#[tokio::test]
async fn merge_from_left_the_same() -> Result<()> {
    let delete_me = 13;
    let (hb, jsdir, rdir) = put_rs_and_js_range!(0..48, format!("await hb.del('{}')", delete_me));
    hb.del(&i32_key_vec(delete_me)).await?;
    diff_dirs(&jsdir, &rdir)?;
    Ok(())
}

#[tokio::test]
async fn merge_from_right_the_same() -> Result<()> {
    let delete_me = 0;
    let (hb, jsdir, rdir) = put_rs_and_js_range!(0..48, format!("await hb.del('{}')", delete_me));
    hb.del(&i32_key_vec(delete_me)).await?;
    diff_dirs(&jsdir, &rdir)?;
    Ok(())
}

#[tokio::test]
async fn double_merge_replace_root() -> Result<()> {
    let rand = Rand::default();
    let keys: Vec<i32> = rand.shuffle((0..100).collect());
    let del_keys = rand.shuffle(keys.clone()).to_vec()[..43].to_vec();

    let extra_js = format!(
        "
    const del_keys = {};
    for (const ikey of del_keys) {{
        const key = String(ikey);
        await hb.del(key);
    }}
",
        serde_json::to_string(&del_keys)?,
    );
    let (hb, jsdir, rdir) = put_rs_and_js_range!(keys.clone().into_iter(), extra_js);
    for key in del_keys.iter() {
        let key = i32_key_vec(*key);
        hb.del(&key).await?;
    }
    diff_dirs(&jsdir, &rdir)?;
    Ok(())
}

#[tokio::test]
async fn order_via_depth_first_search() -> Result<()> {
    let rand = Rand::default();
    let n_keys = 148;
    let stop = 123;
    let mut keys: Vec<i32> = rand.shuffle((0..n_keys).collect())[..(stop as usize)].to_vec();
    let last_i = keys.pop().unwrap();
    let last = i32_key_vec(last_i);

    let extra_js = format!(
        "
    const key = '{}';
    await hb.put(key, key);
    ",
        last_i
    );
    let (hb, jsdir, rsdir) = put_rs_and_js_range!(keys.clone().into_iter(), extra_js);
    let jshb = Hyperbee::from_storage_dir(&jsdir).await?;
    hb.put(&last, Some(&last)).await?;
    assert_eq!(hb.print().await?, jshb.print().await?);

    diff_dirs(&jsdir, &rsdir)?;
    Ok(())
}

#[tokio::test]
async fn pulls_from_child_with_more_keys() -> Result<()> {
    let delete_me = 19;
    let range = (10..20).chain(1..2);
    let (hb, jsdir, rdir) = put_rs_and_js_range!(range, format!("await hb.del('{}')", delete_me));
    hb.del(&i32_key_vec(delete_me)).await?;
    diff_dirs(&jsdir, &rdir)?;
    Ok(())
}

#[tokio::test]
async fn big_random_test() -> Result<()> {
    let rand = Rand::default();
    let n_keys = 1000;
    let keys: Vec<i32> = rand.shuffle((0..n_keys).collect());
    let del_keys = rand.shuffle(keys.clone());

    let extra_js = format!(
        "
    const del_keys = {};
    for (const ikey of del_keys) {{
        const key = String(ikey);
        await hb.del(key);
    }}
",
        serde_json::to_string(&del_keys)?,
    );

    let (hb, jsdir, rsdir) = put_rs_and_js_range!(keys.clone().into_iter(), extra_js);
    for key in del_keys.iter() {
        let key = i32_key_vec(*key);
        hb.del(&key).await?;
    }

    let jshb = Hyperbee::from_storage_dir(&jsdir).await?;
    assert_eq!(hb.print().await?, jshb.print().await?);
    diff_dirs(&jsdir, &rsdir)?;
    Ok(())
}
