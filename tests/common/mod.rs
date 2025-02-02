// cargo thinks everything in here is unused even though it is used in the integration tests
#![allow(dead_code)]
#![allow(unused_imports)]
#![allow(unused_macros)]

use std::{
    fs::File,
    io::Write,
    path::PathBuf,
    process::{Command, Output},
    sync::atomic::{AtomicU64, Ordering},
};

use tokio::sync::OnceCell;

pub mod c;
pub mod js;
pub mod python;

pub type Result<T> = core::result::Result<T, Box<dyn std::error::Error>>;

pub static PATH_TO_DATA_DIR: &str = "tests/common/js/data";
pub static PATH_TO_C_LIB: &str = "target/debug/libhyperbee.so";

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Problem in tests: {0}")]
    TestError(String),
}

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

pub fn git_root() -> Result<String> {
    let x = Command::new("sh")
        .arg("-c")
        .arg("git rev-parse --show-toplevel")
        .output()?;
    Ok(String::from_utf8(x.stdout)?.trim().to_string())
}

pub fn path_to_c_lib() -> Result<PathBuf> {
    Ok(join_paths!(git_root()?, PATH_TO_C_LIB).parse()?)
}

pub fn get_data_dir() -> Result<String> {
    Ok(join_paths!(git_root()?, &PATH_TO_DATA_DIR))
}

pub fn run_script_relative_to_git_root(script: &str) -> Result<Output> {
    Ok(Command::new("sh")
        .arg("-c")
        .arg(format!("cd {} && {}", git_root()?, script))
        .output()?)
}

pub fn build_whole_script(pre_code: &str, code: &str, post_code: &str) -> String {
    format!(
        "{pre_code}
{code}
{post_code}
"
    )
}

/// Run some code.
/// Put the provided `code` into a file named `script_file_name` inside a temporary directory where
/// `copy_dirs` are copied into. `build_command` should output a shell command as a string that
/// runs the script.
pub fn run_code(
    code: &str,
    script_file_name: &str,
    build_command: impl FnOnce(&str, &str) -> String,
    copy_dirs: Vec<PathBuf>,
) -> Result<Output> {
    let working_dir = tempfile::tempdir()?;
    let working_dir_path = working_dir.path().display().to_string();
    let script_path = working_dir.path().join(script_file_name);
    write!(&File::create(&script_path)?, "{code}")?;

    // copy dirs into working dir
    for dir in copy_dirs {
        let dir_cp_cmd = Command::new("cp")
            .arg("-r")
            .arg(&dir)
            .arg(&working_dir_path)
            .output()?;
        if dir_cp_cmd.status.code() != Some(0) {
            return Err(Box::new(Error::TestError(format!(
                "failed to copy dir [{}] to [{working_dir_path}] got stderr: {}",
                dir.display(),
                String::from_utf8_lossy(&dir_cp_cmd.stderr),
            ))));
        }
    }
    let cmd = build_command(&working_dir_path, &script_path.to_string_lossy());
    check_cmd_output(Command::new("sh").arg("-c").arg(cmd).output()?)
}

pub fn run_make_from_dir_with_arg(dir: &str, arg: &str) -> Result<Output> {
    let path = join_paths!(git_root()?, dir);
    let cmd = format!("cd {path} && flock make.lock make {arg} && rm -f make.lock ");
    let out = check_cmd_output(Command::new("sh").arg("-c").arg(cmd).output()?)?;
    Ok(out)
}

pub fn parse_json_result(output: &Output) -> Result<Vec<Vec<u8>>> {
    let stdout = String::from_utf8(output.stdout.clone())?;
    let res: Vec<String> = serde_json::from_str(&stdout)?;
    Ok(res.into_iter().map(|x| x.into()).collect())
}

#[allow(unused_macros)]
macro_rules! write_range_to_hb {
    ($hb:expr) => {{
        write_range_to_hb!($hb, 0..100)
    }};
    ($hb:expr, $range:expr) => {{
        let hb = $hb;
        let keys: Vec<Vec<u8>> = ($range)
            .map(|x| x.clone().to_string().as_bytes().to_vec())
            .collect();

        for k in keys.iter() {
            let val: Option<&[u8]> = Some(k);
            hb.put(k, val).await?;
        }
        keys
    }};
}

#[allow(unused_imports)]
pub(crate) use write_range_to_hb;

pub fn check_cmd_output(out: Output) -> Result<Output> {
    if out.status.code() != Some(0) {
        if !out.stdout.is_empty() {
            eprintln!("stdout:\n{}", String::from_utf8_lossy(&out.stdout));
        }
        if !out.stderr.is_empty() {
            eprintln!("stderr:\n{}", String::from_utf8_lossy(&out.stderr));
        }
        return Err(Box::new(Error::TestError(format!(
            "command output status was not zero. Got:{:?}",
            out.status.code()
        ))));
    }
    Ok(out)
}

static INIT_LOG: OnceCell<()> = OnceCell::const_new();
pub async fn setup_logs() {
    INIT_LOG
        .get_or_init(|| async {
            tracing_subscriber::fmt::fmt()
                .event_format(
                    tracing_subscriber::fmt::format()
                        .without_time()
                        .with_file(true)
                        .with_line_number(true),
                )
                .init();
        })
        .await;
}
// TODO this (and other stuff in this module) are copiedfrom src/tests.rs. DRY this by using a
// feature in src and requiring these tests use that feature.
/// Seedable deterministic pseudorandom number generator used for reproducible randomized testing
pub struct Rand {
    seed: u64,
    counter: AtomicU64,
    sin_scale: f64,
    ordering: Ordering,
}

impl Rand {
    pub fn rand(&self) -> f64 {
        let count = self.counter.fetch_add(1, self.ordering);
        let x = ((self.seed + count) as f64).sin() * self.sin_scale;
        x - x.floor()
    }
    pub fn rand_int_lt(&self, max: u64) -> u64 {
        (self.rand() * (max as f64)).floor() as u64
    }
    pub fn shuffle<T>(&self, mut arr: Vec<T>) -> Vec<T> {
        let mut out = vec![];
        while !arr.is_empty() {
            let i = self.rand_int_lt(arr.len() as u64) as usize;
            out.push(arr.remove(i));
        }
        out
    }
}

impl Default for Rand {
    fn default() -> Self {
        Self {
            seed: 42,
            counter: Default::default(),
            sin_scale: 10_000_f64,
            ordering: Ordering::SeqCst,
        }
    }
}

pub fn i32_key_vec(i: i32) -> Vec<u8> {
    i.clone().to_string().as_bytes().to_vec()
}

macro_rules! print_tree {
    ( $hb:ident ) => {
        println!("{}.print() =\n{}", stringify!($hb), $hb.print().await?)
    };
}
pub(crate) use print_tree;
