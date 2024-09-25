mod common;
use common::{c::run_make_with, run_script_relative_to_git_root, Result};

use crate::common::c::require_c_exe;

#[test]
fn make_test_basic_ffi_hb_get() -> Result<()> {
    //let x: &[u8] = &[128, 228, 64, 216, 240, 85, 50, 53, 10];
    //println!("{}", String::from_utf8_lossy(&x));
    //assert!(false);
    require_c_exe()?;
    let _ = run_make_with("target/hyperbee")?;
    let output = run_script_relative_to_git_root("tests/common/c/target/hyperbee")?;
    dbg!(&output.stdout);
    println!("stdout:\n{}", String::from_utf8_lossy(&output.stdout));
    dbg!(&output.stderr);
    println!("stderr:\n{}", String::from_utf8_lossy(&output.stderr));
    let stdout = String::from_utf8(output.stdout)?.trim().to_string();
    assert_eq!(stdout, "25");
    Ok(())
}
