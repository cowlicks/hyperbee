mod common;
use common::{c::run_make_with, run_script_relative_to_git_root};

use crate::common::c::require_c_exe;

#[test]
fn make_test_basic_ffi_hb_get() -> Result<(), Box<dyn std::error::Error>> {
    require_c_exe()?;
    let _ = run_make_with("target/hyperbee")?;
    let output = run_script_relative_to_git_root("tests/common/c/target/hyperbee")?;
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout)?.trim().to_string();
    assert_eq!(stdout, "25");
    Ok(())
}
