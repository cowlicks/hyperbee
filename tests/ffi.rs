mod common;
use common::{
    c::{require_c_exe, run_make_with},
    js::require_js_data,
    run_script_relative_to_git_root, Result,
};

#[test]
fn test_basic_ffi_hb_get() -> Result<()> {
    require_js_data()?;
    require_c_exe()?;
    run_make_with("target/hyperbee")?;
    let output = run_script_relative_to_git_root("tests/common/c/target/hyperbee")?;
    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
    // TODO FIXME on first run, there is some garbage before the "25"
    assert!(stdout.ends_with("25"));
    Ok(())
}
