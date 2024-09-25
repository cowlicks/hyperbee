mod common;
use common::{
    c::{require_c_exe, run_make_with},
    js::require_js_data,
    run_script_relative_to_git_root, Result,
};

#[test]
fn make_test_basic_ffi_hb_get() -> Result<()> {
    require_js_data()?;
    require_c_exe()?;
    let _ = run_make_with("target/hyperbee")?;
    let mut count = 0;
    loop {
        // TODO FIXME sometimes there is junk in the beggining of the stdout on the first run
        let output = run_script_relative_to_git_root("tests/common/c/target/hyperbee")?;
        dbg!(&output);
        let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
        println!("{}", &stdout);
        if stdout == "25" {
            break;
        }
        if count == 5 {
            panic!("should have passed");
        }
        count += 1;
    }
    Ok(())
}
