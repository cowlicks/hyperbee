mod common;
use common::c::run_make_with;

#[test]
fn make_test_basic_ffi_hb_get() -> Result<(), Box<dyn std::error::Error>> {
    let output = run_make_with("test")?;
    dbg!(&output);
    assert!(output.status.success());
    Ok(())
}
