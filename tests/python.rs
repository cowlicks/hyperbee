mod common;
use common::python::{require_python, run_python};

#[tokio::test]
async fn python() -> Result<(), Box<dyn std::error::Error>> {
    let x = require_python()?;
    dbg!(&x);
    let out = run_python(
        "
async def main():
    hb = await hyperbee_from_ram()
    x = await hb.ffi_put(b'hello', b'world')
    assert(x.old_seq, None)
    assert(x.new_seq, 1)

    x = await hb.ffi_get(b'hello')
    assert(x.value == b'world')

    x = await hb.ffi_del(b'hello')
    assert(x, 1)
",
    )?;
    dbg!(&out);
    assert_eq!(out.status.code(), Some(0));
    Ok(())
}
