mod common;
use common::python::{require_python, run_python};

#[tokio::test]
async fn hello_world_get_set_del() -> Result<(), Box<dyn std::error::Error>> {
    let x = require_python()?;
    dbg!(&x);
    let out = run_python(
        "
async def main():
    hb = await hyperbee_from_ram()
    x = await hb.put(b'hello', b'world')
    assert(x.old_seq == None)
    assert(x.new_seq == 1)

    x = await hb.get(b'hello')
    assert(x.value == b'world')

    x = await hb.delete(b'hello')
    assert(x == 1)
",
    )?;
    dbg!(&out);
    assert_eq!(out.status.code(), Some(0));
    Ok(())
}
