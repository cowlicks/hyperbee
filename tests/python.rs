mod common;
use common::python::{require_python, run_python};

#[tokio::test]
async fn python() -> Result<(), Box<dyn std::error::Error>> {
    require_python()?;
    let out = run_python(
        "
async def main():
    print('yo')
",
    )?;
    dbg!(&out);
    assert_eq!(out.status.code(), Some(0));
    Ok(())
}
