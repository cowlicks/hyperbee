mod common;
use common::{
    parse_json_result,
    python::{require_python, run_python},
    write_100,
};
use hyperbee::Hyperbee;

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

#[tokio::test]
async fn optionals() -> Result<(), Box<dyn std::error::Error>> {
    let _x = require_python()?;
    let storage_dir = tempfile::tempdir()?;
    {
        let hb = Hyperbee::from_storage_dir(&storage_dir).await?;
        let _ = hb.put(b"hello", None).await?;
    }
    let code = format!(
        "
async def main():
    import json
    hb = await hyperbee_from_storage_dir('{}')
    res = await hb.get(b'hello')
    assert(res.value is None)

    res = await hb.put(b'skipval', None)
    res = await hb.get(b'skipval')
    assert(res.value is None)
",
        storage_dir.path().display()
    );

    let output = run_python(&code)?;
    dbg!(&output);
    assert_eq!(output.status.code(), Some(0));
    Ok(())
}
#[tokio::test]
async fn check_version() -> Result<(), Box<dyn std::error::Error>> {
    let _x = require_python()?;
    let out = run_python(
        "
async def main():
    hb = await hyperbee_from_ram()
    x = await hb.version()
    assert(x == 0)
    await hb.put(b'foo', None)
    x = await hb.version()
    assert(x == 2)
",
    )?;
    assert_eq!(out.status.code(), Some(0));
    Ok(())
}

#[tokio::test]
async fn zero_to_one_hundred() -> Result<(), Box<dyn std::error::Error>> {
    let _x = require_python()?;
    let storage_dir = tempfile::tempdir()?;
    let hb = Hyperbee::from_storage_dir(&storage_dir).await?;
    let keys = write_100!(&hb);
    let code = format!(
        "
async def main():
    import json
    hb = await hyperbee_from_storage_dir('{}')
    keys = [bytes(str(i), 'utf8') for i in range(100)]
    results = [await hb.get(k) for k in keys]
    values = [res.value.decode('utf8') for res in results]
    print(json.dumps(values))
",
        storage_dir.path().display()
    );

    let output = run_python(&code)?;
    let res = parse_json_result(&output)?;
    assert_eq!(res, keys);
    assert_eq!(output.status.code(), Some(0));
    Ok(())
}
