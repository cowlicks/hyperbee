mod common;

use common::{js::run_js, parse_json_result, write_100, Result};
use futures_lite::{Stream, StreamExt};
use hyperbee::{
    traverse::{KeyDataResult, TraverseConfig},
    Hyperbee,
};

async fn collect(stream: impl Stream<Item = KeyDataResult>) -> Result<Vec<Vec<u8>>> {
    let stream_res = stream.collect::<Vec<KeyDataResult>>().await;
    Ok(stream_res.into_iter().map(|x| x.unwrap().key).collect())
}

#[tokio::test]
async fn hello_world() -> Result<()> {
    let storage_dir = tempfile::tempdir()?;
    let hb = Hyperbee::from_storage_dir(&storage_dir).await?;
    let key = b"hello";
    let value = b"world";
    hb.put(key, Some(b"world")).await?;
    let res = hb.get(b"hello").await?;
    assert_eq!(res, Some((1u64, Some(value.to_vec()))));
    let output = run_js(
        &storage_dir,
        "
const r = await hb.get('hello');
write(r.value.toString());",
    )?;
    assert_eq!(output.stdout, b"world");
    Ok(())
}

#[tokio::test]
async fn zero_to_one_hundred() -> Result<()> {
    let storage_dir = tempfile::tempdir()?;
    let hb = Hyperbee::from_storage_dir(&storage_dir).await?;
    let keys = write_100!(&hb);
    let output = run_js(
        &storage_dir,
        "
    const out = [];
    for (let i = 0; i < 100; i++) {
        out.push(
        (await hb.get(String(i))).value.toString()
        );
    }
    write(JSON.stringify(out));
    ",
    )?;
    let res = parse_json_result(&output)?;
    assert_eq!(res, keys);
    Ok(())
}

#[tokio::test]
async fn stream_in_same_order() -> Result<()> {
    let storage_dir = tempfile::tempdir()?;
    let hb = Hyperbee::from_storage_dir(&storage_dir).await?;

    // setup test
    let _ = write_100!(&hb);

    let rust_res: Vec<Vec<u8>> = collect(hb.traverse(Default::default()).await?).await?;

    let output = run_js(
        &storage_dir,
        "
    const out = [];
    for await (const x of hb.createReadStream()) {
        out.push(x.value.toString())
    }
    write(JSON.stringify(out));
    ",
    )?;
    let js_res = parse_json_result(&output)?;
    assert_eq!(js_res, rust_res);
    Ok(())
}

#[tokio::test]
async fn sub_database() -> Result<()> {
    let storage_dir = tempfile::tempdir()?;
    let hb = Hyperbee::from_storage_dir(&storage_dir).await?;

    // setup test
    let pref = b"pref";
    let sub = hb.sub(pref, Default::default());

    let _ = write_100!(&sub);
    let traverse_config = TraverseConfig::default();
    let rust_res: Vec<Vec<u8>> = collect(sub.traverse(&traverse_config).await?).await?;

    let output = run_js(
        &storage_dir,
        "
    const out = [];
    const sub = hb.sub('pref');
    for await (const x of sub.createReadStream()) {
        out.push(x.value.toString())
    }
    write(JSON.stringify(out));
    ",
    )?;
    let js_res = parse_json_result(&output)?;
    assert_eq!(js_res, rust_res);
    Ok(())
}
