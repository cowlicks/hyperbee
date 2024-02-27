mod js;

use std::process::Output;

use futures_lite::StreamExt;
use hyperbee::{
    traverse::{Traverse, TraverseConfig, TreeItem},
    CoreMem, Hyperbee,
};
use js::run_js;

async fn collect<'a, M: CoreMem + 'a>(
    stream: Traverse<'a, M>,
) -> Result<Vec<Vec<u8>>, Box<dyn std::error::Error>> {
    let stream_res = stream.collect::<Vec<TreeItem<M>>>().await;
    Ok(stream_res.into_iter().map(|x| x.0.unwrap().key).collect())
}

macro_rules! write_100 {
    ($hb:expr) => {{
        let hb = $hb;
        let keys: Vec<Vec<u8>> = (0..100)
            .map(|x| x.clone().to_string().as_bytes().to_vec())
            .collect();

        for k in keys.iter() {
            let val: Option<&[u8]> = Some(k);
            hb.put(k, val).await?;
        }
        keys
    }};
}

fn parse_js_result(output: Output) -> Result<Vec<Vec<u8>>, Box<dyn std::error::Error>> {
    let stdout = String::from_utf8(output.stdout)?;
    let res: Vec<String> = serde_json::from_str(&stdout)?;
    Ok(res.into_iter().map(|x| x.into()).collect())
}

#[tokio::test]
async fn hello_world() -> Result<(), Box<dyn std::error::Error>> {
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
async fn zero_to_one_hundred() -> Result<(), Box<dyn std::error::Error>> {
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
    let res = parse_js_result(output)?;
    assert_eq!(res, keys);
    Ok(())
}

#[tokio::test]
async fn stream_in_same_order() -> Result<(), Box<dyn std::error::Error>> {
    let storage_dir = tempfile::tempdir()?;
    let hb = Hyperbee::from_storage_dir(&storage_dir).await?;
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
    let js_res = parse_js_result(output)?;
    assert_eq!(js_res, rust_res);
    Ok(())
}

#[tokio::test]
async fn sub_database() -> Result<(), Box<dyn std::error::Error>> {
    let storage_dir = tempfile::tempdir()?;
    let hb = Hyperbee::from_storage_dir(&storage_dir).await?;
    let pref = b"pref";
    let sub = hb.sub(pref, Default::default());

    let _ = write_100!(&sub);
    let traverse_config = TraverseConfig::default();
    let rust_res: Vec<Vec<u8>> = collect(sub.traverse(&traverse_config).await?).await?;

    let output = run_js(
        &storage_dir,
        "
    const out = [];
    const sub = hb.sub('pref', {sep: Buffer.alloc(1)});
    for await (const x of sub.createReadStream()) {
        out.push('pref' + Buffer.alloc(1) + x.value.toString())
    }
    write(JSON.stringify(out));
    ",
    )?;
    let js_res = parse_js_result(output)?;
    assert_eq!(js_res, rust_res);
    Ok(())
}
