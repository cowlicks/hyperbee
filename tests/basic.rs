mod common;
use common::{get_data_dir, join_paths};

use hyperbee::{traverse::TraverseConfig, Hyperbee};

use crate::common::js::require_js_data;
static BASIC_TEST_DATA_STORAGE: &str = "basic";
static MORE_HEIGHT_TEST_DATA_STORAGE: &str = "more_height";
static SMALL_TEST_DATA_STORAGE: &str = "alphabet";

#[tokio::test]
async fn basic_integration() -> Result<(), Box<dyn std::error::Error>> {
    require_js_data()?;
    let start = 0;
    let stop = 25;
    let storage_dir = join_paths!(get_data_dir()?, BASIC_TEST_DATA_STORAGE);
    let hb = Hyperbee::from_storage_dir(storage_dir).await?;
    for i in start..stop {
        let key = i.to_string();
        let expected = (stop - i).to_string();
        let (_seq, res) = hb.get(key.as_bytes()).await?.unwrap();
        let res = res.unwrap();
        let res = std::str::from_utf8(&res).unwrap();
        println!("k {i}, v {res}");
        assert_eq!(res, expected);
    }
    Ok(())
}

#[tokio::test]
async fn no_key() -> Result<(), Box<dyn std::error::Error>> {
    require_js_data()?;
    let storage_dir = join_paths!(get_data_dir()?, BASIC_TEST_DATA_STORAGE);
    let hb = Hyperbee::from_storage_dir(storage_dir).await?;
    let result = hb.get(&"foobar".to_string().into_bytes()).await?;
    assert_eq!(result, Option::None);
    Ok(())
}

#[tokio::test]
async fn stream() -> Result<(), Box<dyn std::error::Error>> {
    use tokio_stream::StreamExt;
    require_js_data()?;
    let start = 0;
    let stop = 25;
    let storage_dir = join_paths!(get_data_dir()?, BASIC_TEST_DATA_STORAGE);
    let hb = Hyperbee::from_storage_dir(storage_dir).await?;
    let mut expected: Vec<Vec<u8>> = (start..stop).map(|i| i.to_string().into()).collect();
    expected.sort();
    let mut result = vec![];
    let stream = hb.traverse(TraverseConfig::default()).await?;
    tokio::pin!(stream);
    while let Some(key_data) = stream.next().await {
        result.push(key_data?);
    }
    let result: Vec<String> = result
        .into_iter()
        .map(|x| String::from_utf8(x.key).unwrap())
        .collect();
    let expected: Vec<String> = expected
        .into_iter()
        .map(|x| String::from_utf8(x).unwrap())
        .collect();
    assert_eq!(result, expected);
    Ok(())
}

#[tokio::test]
async fn height() -> Result<(), Box<dyn std::error::Error>> {
    require_js_data()?;
    let storage_dir = join_paths!(get_data_dir()?, MORE_HEIGHT_TEST_DATA_STORAGE);
    let hb = Hyperbee::from_storage_dir(storage_dir).await?;
    let height = hb.height().await?;
    assert_eq!(height, 5);
    Ok(())
}

#[tokio::test]
async fn print() -> Result<(), Box<dyn std::error::Error>> {
    require_js_data()?;
    let storage_dir = join_paths!(get_data_dir()?, SMALL_TEST_DATA_STORAGE);
    let hb = Hyperbee::from_storage_dir(storage_dir).await?;
    let result = hb.print().await?;
    println!("{result}");
    assert_eq!(
        result,
        "\ta
\tb
\tc
\td
e
\tf
\tg
\th
\ti
j
\tk
\tl
\tm
\tn
o
\tp
\tq
\tr
\ts
t
\tu
\tv
\tw
\tx
\ty
\tz
"
    );
    Ok(())
}
