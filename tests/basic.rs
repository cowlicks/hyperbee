static HYPERBEE_STORAGE_DIR: &str = "./test_data/basic";

#[tokio::test]
async fn basic() -> Result<(), Box<dyn std::error::Error>> {
    let start = 0;
    let stop = 25;
    let mut hb = hyperbee_rs::load_from_storage_dir(HYPERBEE_STORAGE_DIR).await?;
    for i in start..stop {
        let key = i.to_string();
        let expected = (stop - i).to_string();
        let (_seq, res) = hb.get(&key.as_bytes().to_vec()).await?.unwrap();
        let res = std::str::from_utf8(&res).unwrap();
        println!("k {i}, v {res}");
        assert_eq!(res, expected);
    }
    Ok(())
}

#[tokio::test]
async fn no_key() -> Result<(), Box<dyn std::error::Error>> {
    let mut hb = hyperbee_rs::load_from_storage_dir(HYPERBEE_STORAGE_DIR).await?;
    let result = hb.get(&"foobar".to_string().into_bytes()).await?;
    assert_eq!(result, Option::None);
    Ok(())
}

#[tokio::test]
async fn height() -> Result<(), Box<dyn std::error::Error>> {
    let mut hb = hyperbee_rs::load_from_storage_dir("./test_data/more_height/").await?;
    let root = hb
        .get_root(false)
        .await?
        .expect("Root should be written already");
    let result = root.read().await.height().await?;
    assert_eq!(result, 5);
    Ok(())
}

#[tokio::test]
async fn stream() -> Result<(), Box<dyn std::error::Error>> {
    use tokio_stream::StreamExt;
    let start = 0;
    let stop = 25;
    let mut hb = hyperbee_rs::load_from_storage_dir(HYPERBEE_STORAGE_DIR).await?;
    let mut expected: Vec<Vec<u8>> = (start..stop).map(|i| i.to_string().into()).collect();
    expected.sort();
    let mut result = vec![];
    let root = hb
        .get_root(false)
        .await?
        .expect("Root should be written already");
    let stream = hyperbee_rs::traverse::traverse(root);
    tokio::pin!(stream);
    while let Some(x) = stream.next().await {
        let (key_data, _node) = x;
        result.push(key_data?);
    }
    let result: Vec<String> = result
        .into_iter()
        .map(|x| String::from_utf8(x.0).unwrap())
        .collect();
    let expected: Vec<String> = expected
        .into_iter()
        .map(|x| String::from_utf8(x).unwrap())
        .collect();
    assert_eq!(result, expected);
    Ok(())
}
