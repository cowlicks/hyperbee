static HYPERBEE_STORAGE_DIR: &str = "./test_data/basic";

#[tokio::test]
async fn basic() -> Result<(), Box<dyn std::error::Error>> {
    let start = 0;
    let stop = 25;
    let mut hb = hyperbee_rs::load_from_storage_dir(HYPERBEE_STORAGE_DIR).await?;
    for i in start..stop {
        println!("{i}");
        let key = i.to_string();
        let expected = (stop - i).to_string();
        let (seq, res) = hb.get(&key.as_bytes().to_vec()).await?.unwrap();
        let res = std::str::from_utf8(&res).unwrap();
        dbg!(seq, res);
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
