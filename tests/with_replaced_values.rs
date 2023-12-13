static HYPERBEE_STORAGE_DIR: &str = "./test_data/with_replaced_values";

#[tokio::test]
async fn with_replaced_values() -> Result<(), Box<dyn std::error::Error>> {
    let start = 0;
    let stop = 25;
    let mut hb = hyperbee_rs::load_from_storage_dir(HYPERBEE_STORAGE_DIR).await?;
    for i in start..stop {
        let key = i.to_string();
        let expected = (i * 2).to_string();

        let (seq, res) = hb.get(&key.as_bytes().to_vec()).await?.unwrap();
        let res = std::str::from_utf8(&res).unwrap();
        println!("i {i} seq {seq} res {res}");
        assert_eq!(res, expected);
    }
    Ok(())
}
