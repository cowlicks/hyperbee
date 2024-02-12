use hyperbee::Hyperbee;

#[tokio::test]
async fn write_rs() -> Result<(), Box<dyn std::error::Error>> {
    let hb = Hyperbee::from_storage_dir("written_by_rs").await?;
    hb.put(b"hello", Some(b"world")).await?;
    Ok(())
}
