use hyperbee_rs;
use std::{path::Path, sync::Arc};
use tokio::sync::Mutex;

use hypercore::{self, HypercoreBuilder, Storage};

static HYPERBEE_STORAGE_DIR: &str = "./test_data/basic";

#[tokio::test]
async fn integration() -> Result<(), Box<dyn std::error::Error>> {
    let path = Path::new(&HYPERBEE_STORAGE_DIR).to_owned();
    let storage = Storage::new_disk(&path, false).await.unwrap();
    let mut hc = HypercoreBuilder::new(storage).build().await.unwrap();
    let _ = hc.get(11).await.unwrap().unwrap();

    let hc = Arc::new(Mutex::new(hc));
    let mut hb = hyperbee_rs::HyperbeeBuilder::default().core(hc).build()?;
    let buf = "0".as_bytes();
    let x = hb.get(buf.into()).await?.unwrap();
    let x = std::str::from_utf8(&x).unwrap();
    assert_eq!(x, "0");

    let buf = "1".as_bytes();
    let x = hb.get(buf.into()).await?.unwrap();
    let x = std::str::from_utf8(&x).unwrap();
    assert_eq!(x, "1");

    let buf = "2".as_bytes();
    let x = hb.get(buf.into()).await?.unwrap();
    let x = std::str::from_utf8(&x).unwrap();
    assert_eq!(x, "2");
    Ok(())
}
