use hyperbee_rs;
use std::{
    path::Path,
    sync::{Arc, Mutex},
};

use hypercore::{self, HypercoreBuilder, Storage};

static HYPERBEE_STORAGE_DIR: &str = "./hb2";

#[tokio::test]
async fn integration() -> Result<(), Box<dyn std::error::Error>> {
    let path = Path::new(&HYPERBEE_STORAGE_DIR).to_owned();
    let storage = Storage::new_disk(&path, false).await.unwrap();
    let hc = HypercoreBuilder::new(storage).build().await.unwrap();
    let hc = Arc::new(Mutex::new(hc));
    let mut hb = hyperbee_rs::HyperbeeBuilder::default().core(hc).build()?;
    //dbg!(hb.core.info());
    let buf = "hello".as_bytes();
    let x = hb.get(buf.into()).await?.unwrap();

    dbg!(&x);
    Ok(())
}
