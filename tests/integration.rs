use hyperbee_rs;
use std::{path::Path, sync::Arc};
use tokio::sync::Mutex;

use hypercore::{self, HypercoreBuilder, Storage};

static HYPERBEE_STORAGE_DIR: &str = "./test_data/basic";

#[tokio::test]
async fn integration() -> Result<(), Box<dyn std::error::Error>> {
    let N = 25;
    let path = Path::new(&HYPERBEE_STORAGE_DIR).to_owned();
    let storage = Storage::new_disk(&path, false).await.unwrap();
    let mut hc = HypercoreBuilder::new(storage).build().await.unwrap();

    let hc = Arc::new(Mutex::new(hc));
    let mut hb = hyperbee_rs::HyperbeeBuilder::default().core(hc).build()?;
    for i in 0..N {
        println!("{i}");
        let x = i.to_string();
        let res = hb.get(x.as_bytes().into()).await?.unwrap();
        let res = std::str::from_utf8(&res).unwrap();
        assert_eq!(res, x);
    }

    Ok(())
}
