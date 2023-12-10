use std::{path::Path, sync::Arc};
use tokio::sync::Mutex;

use hypercore::{self, HypercoreBuilder, Storage};

static HYPERBEE_STORAGE_DIR: &str = "./test_data/basic";

#[tokio::test]
async fn basic() -> Result<(), Box<dyn std::error::Error>> {
    let start = 0;
    let stop = 25;
    let path = Path::new(&HYPERBEE_STORAGE_DIR).to_owned();
    let storage = Storage::new_disk(&path, false).await.unwrap();
    let hc = HypercoreBuilder::new(storage).build().await.unwrap();

    let hc = Arc::new(Mutex::new(hc));
    let mut hb = hyperbee_rs::HyperbeeBuilder::default().core(hc).build()?;
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
