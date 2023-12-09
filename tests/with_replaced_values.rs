use std::{path::Path, sync::Arc};
use tokio::sync::Mutex;

use hypercore::{self, HypercoreBuilder, Storage};

static HYPERBEE_STORAGE_DIR: &str = "./test_data/with_replaced_values";

#[tokio::test]
async fn with_replaced_values() -> Result<(), Box<dyn std::error::Error>> {
    let start = 0;
    let stop = 25;
    let path = Path::new(&HYPERBEE_STORAGE_DIR).to_owned();
    let storage = Storage::new_disk(&path, false).await.unwrap();
    let hc = HypercoreBuilder::new(storage).build().await.unwrap();

    let hc = Arc::new(Mutex::new(hc));
    let mut hb = hyperbee_rs::HyperbeeBuilder::default().core(hc).build()?;
    for i in start..stop {
        let key = i.to_string();
        let expected = (i * 2).to_string();

        let (seq, res) = hb.get(key.as_bytes().into()).await?.unwrap();
        let res = std::str::from_utf8(&res).unwrap();
        println!("i {i} seq {seq} res {res}");
        assert_eq!(res, expected);
    }
    Ok(())
}
