use hyperbee_rs::{BlocksBuilder, Hyperbee, HyperbeeBuilder, HyperbeeError};
use hypercore::{HypercoreBuilder, Storage};

use std::sync::Arc;
use tokio::sync::RwLock;

async fn in_memory_hyperbee(
) -> Result<Hyperbee<random_access_memory::RandomAccessMemory>, HyperbeeError> {
    let hc = Arc::new(RwLock::new(
        HypercoreBuilder::new(Storage::new_memory().await?)
            .build()
            .await?,
    ));
    let blocks = BlocksBuilder::default().core(hc).build().unwrap();
    Ok(HyperbeeBuilder::default()
        .blocks(Arc::new(RwLock::new(blocks)))
        .build()?)
}
#[tokio::test]
async fn basic_put() -> Result<(), Box<dyn std::error::Error>> {
    let mut hb = in_memory_hyperbee().await?;
    let key = vec![1, 2];
    let val = vec![3, 4];
    hb.put(&key, val.clone()).await?;
    let result = hb.get(&key).await?;
    assert_eq!(result.unwrap().1, val);
    Ok(())
}
