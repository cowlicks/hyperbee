use hyperbee_rs::{blocks::BlocksBuilder, Hyperbee, HyperbeeBuilder, HyperbeeError};
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
    for i in 0..4 {
        let key = vec![i];
        let val = vec![i];
        hb.put(&key, Some(val.clone())).await?;
        for j in 0..(i + 1) {
            let key = vec![j];
            let val = vec![j];
            let res = hb.get(&key).await?.unwrap();
            dbg!(&res);
            assert_eq!(res.1, val);
        }
    }
    Ok(())
}

#[tokio::test]
async fn basic_put_with_replace() -> Result<(), Box<dyn std::error::Error>> {
    let mut hb = in_memory_hyperbee().await?;
    for i in 0..4 {
        let key = vec![i];
        let val = vec![i];
        // initial values
        hb.put(&key, Some(val.clone())).await?;
        // replace replace with val + 1
        let val = vec![i + 1_u8];
        hb.put(&key, Some(val.clone())).await?;
        for j in 0..(i + 1) {
            let key = vec![j];
            let val = vec![j + 1];
            let res = hb.get(&key).await?.unwrap();
            dbg!(&res, &key, &val);
            assert_eq!(res.1, val);
        }
    }
    Ok(())
}

#[tokio::test]
async fn print_put() -> Result<(), Box<dyn std::error::Error>> {
    let mut hb = in_memory_hyperbee().await?;
    for i in 0..3 {
        let is = i.to_string();
        let key = is.clone().as_bytes().to_vec();
        let val = is.clone().as_bytes().to_vec();
        hb.put(&key, Some(val.clone())).await?;
    }
    let tree = hb.print().await?;
    assert_eq!(
        tree,
        "0
1
2
"
    );
    Ok(())
}

#[tokio::test]
async fn multilevel_put() -> Result<(), Box<dyn std::error::Error>> {
    let mut hb = in_memory_hyperbee().await?;
    for i in 0..10 {
        let is = i.to_string();
        let key = is.clone().as_bytes().to_vec();
        let val = is.clone().as_bytes().to_vec();
        println!("Put {i}");
        hb.put(&key, Some(val.clone())).await?;
        let tree = hb.print().await?;
        println!("{tree}");

        for j in 0..(i + 1) {
            let js = j.to_string();
            let key = js.clone().as_bytes().to_vec();
            let val = js.clone().as_bytes().to_vec();
            println!("Get {j}");
            let res = hb.get(&key).await?.unwrap();
            assert_eq!(res.1, val);
        }
    }
    Ok(())
}
