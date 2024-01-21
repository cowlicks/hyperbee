#![cfg(test)]

use std::sync::Arc;

use hypercore::{HypercoreBuilder, Storage};
use tokio::sync::{OnceCell, RwLock};

use crate::{
    blocks::BlocksBuilder, CoreMem, Hyperbee, HyperbeeBuilder, HyperbeeError, SharedNode, MAX_KEYS,
};

static INIT_LOG: OnceCell<()> = OnceCell::const_new();
pub async fn setup_logs() {
    INIT_LOG
        .get_or_init(|| async {
            tracing_subscriber::fmt::init();
        })
        .await;
}

pub async fn check_tree<M: CoreMem>(
    mut hb: Hyperbee<M>,
) -> Result<Hyperbee<M>, Box<dyn std::error::Error>> {
    let root = hb
        .get_root(false)
        .await?
        .expect("We would only be checking an exiting tree");

    if root.read().await.keys.len() > MAX_KEYS {
        panic!("too many keys!");
    }
    Ok(hb)
}
pub async fn in_memory_hyperbee(
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
macro_rules! hb_put {
    ( $stop:expr ) => {
        async move {
            let mut hb = in_memory_hyperbee().await?;
            for i in 0..($stop) {
                let key = i.to_string().clone().as_bytes().to_vec();
                let val = key.clone();
                hb.put(&key, Some(val.clone())).await?;
            }
            Ok::<Hyperbee<RandomAccessMemory>, HyperbeeError>(hb)
        }
    };
}
