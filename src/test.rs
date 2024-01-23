#![cfg(test)]

use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

use hypercore::{HypercoreBuilder, Storage};
use tokio::sync::{OnceCell, RwLock};

use crate::{
    blocks::BlocksBuilder, CoreMem, Hyperbee, HyperbeeBuilder, HyperbeeError, SharedNode, MAX_KEYS,
};

#[allow(dead_code)]
static INIT_LOG: OnceCell<()> = OnceCell::const_new();
#[allow(dead_code)]
pub async fn setup_logs() {
    INIT_LOG
        .get_or_init(|| async {
            tracing_subscriber::fmt::init();
        })
        .await;
}

fn is_sorted(arr: &Vec<Vec<u8>>) {
    for i in 0..arr.len() {
        let len = arr.len();
        if len != 0 && i < len - 1 && arr[i] >= arr[i + 1] {
            panic!("elements {i} > {}.. out of order", i + 1);
        }
    }
}

fn interleave<T: Clone>(a: &Vec<T>, b: &Vec<T>) -> Vec<T> {
    let (a_len, b_len) = (a.len(), b.len());
    let max_len = std::cmp::max(a_len, b_len);
    let mut out = vec![];
    for i in 0..max_len {
        if i < a_len {
            out.push(a[i].clone());
        }
        if i < b_len {
            out.push(b[i].clone());
        }
    }
    out
}

#[async_recursion::async_recursion(?Send)]
pub async fn check_node<M: CoreMem>(node: SharedNode<M>) {
    let (n_keys, n_children) = {
        let r_node = node.read().await;
        (r_node.n_keys().await, r_node.n_children().await)
    };

    if n_keys > MAX_KEYS {
        panic!("too many keys!");
    }

    if n_children != 0 && (n_children != n_keys + 1) {
        panic!("kids exist but # kids [{n_children}] != # keys [{n_keys}] + 1");
    }

    let mut keys_vec = vec![];
    for i in 0..n_keys {
        keys_vec.push(
            node.write()
                .await
                .get_key(i)
                .await
                .expect("should always get key in tests"),
        );
    }

    is_sorted(&keys_vec);
    let mut children_vec = vec![];
    for i in 0..n_children {
        let child = node
            .read()
            .await
            .get_child(i)
            .await
            .expect("get child always works in tests");
        children_vec.push(child.write().await.get_key(0).await.unwrap());
        check_node(child).await;
    }
    is_sorted(&children_vec);
    let interleaved = interleave(&children_vec, &keys_vec);
    is_sorted(&interleaved);
}

/// Used for testing. Asserts various invariants hold within the tree.
/// Invariants checked for each node are:
/// * # keys is greater than min keys (unless node is root)
/// * # keys is less than max keys
/// * keys are ordered
/// * children values are ordered
/// * # keys + 1 == # children unless this is a leaf node
/// * key's keys are between the nodes
/// * all children respect these invariants
pub async fn check_tree<M: CoreMem>(
    mut hb: Hyperbee<M>,
) -> Result<Hyperbee<M>, Box<dyn std::error::Error>> {
    let root = hb
        .get_root(false)
        .await?
        .expect("We would only be checking an exiting tree");

    let (n_keys, n_children) = {
        let r_root = root.read().await;
        (r_root.n_keys().await, r_root.n_children().await)
    };

    if n_keys > MAX_KEYS {
        panic!("too many keys!");
    }

    if n_children != 0 && (n_children != n_keys + 1) {
        panic!("kids exist but # kids [{n_children}] != # keys [{n_keys}] + 1");
    }

    for i in 0..n_children {
        let child = root
            .read()
            .await
            .get_child(i)
            .await
            .expect("get child always works in tests");
        check_node(child).await;
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
#[allow(unused_macros)]
/// Macro used for creating trees for testing.
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

/// Seedable pseudorandom number generator used for reproducible randomized testing
/// NB: we choose [`u32`] for seed and counter and [`f64`] as `sin_scale` and `rand`'s return value
/// because you can convert between `u32` and `f64` losslessly.
pub struct Rand {
    seed: u64,
    counter: AtomicU64,
    sin_scale: f64,
    ordering: Ordering,
}

impl Rand {
    pub fn rand(&self) -> f64 {
        let count = self.counter.fetch_add(1, self.ordering);
        let x = ((self.seed + count) as f64).sin() * self.sin_scale;
        x - x.floor()
    }
    pub fn rand_int_lt(&self, max: u64) -> u64 {
        (self.rand() * (max as f64)).floor() as u64
    }
    pub fn shuffle<T>(&self, mut arr: Vec<T>) -> Vec<T> {
        let mut out = vec![];
        while !arr.is_empty() {
            let i = self.rand_int_lt(arr.len() as u64) as usize;
            out.push(arr.remove(i));
        }
        out
    }
}

impl Default for Rand {
    fn default() -> Self {
        Self {
            seed: 42,
            counter: Default::default(),
            sin_scale: 10_000_f64,
            ordering: Ordering::SeqCst,
        }
    }
}

#[test]
fn deterministic_rand_test() {
    let r = Rand::default();
    assert_eq!(r.rand(), 0.7845208436629036);
    assert_eq!(r.rand(), 0.2525737140167621);
    assert_eq!(r.rand(), 0.01925105413576489);
    assert_eq!(r.rand(), 0.03524534118514566);
    assert_eq!(r.rand(), 0.8834764880921284);
}
