use std::sync::Arc;
use tokio::sync::RwLock;

use crate::{
    prefixed::{Prefixed as RustPrefixed, PrefixedConfig as RustPrefixedConfig},
    Hyperbee as RustHyperbee, HyperbeeError, Shared,
};

#[derive(Debug, uniffi::Record)]
pub struct Gotten {
    pub seq: u64,
    pub value: Option<Vec<u8>>,
}

impl Gotten {
    fn new(seq: u64, value: Option<Vec<u8>>) -> Self {
        Self { seq, value }
    }
}

#[derive(Debug, uniffi::Record)]
pub struct Putten {
    pub old_seq: Option<u64>,
    pub new_seq: u64,
}

impl Putten {
    fn new(old_seq: Option<u64>, new_seq: u64) -> Self {
        Self { old_seq, new_seq }
    }
}

/// An append only B-Tree built on [`Hypercore`](hypercore::Hypercore). It provides a key-value
/// store API, with methods for [inserting](Hyperbee::put), [getting](Hyperbee::get), and
/// [deleting](Hyperbee::del) key-value pair. As well as creating [sorted
/// iterators](Hyperbee::traverse), and ["sub" B-Trees](Hyperbee::sub) for grouping related data.
#[derive(Debug, uniffi::Object)]
struct Hyperbee {
    rust_hyperbee: Shared<RustHyperbee>,
}

#[uniffi::export(async_runtime = "tokio")]
impl Hyperbee {
    /// The number of blocks in the hypercore.
    /// The first block is always the header block so:
    /// `version` would be the `seq` of the next block
    /// `version - 1` is most recent block
    pub async fn version(&self) -> u64 {
        self.rust_hyperbee.read().await.version().await
    }

    /// Get the value corresponding to the provided `key` from the Hyperbee
    async fn get(&self, key: &[u8]) -> Result<Option<Gotten>, HyperbeeError> {
        Ok(self
            .rust_hyperbee
            .read()
            .await
            .get(key)
            .await?
            .map(|(seq, value)| Gotten::new(seq, value)))
    }

    /// Insert the given key and value into the tree
    /// Returs the `seq` of the new key, and `Option<u64>` which contains the `seq` of the old key
    /// if it was replaced.
    async fn put(&self, key: &[u8], value: Option<Vec<u8>>) -> Result<Putten, HyperbeeError> {
        let (old_seq, new_seq) = self
            .rust_hyperbee
            .read()
            .await
            .put(key, value.as_deref())
            .await?;
        Ok(Putten::new(old_seq, new_seq))
    }

    /// Delete the given key from the tree.
    /// Returns the `seq` from the key if it was deleted.
    async fn delete(&self, key: &[u8]) -> Result<Option<u64>, HyperbeeError> {
        self.rust_hyperbee.read().await.del(key).await
    }

    /// Create a new tree with all it's operation's prefixed by the provided `prefix`.
    /// Requires a config object to define the seperator between the prefix and the key.
    /// Create the config with [`default_sub_config`]. This provides gives a NULL bytes seperator,
    /// which is the same value as the Holepunch JavaScript library.
    async fn sub(&self, prefix: &[u8], config: RustPrefixedConfig) -> Prefixed {
        let rust_prefixed = self.rust_hyperbee.read().await.sub(prefix, config);
        Prefixed {
            rust_prefixed: Arc::new(RwLock::new(rust_prefixed)),
        }
    }
}

/// Helper for creating a Hyperbee that exists in RAM.
#[uniffi::export]
async fn hyperbee_from_ram() -> Result<Hyperbee, HyperbeeError> {
    let rust_hyperbee = RustHyperbee::from_ram().await?;
    Ok(Hyperbee {
        rust_hyperbee: Arc::new(RwLock::new(rust_hyperbee)),
    })
}

/// Helper for creating a Hyperbee from the provided path to a storage directory
#[uniffi::export(async_runtime = "tokio")]
async fn hyperbee_from_storage_dir(path_to_storage_dir: &str) -> Result<Hyperbee, HyperbeeError> {
    let rust_hyperbee = RustHyperbee::from_storage_dir(path_to_storage_dir).await?;
    Ok(Hyperbee {
        rust_hyperbee: Arc::new(RwLock::new(rust_hyperbee)),
    })
}

#[derive(Debug, uniffi::Object)]
struct Prefixed {
    rust_prefixed: Shared<RustPrefixed>,
}

#[uniffi::export(async_runtime = "tokio")]
impl Prefixed {
    /// Get the value associated with the provided key plus this [`Prefixed`]'s instance's `prefix`
    async fn get(&self, key: &[u8]) -> Result<Option<Gotten>, HyperbeeError> {
        Ok(self
            .rust_prefixed
            .read()
            .await
            .get(key)
            .await?
            .map(|(seq, value)| Gotten::new(seq, value)))
    }

    /// Insert the value into the tree with the provided key plus this [`Prefixed`]'s instance's `prefix`
    async fn put(&self, key: &[u8], value: Option<Vec<u8>>) -> Result<Putten, HyperbeeError> {
        let (old_seq, new_seq) = self
            .rust_prefixed
            .read()
            .await
            .put(key, value.as_deref())
            .await?;
        Ok(Putten::new(old_seq, new_seq))
    }

    /// Insert the provided key and it's value. Key is is Prefixed by this [`Prefixed`]'s instance's `prefix`
    async fn delete(&self, key: &[u8]) -> Result<Option<u64>, HyperbeeError> {
        self.rust_prefixed.read().await.del(key).await
    }
}

/// Create the default config for creating a [`Prefixed`] instance
#[uniffi::export]
fn default_sub_config() -> RustPrefixedConfig {
    RustPrefixedConfig::default()
}
