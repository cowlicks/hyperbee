use std::{path::Path, sync::Arc};

use derive_builder::Builder;
use futures_lite::Stream;
use hypercore::AppendOutcome;
use tokio::sync::RwLock;

use crate::{
    error::HyperbeeError,
    messages::header::Metadata,
    prefixed::{Prefixed, PrefixedConfig},
    traverse::{KeyDataResult, TraverseConfig},
    tree, KeyValueData,
};

use super::{tree::Tree, CoreMem, Shared};

/// An append only B-Tree built on [`Hypercore`](hypercore::Hypercore). It provides a key-value
/// store API, with methods for [inserting](Hyperbee::put), [getting](Hyperbee::get), and
/// [deleting](Hyperbee::del) key-value pair. As well as creating [sorted
/// iterators](Hyperbee::traverse), and ["sub" B-Trees](Hyperbee::sub) for grouping related data.
#[derive(Debug, Builder)]
#[builder(pattern = "owned", derive(Debug))]
pub struct Hyperbee<M: CoreMem> {
    tree: Shared<Tree>,
}

impl<M: CoreMem> Hyperbee {
    /// The number of blocks in the hypercore.
    /// The first block is always the header block so:
    /// `version` would be the `seq` of the next block
    /// `version - 1` is most recent block
    pub async fn version(&self) -> u64 {
        self.tree.read().await.version().await
    }

    /// Create the header for the Hyperbee. This must be done before writing anything else to the
    /// tree.
    pub async fn create_header(
        &self,
        metadata: Option<Metadata>,
    ) -> Result<AppendOutcome, HyperbeeError> {
        self.tree.read().await.create_header(metadata).await
    }

    /// The number of levels in the tree
    pub async fn height(&self) -> Result<usize, HyperbeeError> {
        self.tree.read().await.height().await
    }

    /// Returs a string representing the structure of the tree showing the keys in each node
    pub async fn print(&self) -> Result<String, HyperbeeError> {
        self.tree.read().await.print().await
    }

    /// Get the value corresponding to the provided `key` from the Hyperbee
    /// # Errors
    /// When `Hyperbee.get_root` fails
    pub async fn get(&self, key: &[u8]) -> Result<Option<(u64, Option<Vec<u8>>)>, HyperbeeError> {
        self.tree.read().await.get(key).await
    }

    /// Insert the given key and value into the tree
    /// Returs the `seq` of the new key, and `Option<u64>` which contains the `seq` of the old key
    /// if it was replaced.
    #[tracing::instrument(level = "trace", skip(self), ret)]
    pub async fn put(
        &self,
        key: &[u8],
        value: Option<&[u8]>,
    ) -> Result<(Option<u64>, u64), HyperbeeError> {
        self.tree.read().await.put(key, value).await
    }

    /// Like [`Hyperbee::put`] but takes a `compare_and_swap` function.
    /// The `compared_and_swap` function is called with the old key (if present), and the new key.
    /// The new key is only inserted if `compare_and_swap` returns true.
    /// Returs two `Option<u64>`s. The first is the old key, the second is the new key.
    pub async fn put_compare_and_swap(
        &self,
        key: &[u8],
        value: Option<&[u8]>,
        cas: impl FnOnce(Option<&KeyValueData>, &KeyValueData) -> bool,
    ) -> Result<(Option<u64>, Option<u64>), HyperbeeError> {
        self.tree
            .read()
            .await
            .put_compare_and_swap(key, value, cas)
            .await
    }

    /// Delete the given key from the tree.
    /// Returns the `seq` from the key if it was deleted.
    pub async fn del(&self, key: &[u8]) -> Result<Option<u64>, HyperbeeError> {
        self.tree.read().await.del(key).await
    }

    /// Like [`Hyperbee::del`] but takes a `compare_and_swap` function.
    /// Before deleting the function is called with existing key's [`KeyValueData`].
    /// The key is only deleted if `compare_and_swap` returs true.
    /// Returns the `bool` representing the result of `compare_and_swap`, and the `seq` for the
    /// key.
    pub async fn del_compare_and_swap(
        &self,
        key: &[u8],
        cas: impl FnOnce(&KeyValueData) -> bool,
    ) -> Result<Option<(bool, u64)>, HyperbeeError> {
        self.tree.read().await.del_compare_and_swap(key, cas).await
    }

    /// Create a new tree with all it's operation's prefixed by the provided `prefix`.
    pub fn sub(&self, prefix: &[u8], config: PrefixedConfig) -> Prefixed {
        Prefixed::new(prefix, self.tree.clone(), config)
    }

    /// Traverse the tree based on the given [`TraverseConfig`]
    pub async fn traverse<'a>(
        &self,
        conf: TraverseConfig,
    ) -> Result<impl Stream<Item = KeyDataResult> + 'a, HyperbeeError>
    where
        M: 'a,
    {
        self.tree.read().await.traverse(conf).await
    }
}

impl Hyperbee<random_access_disk::RandomAccessDisk> {
    /// Helper for creating a Hyperbee
    /// # Panics
    /// when storage path is incorrect
    /// when Hypercore failse to build
    /// when Blocks fails to build
    ///
    /// # Errors
    /// when Hyperbee fails to build
    pub async fn from_storage_dir<T: AsRef<Path>>(
        path_to_storage_dir: T,
    ) -> Result<Hyperbee<random_access_disk::RandomAccessDisk>, HyperbeeError> {
        let tree = tree::Tree::from_storage_dir(path_to_storage_dir).await?;
        Ok(HyperbeeBuilder::default()
            .tree(Arc::new(RwLock::new(tree)))
            .build()?)
    }
}

impl Hyperbee<random_access_memory::RandomAccessMemory> {
    /// Helper for creating a Hyperbee in RAM
    pub async fn from_ram(
    ) -> Result<Hyperbee<random_access_memory::RandomAccessMemory>, HyperbeeError> {
        let tree = tree::Tree::from_ram().await?;
        Ok(HyperbeeBuilder::default()
            .tree(Arc::new(RwLock::new(tree)))
            .build()?)
    }
}
