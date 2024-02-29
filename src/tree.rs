use derive_builder::Builder;
use futures_lite::{Stream, StreamExt};
use hypercore::{AppendOutcome, HypercoreBuilder, Storage};
use prost::Message;

use crate::{
    blocks::{Blocks, BlocksBuilder},
    error::HyperbeeError,
    messages::{header::Metadata, Header},
    nearest_node,
    traverse::{self, KeyDataResult, Traverse, TraverseConfig},
    CoreMem, Node, Shared, PROTOCOL,
};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::sync::{Mutex, RwLock};

/// A key/value store built on [`hypercore::Hypercore`]. It uses an append only
/// [B-Tree](https://en.wikipedia.org/wiki/B-tree) and is compatible with the [JavaScript Hyperbee
/// library](https://docs.holepunch.to/building-blocks/hyperbee)
#[derive(Debug, Builder)]
#[builder(pattern = "owned", derive(Debug))]
pub struct Tree<M: CoreMem> {
    pub blocks: Shared<Blocks<M>>,
}

impl<M: CoreMem> Tree<M> {
    /// The number of blocks in the hypercore.
    /// The first block is always the header block so:
    /// `version` would be the `seq` of the next block
    /// `version - 1` is most recent block
    pub async fn version(&self) -> u64 {
        self.blocks.read().await.info().await.length
    }
    /// Gets the root of the tree.
    /// When `ensure_header == true` write the hyperbee header onto the hypercore if it does not exist.
    pub async fn get_root(
        &self,
        ensure_header: bool,
    ) -> Result<Option<Shared<Node<M>>>, HyperbeeError> {
        let blocks = self.blocks.read().await;
        let version = self.version().await;
        if version <= 1 {
            if version == 0 && ensure_header {
                self.ensure_header().await?;
            }
            return Ok(None);
        }
        let root = blocks
            .get(&(version - 1), self.blocks.clone())
            .await?
            .read()
            .await
            .get_tree_node(0)?;
        Ok(Some(root))
    }

    /// Get the value corresponding to the provided `key` from the Hyperbee
    /// # Errors
    /// When `Hyperbee.get_root` fails
    pub async fn get(&self, key: &[u8]) -> Result<Option<(u64, Option<Vec<u8>>)>, HyperbeeError> {
        let node = match self.get_root(false).await? {
            None => return Ok(None),
            Some(node) => node,
        };
        let (matched, path) = nearest_node(node, key).await?;
        if matched.is_some() {
            let (node, key_index) = path
                .last()
                .expect("Since `matched` was true, there must be at least one node in `path`");
            let kv = node.read().await.get_key_value(*key_index).await?;
            return Ok(Some((kv.seq, kv.value)));
        }
        Ok(None)
    }

    /// Ensure the tree has a header
    async fn ensure_header(&self) -> Result<bool, HyperbeeError> {
        match self.create_header(None).await {
            Ok(_) => Ok(true),
            Err(e) => match e {
                HyperbeeError::HeaderAlreadyExists => Ok(false),
                other_errors => Err(other_errors),
            },
        }
    }

    /// Create the header for the Hyperbee. This must be done before writing anything else to the
    /// tree.
    pub async fn create_header(
        &self,
        metadata: Option<Metadata>,
    ) -> Result<AppendOutcome, HyperbeeError> {
        if self.blocks.read().await.info().await.length != 0 {
            return Err(HyperbeeError::HeaderAlreadyExists);
        }
        let header = Header {
            protocol: PROTOCOL.to_string(),
            metadata,
        };
        let mut buf = Vec::with_capacity(header.encoded_len());
        header
            .encode(&mut buf)
            .map_err(HyperbeeError::HeaderEncodingError)?;
        self.blocks.read().await.append(&buf).await
    }

    /// Returs a string representing the structure of the tree showing the keys in each node
    pub async fn print(&self) -> Result<String, HyperbeeError> {
        let root = self
            .get_root(false)
            .await?
            .ok_or(HyperbeeError::NoRootError)?;
        let out = traverse::print(root).await?;
        Ok(out)
    }

    /// Traverse the tree based on the given [`TraverseConfig`]
    pub async fn traverse<'a>(
        &self,
        conf: TraverseConfig,
    ) -> Result<impl Stream<Item = KeyDataResult> + 'a, HyperbeeError>
    where
        M: 'a,
    {
        let root = self
            .get_root(false)
            .await?
            .ok_or(HyperbeeError::NoRootError)?;
        let stream = Traverse::new(root, conf);
        Ok(stream.map(move |kv_and_node| kv_and_node.0))
    }
}

impl Tree<random_access_disk::RandomAccessDisk> {
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
    ) -> Result<Tree<random_access_disk::RandomAccessDisk>, HyperbeeError> {
        let p: PathBuf = path_to_storage_dir.as_ref().to_owned();
        let storage = Storage::new_disk(&p, false).await?;
        let hc = Arc::new(Mutex::new(HypercoreBuilder::new(storage).build().await?));
        let blocks = BlocksBuilder::default().core(hc).build()?;
        Ok(TreeBuilder::default()
            .blocks(Arc::new(RwLock::new(blocks)))
            .build()?)
    }
}

impl Tree<random_access_memory::RandomAccessMemory> {
    /// Helper for creating a Hyperbee in RAM
    pub async fn from_ram() -> Result<Tree<random_access_memory::RandomAccessMemory>, HyperbeeError>
    {
        let hc = Arc::new(Mutex::new(
            HypercoreBuilder::new(Storage::new_memory().await?)
                .build()
                .await?,
        ));
        let blocks = BlocksBuilder::default().core(hc).build()?;
        Ok(TreeBuilder::default()
            .blocks(Arc::new(RwLock::new(blocks)))
            .build()?)
    }
}

impl<M: CoreMem> Clone for Tree<M> {
    fn clone(&self) -> Self {
        Self {
            blocks: self.blocks.clone(),
        }
    }
}
