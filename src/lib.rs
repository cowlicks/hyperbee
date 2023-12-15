/// Rust version of [hyperbee](https://github.com/holepunchto/hyperbee)
/// A B-tree built on top of Hypercore.
pub mod messages {
    include!(concat!(env!("OUT_DIR"), "/_.rs"));
}

use derive_builder::Builder;
use hypercore::{Hypercore, HypercoreBuilder, HypercoreError, Storage};
use messages::{Node, YoloIndex};
use prost::{bytes::Buf, DecodeError, Message};
use random_access_storage::RandomAccess;
use thiserror::Error;

use std::{collections::BTreeMap, fmt::Debug, path::Path, sync::Arc};
use tokio::sync::RwLock;

pub trait CoreMem: RandomAccess + Debug + Send {}
impl<T: RandomAccess + Debug + Send> CoreMem for T {}

#[derive(Error, Debug)]
pub enum HyperbeeError {
    #[error("There was an error in the underlying Hypercore")]
    HypercoreError(#[from] HypercoreError),
    #[error("There was an error decoding Hypercore data")]
    DecodeError(#[from] DecodeError),
    #[error("Hyperbee has no root")]
    NoRootError(),
    #[error("No key at seq  `{0}`")]
    NoKeyAtSeqError(u64),
    #[error("No key at seq  `{0}`")]
    NoValueAtSeqError(u64),
    #[error("No child at seq  `{0}`")]
    NoChildAtSeqError(u64),
    #[error("No block at seq  `{0}`")]
    NoBlockAtSeqError(u64),
    #[error("There was an error building the hyperbee")]
    HyperbeeBuilderError(#[from] HyperbeeBuilderError),
    //#[error("There was an error building the block cache")]
    //BlocksBuilderError(#[from] BlocksBuilderError),
}

#[derive(Clone, Debug)]
struct Key {
    seq: u64,
    value: Option<Vec<u8>>,
}

#[derive(Clone, Debug)]
struct Child {
    seq: u64,
    offset: u64,
}

#[derive(Clone, Debug)]
/// A block off the Hypercore
pub struct BlockEntry {
    /// Pointers::new(Node::new(hypercore.get(seq)).index))
    index: Pointers,
    /// Node::new(hypercore.get(seq)).key
    key: Vec<u8>,
    /// Node::new(hypercore.get(seq)).value
    value: Option<Vec<u8>>,
}

#[derive(Debug, Builder)]
#[builder(pattern = "owned", derive(Debug))]
pub struct Blocks<M: CoreMem> {
    #[builder(default)]
    cache: Arc<RwLock<BTreeMap<u64, Arc<RwLock<BlockEntry>>>>>,
    core: Arc<RwLock<Hypercore<M>>>,
}

type ChildPointersWithNodes<M> = RwLock<Vec<(Child, Option<Arc<RwLock<TreeNode<M>>>>)>>;
#[derive(Debug)]
struct Children<M: CoreMem> {
    blocks: Arc<RwLock<Blocks<M>>>,
    children: ChildPointersWithNodes<M>,
}

/// A node in the tree
#[derive(Debug)]
pub struct TreeNode<M: CoreMem> {
    keys: Vec<Key>,
    children: Children<M>,
    blocks: Arc<RwLock<Blocks<M>>>,
}

#[derive(Debug, Builder)]
#[builder(pattern = "owned", derive(Debug))]
pub struct Hyperbee<M: CoreMem> {
    blocks: Arc<RwLock<Blocks<M>>>,
    // TODO add root here so tree is not dropped after each .get()
    #[builder(default)]
    root: Option<Arc<RwLock<TreeNode<M>>>>,
}

#[derive(Clone, Debug)]
struct Level {
    keys: Vec<Key>,
    children: Vec<Child>,
}

#[derive(Clone, Debug)]
struct Pointers {
    levels: Vec<Level>,
}

impl Key {
    fn new(seq: u64, value: Option<Vec<u8>>) -> Self {
        Key { seq, value }
    }
}

impl Child {
    fn new(seq: u64, offset: u64) -> Self {
        Child { seq, offset }
    }
}

impl Level {
    fn new(keys: Vec<Key>, children: Vec<Child>) -> Self {
        Self { keys, children }
    }
}

impl Pointers {
    fn new<B: Buf>(buf: B) -> Result<Self, DecodeError> {
        let levels: Vec<_> = YoloIndex::decode(buf)?
            .levels
            .iter()
            .map(|level| {
                let keys = level
                    .keys
                    .iter()
                    .map(|k| Key::new(*k, Option::None))
                    .collect();
                let mut children = vec![];
                for i in (0..(level.children.len())).step_by(2) {
                    children.push(Child::new(level.children[i], level.children[i + 1]));
                }
                Level::new(keys, children)
            })
            .collect();
        Ok(Pointers { levels })
    }

    fn get(&self, i: usize) -> &Level {
        &self.levels[i]
    }
}

impl<M: CoreMem> Blocks<M> {
    pub async fn get(&self, seq: &u64) -> Result<Arc<RwLock<BlockEntry>>, HyperbeeError> {
        match self._get_from_cache(seq).await {
            Some(block) => Ok(block),
            None => {
                let be = self
                    ._get_from_core(seq)
                    .await?
                    .ok_or(HyperbeeError::NoBlockAtSeqError(*seq))?;
                let be = Arc::new(RwLock::new(be));
                self.cache.write().await.insert(*seq, be.clone());
                Ok(be)
            }
        }
    }
    async fn _get_from_cache(&self, seq: &u64) -> Option<Arc<RwLock<BlockEntry>>> {
        self.cache.read().await.get(seq).cloned()
    }

    async fn _get_from_core(&self, seq: &u64) -> Result<Option<BlockEntry>, HyperbeeError> {
        match self.core.write().await.get(*seq).await? {
            Some(core_block) => {
                let node = Node::decode(&core_block[..])?;
                Ok(Some(BlockEntry::new(node)?))
            }
            None => Ok(None),
        }
    }
    async fn info(&self) -> hypercore::Info {
        self.core.read().await.info()
    }
}

impl<M: CoreMem> Children<M> {
    fn new(blocks: Arc<RwLock<Blocks<M>>>, children: Vec<Child>) -> Self {
        Self {
            blocks,
            children: RwLock::new(children.into_iter().map(|c| (c, Option::None)).collect()),
        }
    }
    async fn get_child(&self, index: usize) -> Result<Arc<RwLock<TreeNode<M>>>, HyperbeeError> {
        let child_data = match &self.children.read().await[index] {
            (_, Some(node)) => return Ok(node.clone()),
            (child_data, None) => child_data.clone(),
        };
        let block = self.blocks.read().await.get(&child_data.seq).await?;
        let node = Arc::new(RwLock::new(
            block
                .read()
                .await
                .get_tree_node(child_data.offset, self.blocks.clone())?,
        ));
        self.children.write().await[index].1 = Some(node.clone());
        Ok(node)
    }

    async fn is_empty(&self) -> bool {
        self.children.read().await.is_empty()
    }
}

impl<M: CoreMem> TreeNode<M> {
    fn new(keys: Vec<Key>, children: Vec<Child>, blocks: Arc<RwLock<Blocks<M>>>) -> Self {
        TreeNode {
            keys,
            children: Children::new(blocks.clone(), children),
            blocks,
        }
    }
    async fn get_key(&self, index: usize) -> Result<Vec<u8>, HyperbeeError> {
        let key = &self.keys[index];
        if let Some(value) = &key.value {
            return Ok(value.clone());
        }
        Ok(self
            .blocks
            .read()
            .await
            .get(&key.seq)
            .await?
            .read()
            .await
            .key
            .clone())
    }

    async fn get_value_of_key(
        &self,
        index: usize,
    ) -> Result<Option<(u64, Vec<u8>)>, HyperbeeError> {
        let seq = &self.keys[index].seq;
        Ok(self
            .blocks
            .read()
            .await
            .get(seq)
            .await?
            .read()
            .await
            .value
            .clone()
            .map(|v| (*seq, v)))
    }

    async fn get_child(&self, index: usize) -> Result<Arc<RwLock<TreeNode<M>>>, HyperbeeError> {
        self.children.get_child(index).await
    }
}

impl BlockEntry {
    fn new(entry: Node) -> Result<Self, HyperbeeError> {
        Ok(BlockEntry {
            index: Pointers::new(&entry.index[..])?,
            key: entry.key,
            value: entry.value,
        })
    }

    /// offset is the offset of the node within the hypercore block
    fn get_tree_node<M: CoreMem>(
        &self,
        offset: u64,
        blocks: Arc<RwLock<Blocks<M>>>,
    ) -> Result<TreeNode<M>, HyperbeeError> {
        let node_data = self.index.get(offset as usize);
        Ok(TreeNode::new(
            node_data.keys.clone(),
            node_data.children.clone(),
            blocks,
        ))
    }
}

// TODO use builder pattern macros for Hyperbee opts
impl<M: CoreMem> Hyperbee<M> {
    /// trying to duplicate Js Hb.versinon
    pub async fn version(&self) -> u64 {
        self.blocks.read().await.info().await.length
    }
    /// Gets the root of the tree
    async fn get_root(&mut self) -> Result<Arc<RwLock<TreeNode<M>>>, HyperbeeError> {
        match &self.root {
            Some(root) => Ok(root.clone()),
            None => {
                let root = self
                    .blocks
                    .read()
                    .await
                    .get(&(self.version().await - 1))
                    .await?
                    .read()
                    .await
                    .get_tree_node(0, self.blocks.clone())?;
                let root = Arc::new(RwLock::new(root));
                self.root = Some(root.clone());
                Ok(root)
            }
        }
    }

    /// Get the value associated with a key
    pub async fn get(&mut self, key: &Vec<u8>) -> Result<Option<(u64, Vec<u8>)>, HyperbeeError> {
        let mut node = self.get_root().await?;
        loop {
            // TODO do this with a binary search
            // find the matching key, or next child
            let next_node = {
                let read_node = node.read().await;
                let child_index: usize = 'found: {
                    for i in 0..read_node.keys.len() {
                        let val = read_node.get_key(i).await?;
                        // found matching child
                        if *key < val {
                            break 'found i;
                        }
                        // found matching key
                        if val == *key {
                            return read_node.get_value_of_key(i).await;
                        }
                    }
                    // key is greater than all of this nodes keys, take last child, which has index
                    // of node.keys.len()
                    read_node.keys.len()
                };

                // leaf node with no match
                if read_node.children.is_empty().await {
                    return Ok(None);
                }

                // get next node
                read_node.get_child(child_index).await?.clone()
            };

            // swap current node with the next one
            node = next_node;
        }
    }
}

pub async fn load_from_storage_dir(
    storage_dir: &str,
) -> Result<Hyperbee<random_access_disk::RandomAccessDisk>, HyperbeeError> {
    let path = Path::new(storage_dir).to_owned();
    let storage = Storage::new_disk(&path, false).await.unwrap();
    let hc = Arc::new(RwLock::new(
        HypercoreBuilder::new(storage).build().await.unwrap(),
    ));
    let blocks = BlocksBuilder::default().core(hc).build().unwrap();
    Ok(HyperbeeBuilder::default()
        .blocks(Arc::new(RwLock::new(blocks)))
        .build()?)
}
