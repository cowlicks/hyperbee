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
    offset: u64,             // correct?
    _value: Option<Vec<u8>>, // correct?
}

#[derive(Clone, Debug)]
/// A block off the Hypercore
pub struct BlockEntry {
    /// index in the hypercore
    seq: u64,
    /// Pointers::new(Node::new(hypercore.get(seq)).index))
    _index: Option<Pointers>,
    /// Node::new(hypercore.get(seq)).index
    index_buffer: Vec<u8>,
    /// Node::new(hypercore.get(seq)).key
    key: Vec<u8>,
    /// Node::new(hypercore.get(seq)).value
    value: Option<Vec<u8>>,
}

#[derive(Debug, Builder)]
#[builder(pattern = "owned", derive(Debug))]
pub struct Blocks<M: CoreMem> {
    #[builder(default)]
    cache: BTreeMap<u64, Arc<RwLock<BlockEntry>>>,
    core: Hypercore<M>,
}

impl<M: CoreMem> Blocks<M> {
    async fn get(&mut self, seq: &u64) -> Result<Arc<RwLock<BlockEntry>>, HyperbeeError> {
        match self.cache.get(&seq) {
            Some(be) => Ok(be.clone()),
            None => {
                let be = self
                    ._get_block(seq)
                    .await?
                    .ok_or(HyperbeeError::NoBlockAtSeqError(*seq))?;
                let be = Arc::new(RwLock::new(be));
                self.cache.insert(*seq, be.clone());
                Ok(be)
            }
        }
    }
    async fn _get_block(&mut self, seq: &u64) -> Result<Option<BlockEntry>, HyperbeeError> {
        match self.core.get(*seq).await? {
            Some(core_block) => {
                let node = Node::decode(&core_block[..])?;
                Ok(Some(BlockEntry::new(*seq, node)))
            }
            None => Ok(None),
        }
    }
}

/// A node in the tree
#[derive(Debug)]
pub struct TreeNode<M: CoreMem> {
    keys: Vec<Key>,
    children: Vec<Child>,
    blocks: Arc<RwLock<Blocks<M>>>,
}

#[derive(Debug, Builder)]
#[builder(pattern = "owned", derive(Debug))]
pub struct Hyperbee<M: CoreMem> {
    blocks: Arc<RwLock<Blocks<M>>>,
    // TODO add root here so tree is not dropped after each .get()
    //#[builder(default)]
    //root: Option<TreeNode<M>>,
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

/// The function we use to get key's keys, key's values, and TreeNodes out of the core
/// Getting a key's key:
/// get_block(core, key.seq).await?.unwrap().key
/// Getting a key's value:
/// get_block(core, key.seq).await?.unwrap().value
/// Getting a child as TreeNode:
/// get_block(core, child.seq).await?.unwrap().get_tree_node(child.offset)
async fn get_block<M: CoreMem>(
    blocks: &Arc<RwLock<Blocks<M>>>,
    seq: u64,
) -> Result<Option<BlockEntry>, HyperbeeError> {
    match blocks.write().await.core.get(seq).await? {
        Some(core_block) => {
            let node = Node::decode(&core_block[..])?;
            Ok(Some(BlockEntry::new(seq, node)))
        }
        None => Ok(None),
    }
}

impl Key {
    fn new(seq: u64, value: Option<Vec<u8>>) -> Self {
        Key { seq, value }
    }
}

impl Child {
    fn new(seq: u64, offset: u64, _value: Option<Vec<u8>>) -> Self {
        Child {
            seq,
            offset,
            _value,
        }
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
                    children.push(Child::new(
                        level.children[i],
                        level.children[i + 1],
                        Option::None,
                    ));
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

impl<M: CoreMem> TreeNode<M> {
    fn new(
        block: BlockEntry,
        keys: Vec<Key>,
        children: Vec<Child>,
        blocks: Arc<RwLock<Blocks<M>>>,
    ) -> Self {
        TreeNode {
            keys,
            children,
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
            .write()
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
            .write()
            .await
            .get(seq)
            .await?
            .read()
            .await
            .value
            .clone()
            .map(|v| (*seq, v)))
    }

    async fn get_child(&self, index: usize) -> Result<TreeNode<M>, HyperbeeError> {
        let child = &self.children[index];
        Ok(self
            .blocks
            .write()
            .await
            .get(&child.seq)
            .await?
            .read()
            .await
            .clone()
            .get_tree_node(child.offset, self.blocks.clone())?)
        /*
        let child_block = get_block(&self.blocks, child.seq)
            .await?
            .ok_or(HyperbeeError::NoChildAtSeqError(child.seq))?;
        child_block.get_tree_node(child.offset, self.blocks.clone())
        )*/
    }
}

impl BlockEntry {
    fn new(seq: u64, entry: Node) -> Self {
        BlockEntry {
            seq,
            _index: Option::None,
            index_buffer: entry.index,
            key: entry.key,
            value: entry.value,
        }
    }

    fn is_target(&self, key: &[u8]) -> bool {
        key == self.key
    }

    /// offset is the offset of the node within the hypercore block
    fn get_tree_node<M: CoreMem>(
        self,
        offset: u64,
        blocks: Arc<RwLock<Blocks<M>>>,
    ) -> Result<TreeNode<M>, HyperbeeError> {
        let pointers = Pointers::new(&self.index_buffer[..])?;
        let node_data = pointers.get(offset as usize);
        Ok(TreeNode::new(
            self,
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
        self.blocks.read().await.core.info().length
    }
    /// Gets the root of the tree
    async fn get_root(&mut self, _ensure_header: bool) -> Result<TreeNode<M>, HyperbeeError> {
        let block = get_block(&self.blocks, self.version().await - 1)
            .await?
            .ok_or(HyperbeeError::NoRootError())?;
        block.get_tree_node(0, self.blocks.clone())
    }

    /// Get the value associated with a key
    pub async fn get(&mut self, key: &Vec<u8>) -> Result<Option<(u64, Vec<u8>)>, HyperbeeError> {
        let mut node = self.get_root(false).await?;
        loop {
            // TODO do this with a binary search
            // find the matching key, or next child
            let child_index: usize = 'found: {
                for i in 0..node.keys.len() {
                    let val = node.get_key(i).await?;
                    // found matching child
                    if *key < val {
                        break 'found i;
                    }
                    // found matching key
                    if val == *key {
                        return node.get_value_of_key(i).await;
                    }
                }
                // key is greater than all of this nodes keys, take last child
                node.keys.len()
            };

            // leaf node with no match
            if node.children.is_empty() {
                return Ok(None);
            }

            // get next node
            node = node.get_child(child_index).await?;
        }
    }
}

pub async fn load_from_storage_dir(
    storage_dir: &str,
) -> Result<Hyperbee<random_access_disk::RandomAccessDisk>, HyperbeeError> {
    let path = Path::new(storage_dir).to_owned();
    let storage = Storage::new_disk(&path, false).await.unwrap();
    let hc = HypercoreBuilder::new(storage).build().await.unwrap();
    let blocks = BlocksBuilder::default().core(hc).build().unwrap();
    Ok(HyperbeeBuilder::default()
        .blocks(Arc::new(RwLock::new(blocks)))
        .build()?)
}
