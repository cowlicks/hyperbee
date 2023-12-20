/// Rust version of [hyperbee](https://github.com/holepunchto/hyperbee)
/// A B-tree built on top of Hypercore.
pub mod messages {
    include!(concat!(env!("OUT_DIR"), "/_.rs"));
}
pub mod traverse;

use derive_builder::Builder;
use hypercore::{Hypercore, HypercoreBuilder, HypercoreError, Storage};
use messages::{Node as NodeSchema, YoloIndex};
use prost::{bytes::Buf, DecodeError, Message};
use random_access_storage::RandomAccess;
use thiserror::Error;

use std::{collections::BTreeMap, fmt::Debug, num::TryFromIntError, path::Path, sync::Arc};
use tokio::sync::RwLock;

pub trait CoreMem: RandomAccess + Debug + Send {}
impl<T: RandomAccess + Debug + Send> CoreMem for T {}

#[derive(Error, Debug)]
pub enum HyperbeeError {
    #[error("There was an error in the underlying Hypercore")]
    HypercoreError(#[from] HypercoreError),
    #[error("There was an error decoding Hypercore data")]
    DecodeError(#[from] DecodeError),
    #[error("No block at seq  `{0}`")]
    NoBlockAtSeqError(u64),
    #[error("There was an error building the hyperbee")]
    HyperbeeBuilderError(#[from] HyperbeeBuilderError),
    #[error("Converting a u64 value [{0}] to usize failed. This is possibly a 32bit platform. Got error {1}")]
    U64ToUsizeConversionError(u64, TryFromIntError),
    #[error("Could not traverse child node. Got error: {0}")]
    GetChildInTraverseError(Box<dyn std::error::Error>),
}

#[derive(Clone, Debug)]
pub struct Key {
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
    /// Pointers::new(NodeSchema::new(hypercore.get(seq)).index))
    index: Pointers,
    /// NodeSchema::new(hypercore.get(seq)).key
    key: Vec<u8>,
    /// NodeSchema::new(hypercore.get(seq)).value
    value: Option<Vec<u8>>,
}

type Shared<T> = Arc<RwLock<T>>;
type SharedNode<T> = Shared<Node<T>>;
type SharedBlock = Shared<BlockEntry>;

#[derive(Debug, Builder)]
#[builder(pattern = "owned", derive(Debug))]
pub struct Blocks<M: CoreMem> {
    #[builder(default)]
    cache: Shared<BTreeMap<u64, SharedBlock>>,
    core: Shared<Hypercore<M>>,
}

#[derive(Debug)]
struct Children<M: CoreMem> {
    blocks: Shared<Blocks<M>>,
    children: RwLock<Vec<(Child, Option<SharedNode<M>>)>>,
}

/// A node in the tree
#[derive(Debug)]
pub struct Node<M: CoreMem> {
    pub keys: Vec<Key>,
    children: Children<M>,
    blocks: Shared<Blocks<M>>,
}

#[derive(Debug, Builder)]
#[builder(pattern = "owned", derive(Debug))]
pub struct Hyperbee<M: CoreMem> {
    blocks: Shared<Blocks<M>>,
    // TODO add root here so tree is not dropped after each .get()
    #[builder(default)]
    root: Option<SharedNode<M>>,
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
    /// # Errors
    /// when the provided `seq` is not in the Hypercore
    /// when the data in the Hypercore block cannot be decoded
    pub async fn get(&self, seq: &u64) -> Result<Shared<BlockEntry>, HyperbeeError> {
        if let Some(block) = self._get_from_cache(seq).await {
            Ok(block)
        } else {
            let block_entry = self
                ._get_from_core(seq)
                .await?
                .ok_or(HyperbeeError::NoBlockAtSeqError(*seq))?;
            let block_entry = Arc::new(RwLock::new(block_entry));
            self.cache.write().await.insert(*seq, block_entry.clone());
            Ok(block_entry)
        }
    }
    async fn _get_from_cache(&self, seq: &u64) -> Option<Shared<BlockEntry>> {
        self.cache.read().await.get(seq).cloned()
    }

    async fn _get_from_core(&self, seq: &u64) -> Result<Option<BlockEntry>, HyperbeeError> {
        match self.core.write().await.get(*seq).await? {
            Some(core_block) => {
                let node = NodeSchema::decode(&core_block[..])?;
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
    fn new(blocks: Shared<Blocks<M>>, children: Vec<Child>) -> Self {
        Self {
            blocks,
            children: RwLock::new(children.into_iter().map(|c| (c, Option::None)).collect()),
        }
    }
    async fn get_child(&self, index: usize) -> Result<Shared<Node<M>>, HyperbeeError> {
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

async fn nearest_node<M: CoreMem>(
    node: SharedNode<M>,
    key: &Vec<u8>,
) -> Result<(bool, Vec<SharedNode<M>>, Vec<usize>), HyperbeeError> {
    let mut current_node = node;
    let mut node_path: Vec<SharedNode<M>> = vec![];
    let mut child_idxs: Vec<usize> = vec![];
    loop {
        node_path.push(current_node.clone());
        let next_node = {
            let read_node = current_node.read().await;
            let child_index: usize = 'found: {
                for i in 0..read_node.keys.len() {
                    let val = read_node.get_key(i).await?;
                    // found matching child
                    if *key < val {
                        child_idxs.push(i);
                        break 'found i;
                    }
                    // found matching key
                    if val == *key {
                        child_idxs.push(i);
                        return Ok((true, node_path, child_idxs));
                    }
                }
                // key is greater than all of this nodes keys, take last child, which has index
                // of node.keys.len()
                child_idxs.push(read_node.keys.len());
                read_node.keys.len()
            };

            // leaf node with no match
            if read_node.children.is_empty().await {
                return Ok((false, node_path, child_idxs));
            }

            read_node.get_child(child_index).await?
        };
        current_node = next_node;
    }
}

impl<M: CoreMem> Node<M> {
    fn new(keys: Vec<Key>, children: Vec<Child>, blocks: Shared<Blocks<M>>) -> Self {
        Node {
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

    async fn get_child(&self, index: usize) -> Result<Shared<Node<M>>, HyperbeeError> {
        self.children.get_child(index).await
    }
}

impl BlockEntry {
    fn new(entry: NodeSchema) -> Result<Self, HyperbeeError> {
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
        blocks: Shared<Blocks<M>>,
    ) -> Result<Node<M>, HyperbeeError> {
        let node_data = self.index.get(
            usize::try_from(offset)
                .map_err(|e| HyperbeeError::U64ToUsizeConversionError(offset, e))?,
        );
        Ok(Node::new(
            node_data.keys.clone(),
            node_data.children.clone(),
            blocks,
        ))
    }
}

impl<M: CoreMem> Hyperbee<M> {
    /// Trying to duplicate Js Hb.version
    pub async fn version(&self) -> u64 {
        self.blocks.read().await.info().await.length
    }
    /// Gets the root of the tree
    pub async fn get_root(&mut self) -> Result<Shared<Node<M>>, HyperbeeError> {
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

    /// Get the value corresponding to the provided `key` from the Hyperbee
    /// # Errors
    /// When `Hyperbee.get_root` fails
    pub async fn get(&mut self, key: &Vec<u8>) -> Result<Option<(u64, Vec<u8>)>, HyperbeeError> {
        let node = self.get_root().await?;
        let (matched, path, indexes) = nearest_node(node, key).await?;
        if matched {
            return path
                .last()
                .expect("Since `matched` was true, there must be at least one node in `path`")
                .read()
                .await
                .get_value_of_key(*indexes.last().expect(
                    "Since `matched` was true, there must be at least one node in `indexes`",
                ))
                .await;
        }
        Ok(None)
    }
}

/// helper for creating a Hyperbee
/// # Panics
/// when storage path is incorrect
/// when Hypercore failse to build
/// when Blocks fails to build
///
/// # Errors
/// when Hyperbee fails to build
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
