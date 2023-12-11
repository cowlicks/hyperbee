pub mod messages {
    include!(concat!(env!("OUT_DIR"), "/_.rs"));
}

use derive_builder::Builder;
use hypercore::{Hypercore, HypercoreError};
use messages::{Node, YoloIndex};
use prost::{bytes::Buf, DecodeError, Message};
use random_access_storage::RandomAccess;
use thiserror::Error;

use std::{fmt::Debug, sync::Arc};
use tokio::sync::Mutex;

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
struct BlockEntry<M: CoreMem> {
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
    /// Our reference to the Hypercore
    core: Arc<Mutex<Hypercore<M>>>,
}

/// A node in the tree
#[derive(Debug)]
struct TreeNode<M: CoreMem> {
    block: BlockEntry<M>,
    keys: Vec<Key>,
    children: Vec<Child>,
    //offset: u64,
    //changed: bool,
}

#[derive(Debug, Builder)]
#[builder(pattern = "owned", derive(Debug))]
pub struct Hyperbee<M: CoreMem> {
    core: Arc<Mutex<Hypercore<M>>>,
}

//
// NB: this is a smart wrapper around the proto_buf messages::yolo_index::Level;
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
    core: &Arc<Mutex<Hypercore<M>>>,
    seq: u64,
) -> Result<Option<BlockEntry<M>>, HyperbeeError> {
    match core.lock().await.get(seq).await? {
        Some(core_block) => {
            let node = Node::decode(&core_block[..])?;
            Ok(Some(BlockEntry::new(seq, node, core.clone())))
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
        let levels = YoloIndex::decode(buf)?
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
    fn new(block: BlockEntry<M>, keys: Vec<Key>, children: Vec<Child>, _offset: u64) -> Self {
        TreeNode {
            block,
            keys,
            children,
            //offset,
            //changed: false,
        }
    }
    async fn get_key(&self, index: usize) -> Result<Vec<u8>, HyperbeeError> {
        let key = &self.keys[index];
        if let Some(value) = &key.value {
            return Ok(value.clone());
        }
        if key.seq == self.block.seq {
            Ok(self.block.key.clone())
        } else {
            Ok(get_block(&self.block.core, key.seq)
                .await?
                .ok_or(HyperbeeError::NoKeyAtSeqError(key.seq))?
                .key)
        }
    }

    async fn get_key_value(&self, index: usize) -> Result<Option<(u64, Vec<u8>)>, HyperbeeError> {
        let seq = &self.keys[index].seq;
        return match get_block(&self.block.core, *seq).await? {
            Some(block) => Ok(block.value.map(|v| (block.seq, v))),
            None => Err(HyperbeeError::NoValueAtSeqError(*seq)),
        };
    }

    async fn get_child(&self, index: usize) -> Result<TreeNode<M>, HyperbeeError> {
        let child = &self.children[index];
        let child_block = get_block(&self.block.core, child.seq)
            .await?
            .ok_or(HyperbeeError::NoChildAtSeqError(child.seq))?;
        child_block.get_tree_node(child.offset)
    }
}

impl<M: CoreMem> BlockEntry<M> {
    fn new(seq: u64, entry: Node, core: Arc<Mutex<Hypercore<M>>>) -> Self {
        BlockEntry {
            seq,
            _index: Option::None,
            index_buffer: entry.index,
            key: entry.key,
            value: entry.value,
            core,
        }
    }

    fn is_target(&self, key: &[u8]) -> bool {
        key == self.key
    }

    /// offset is the offset of the node within the hypercore block
    fn get_tree_node(self, offset: u64) -> Result<TreeNode<M>, HyperbeeError> {
        let pointers = Pointers::new(&self.index_buffer[..])?;
        let node_data = pointers.get(offset as usize);
        Ok(TreeNode::new(
            self,
            node_data.keys.clone(),
            node_data.children.clone(),
            offset,
        ))
    }
}

// TODO use builder pattern macros for Hyperbee opts
impl<M: CoreMem> Hyperbee<M> {
    /// trying to duplicate Js Hb.versinon
    pub async fn version(&self) -> u64 {
        self.core.lock().await.info().length
    }
    /// Gets the root of the tree
    async fn get_root(&mut self, _ensure_header: bool) -> Result<TreeNode<M>, HyperbeeError> {
        let block = get_block(&self.core, self.version().await - 1)
            .await?
            .ok_or(HyperbeeError::NoRootError())?;
        block.get_tree_node(0)
    }

    /// Get the value associated with a key
    pub async fn get(&mut self, key: &Vec<u8>) -> Result<Option<(u64, Vec<u8>)>, HyperbeeError> {
        let mut node = self.get_root(false).await?;
        loop {
            // check if this is our guy
            if node.block.is_target(key) {
                return Ok(node.block.value.map(|v| (node.block.seq, v)));
            }

            // find the matching key, or next child
            // TODO do this with a binary search
            let child_index: usize = 'found: {
                for i in 0..node.keys.len() {
                    let val = node.get_key(i).await?;
                    // found matching child
                    if *key < val {
                        break 'found i;
                    }
                    // found matching key
                    if val == *key {
                        return node.get_key_value(i).await;
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
