pub mod messages {
    include!(concat!(env!("OUT_DIR"), "/_.rs"));
}

use async_recursion::async_recursion;
use derive_builder::Builder;
use hypercore::{Hypercore, HypercoreError};
use messages::{Node, YoloIndex};
use prost::{bytes::Buf, DecodeError, EncodeError, Message};
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
    // TODO wrap a ref to the hyperbee or hypercore here
    // this is our reference to the hypercore.
    // we use it do things like hyperore.get(..)
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

//
// NB: this is a smart wrapper around the proto_buf messages::yolo_index::Level;
#[derive(Clone, Debug)]
pub struct Level {
    keys: Vec<Key>,
    children: Vec<Child>,
}

#[derive(Clone, Debug)]
pub struct Pointers {
    levels: Vec<Level>,
}

#[derive(Debug, Builder)]
#[builder(pattern = "owned", derive(Debug))]
pub struct Hyperbee<M: CoreMem> {
    core: Arc<Mutex<Hypercore<M>>>,
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

    pub fn has_key(&self, seq: u64) -> bool {
        for lvl in &self.levels {
            for k in &lvl.keys {
                if k.seq == seq {
                    return true;
                }
            }
        }
        false
    }
}

pub fn deflate(index: Vec<Level>) -> Result<Vec<u8>, EncodeError> {
    let levels = index
        .iter()
        .map(|level| {
            let keys: Vec<u64> = level.keys.iter().map(|k| k.seq).collect();
            let mut children = vec![];
            for i in 0..(level.children.len()) {
                children.push(level.children[i].seq);
                children.push(level.children[i].offset);
            }

            messages::yolo_index::Level { keys, children }
        })
        .collect();
    let mut buf = vec![];
    (YoloIndex { levels }).encode(&mut buf)?;
    Ok(buf)
}

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
        let key = self.keys[index].clone();
        if let Some(value) = key.value {
            return Ok(value);
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

    async fn get_child(&self, index: usize) -> Result<TreeNode<M>, HyperbeeError> {
        let child = self.children[index].clone();
        let child_block = get_block(&self.block.core, child.seq)
            .await?
            .ok_or(HyperbeeError::NoChildAtSeqError(child.seq))?;
        child_block.get_tree_node(child.offset)
    }

    #[async_recursion]
    pub async fn nearest_node(
        &self,
        key: Vec<u8>,
        mut path: Vec<isize>,
    ) -> Result<(bool, Vec<isize>), HyperbeeError> {
        for i in 0..self.keys.len() {
            let val = self.get_key(i).await?;
            if val >= key {
                path.push(i as isize);

                if val == key {
                    return Ok((true, path));
                }

                if self.children.is_empty() {
                    return Ok((false, path));
                }

                return self
                    .get_child(i + 1)
                    .await?
                    .nearest_node(key.clone(), path)
                    .await;
            }
        }
        path.push(-1);
        if self.children.is_empty() {
            return Ok((false, path));
        }
        self.get_child(0)
            .await?
            .nearest_node(key.clone(), path)
            .await
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
        let buf: Vec<u8> = self.index_buffer.clone();
        let pointers = Pointers::new(&buf[..])?;
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

    pub async fn get(&mut self, key: Vec<u8>) -> Result<Option<(u64, Vec<u8>)>, HyperbeeError> {
        let mut node = self.get_root(false).await?;
        loop {
            // check if this is our guy
            if node.block.is_target(&key) {
                return Ok(node.block.value.map(|v| (node.block.seq, v.clone())));
            }

            // find
            let mut ki: usize = node.keys.len();
            for i in 0..node.keys.len() {
                let val = node.get_key(i).await?;
                if key < val {
                    ki = i;
                    break;
                }
                if val == key {
                    let the_key = node.keys[i].clone();
                    return match get_block(&self.core, the_key.seq).await? {
                        Some(block) => Ok(block.value.map(|v| (block.seq, v.clone()))),
                        None => Ok(None),
                    };
                }
            }

            if node.children.is_empty() {
                return Ok(None);
            }

            node = node.get_child(ki).await?;
        }
    }
}
