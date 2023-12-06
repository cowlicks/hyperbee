pub mod messages {
    include!(concat!(env!("OUT_DIR"), "/_.rs"));
}

use async_recursion::async_recursion;
use derive_builder::Builder;
use hypercore::{Hypercore, HypercoreError};
use messages::{Node, YoloIndex};
use prost::{bytes::Buf, DecodeError, EncodeError, Message};

use std::sync::Arc;
use tokio::sync::Mutex;

pub trait CoreMem: random_access_storage::RandomAccess + std::fmt::Debug + Send {}
impl<T: random_access_storage::RandomAccess + std::fmt::Debug + Send> CoreMem for T {}

#[derive(Clone, Debug)]
pub struct Key {
    seq: u64,
    value: Option<Vec<u8>>,
}

#[derive(Clone, Debug)]
pub struct Child {
    seq: u64,
    offset: u64,            // correct?
    value: Option<Vec<u8>>, // correct?
}

#[derive(Clone, Debug)]
pub struct BlockEntry<M: CoreMem> {
    /// index in the hypercore
    seq: u64,
    /// Pointers::new(Node::new(hypercore.get(seq)).index))
    index: Option<Pointers>,
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
pub struct TreeNode<M: CoreMem> {
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
    //this.core = core
    pub core: Arc<Mutex<Hypercore<M>>>,
}
impl Key {
    fn new(seq: u64, value: Option<Vec<u8>>) -> Self {
        Key { seq, value }
    }
}

impl Child {
    fn new(seq: u64, offset: u64, value: Option<Vec<u8>>) -> Self {
        Child { seq, offset, value }
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
                    .map(|k| Key::new(k.clone(), Option::None))
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
        return &self.levels[i];
    }

    pub fn has_key(&self, seq: u64) -> bool {
        for lvl in &self.levels {
            for k in &lvl.keys {
                if k.seq == seq {
                    return true;
                }
            }
        }
        return false;
    }
}

pub fn deflate(index: Vec<Level>) -> Result<Vec<u8>, EncodeError> {
    let levels = index
        .iter()
        .map(|level| {
            let keys: Vec<u64> = level.keys.iter().map(|k| k.seq.clone()).collect();
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
    return Ok(buf);
}

impl<M: CoreMem> TreeNode<M> {
    fn new(block: BlockEntry<M>, keys: Vec<Key>, children: Vec<Child>, offset: u64) -> Self {
        let out = TreeNode {
            block,
            keys,
            children,
            //offset,
            //changed: false,
        };
        out
    }
    async fn get_key(&self, index: usize) -> Vec<u8> {
        let key = self.keys[index].clone();
        if let Some(value) = key.value {
            dbg!("get_key has value");
            return value;
        }
        let value = if key.seq == self.block.seq {
            dbg!("get_key key.seq == block.seq");
            //self.block.key.clone()
            todo!()
        } else {
            self._get_key(key.seq).await
        };
        return value;
    }

    // TODO dedupe with hb.get_block
    async fn get_block(&self, seq: u64) -> BlockEntry<M> {
        let mut core = self.block.core.lock().await;
        let b = core.get(seq).await.unwrap().unwrap();
        let node = Node::decode(&b[..]).unwrap();
        let b = BlockEntry::new(seq, node, self.block.core.clone());
        b
    }

    async fn _get_key(&self, seq: u64) -> Vec<u8> {
        let block = self.get_block(seq).await;
        block.key
    }

    /*
    async getChildNode (index) {
      const child = this.children[index]
      if (child.value) return child.value
      const block = child.seq === this.block.seq ? this.block : await this.block.tree.getBlock(child.seq)
      return (child.value = block.getTreeNode(child.offset))
    }
    */
    pub async fn get_child(&self, index: usize) -> TreeNode<M> {
        let child = self.children[index].clone();
        let child_block = self.get_block(child.seq).await;
        return child_block.get_tree_node(child.offset);
    }

    pub async fn nearest_node(&mut self, key: Vec<u8>) -> Result<TreeNode<M>, HypercoreError> {
        todo!()
    }
}

impl<M: CoreMem> BlockEntry<M> {
    fn new(seq: u64, entry: Node, core: Arc<Mutex<Hypercore<M>>>) -> Self {
        BlockEntry {
            seq,
            index: Option::None,
            index_buffer: entry.index.into(),
            key: entry.key.into(),
            value: entry.value.map(|x| x.into()),
            core,
        }
    }

    fn is_target(&self, key: &[u8]) -> bool {
        key == &self.key
    }

    /*
     getTreeNode (offset) {
        if (this.index === null) {
          this.index = inflate(this.indexBuffer)
          this.indexBuffer = null
        }
        const entry = this.index.get(offset)
        return new TreeNode(this, entry.keys, entry.children, offset)
     }
    */

    /// offset is the offset of the node within the hypercore block
    fn get_tree_node(self, offset: u64) -> TreeNode<M> {
        let buf: Vec<u8> = self.index_buffer.clone().into();
        let pointers = Pointers::new(&buf[..]).unwrap();
        let node_data = pointers.get(offset as usize);
        TreeNode::new(
            self,
            node_data.keys.clone(),
            node_data.children.clone(),
            offset,
        )
    }
}

// TODO use builder pattern macros for Hyperbee opts

impl<M: CoreMem> Hyperbee<M> {
    /// trying to duplicate Js Hb.versinon
    pub async fn version(&self) -> u64 {
        self.core.lock().await.info().length
    }
    /// Gets the root of the tree
    pub async fn get_root(&mut self, _ensure_header: bool) -> TreeNode<M> {
        let block: BlockEntry<M> = self.get_block(self.version().await - 1).await;
        block.get_tree_node(0)
    }

    pub async fn get_block(&mut self, seq: u64) -> BlockEntry<M> {
        let x = self.core.lock().await.get(seq).await.unwrap().unwrap();
        let node = Node::decode(&x[..]).unwrap();
        BlockEntry::new(seq, node, self.core.clone())
    }

    pub async fn get(&mut self, key: Vec<u8>) -> Result<Option<Vec<u8>>, HypercoreError> {
        let node = self.get_root(false).await;
        dbg!(&node.children.len());
        dbg!(&node.keys.len());
        loop {
            // check if this is our guy
            if node.block.is_target(&key) {
                return Ok(Some(node.block.index_buffer.clone().into()));
            }

            // find
            let mut ki: isize = -1;
            for i in 0..node.keys.len() {
                let val = node.get_key(i).await;
                dbg!(&val.len());
                if val > key {
                    ki = i as isize;
                    break;
                }
                if val == key {
                    return Ok(self.get_block(node.keys[ki as usize].seq).await.value);
                }
            }

            // get Child
            if ki == 0 {
                return Ok(self.get_block(node.keys[ki as usize].seq).await.value);
            }
            dbg!(ki);
            todo!()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn foo() {
        let buf = vec![10, 2, 1, 2, 18, 2, 3, 4];
        let level = messages::yolo_index::Level::decode(&buf[..]);
        dbg!(&level);
    }

    #[test]
    fn vec_order() {
        let a: Vec<u8> = vec![1];
        let b: Vec<u8> = vec![1, 2];
        let c: Vec<u8> = vec![1, 3];
        let d: Vec<u8> = vec![5];
        assert!(a < b);
        assert!(a < c);
        assert!(c < d);
    }
    #[test]
    fn looper() {
        let mut o = 1;
        loop {
            let x = o + 2;
            dbg!(&x);
            dbg!(&o);
            o = o + 3;
            if o > 55 {
                break;
            }
        }
    }
}
