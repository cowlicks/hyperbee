pub mod messages {
    include!(concat!(env!("OUT_DIR"), "/_.rs"));
}

use derive_builder::Builder;
use hypercore::{Hypercore, HypercoreError, Info};
use messages::{Node, YoloIndex};
use prost::{bytes::Buf, DecodeError, EncodeError, Message};

pub trait CoreMem: random_access_storage::RandomAccess + std::fmt::Debug + Send {}
impl<T: random_access_storage::RandomAccess + std::fmt::Debug + Send> CoreMem for T {}

#[derive(Clone, Debug)]
struct Key {
    seq: u64,
    value: Option<Vec<u8>>,
}

#[derive(Clone, Debug)]
struct Child {
    seq: u64,
    offset: u64,            // correct?
    value: Option<Vec<u8>>, // correct?
}

#[derive(Clone, Debug)]
pub struct BlockEntry {
    /// index in the hypercore
    seq: u64,
    index: Option<Pointers>,
    index_buffer: Vec<u8>,
    key: Vec<u8>,
    value: Option<Vec<u8>>,
}

/// A node in the tree
#[derive(Debug)]
pub struct TreeNode {
    block: BlockEntry,
    keys: Vec<Key>,
    children: Vec<Child>,
    offset: u64,
    changed: bool,
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

#[derive(Debug, Builder)]
#[builder(pattern = "owned", derive(Debug))]
pub struct Hyperbee<M: CoreMem> {
    //this.core = core
    pub core: Hypercore<M>,
    //this.keyEncoding = opts.keyEncoding ? codecs(opts.keyEncoding) : null
    // TODO make enum
    //key_encoding: String,

    //this.valueEncoding = opts.valueEncoding ? codecs(opts.valueEncoding) : null
    // TODO make enum
    //value_encoding: String,

    //this.extension = opts.extension !== false ? opts.extension || Extension.register(this) : null
    //this.metadata = opts.metadata || null
    //this.lock = opts.lock || mutexify()

    //this.sep = opts.sep || SEP
    // TODO set default
    //sep: String,

    //this.readonly = !!opts.readonly
    // TODO set default false
    //readonly: bool,

    //this.prefix = opts.prefix || null
    // TODO set default
    //prefix: String,
    //this._unprefixedKeyEncoding = this.keyEncoding
    //this._sub = !!this.prefix
    //this._checkout = opts.checkout || 0
    //this._view = !!opts._view

    //this._onappendBound = this._view ? null : this._onappend.bind(this)
    //this._ontruncateBound = this._view ? null : this._ontruncate.bind(this)
    //this._watchers = this._onappendBound ? [] : null
    //this._entryWatchers = this._onappendBound ? [] : null
    //this._sessions = opts.sessions !== false

    //this._batches = []

    //if (this._watchers) {
    //  this.core.on('append', this._onappendBound)
    //  this.core.on('truncate', this._ontruncateBound)
    //}

    //if (this.prefix && opts._sub) {
    //  this.keyEncoding = prefixEncoding(this.prefix, this.keyEncoding)
    //}
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

    fn has_key(&self, seq: u64) -> bool {
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

fn deflate(index: Vec<Level>) -> Result<Vec<u8>, EncodeError> {
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

impl TreeNode {
    fn new(block: BlockEntry, keys: Vec<Key>, children: Vec<Child>, offset: u64) -> Self {
        let out = TreeNode {
            block,
            offset,
            keys,
            children,
            changed: false,
        };
        out.preload();
        out
    }
    fn preload(&self) {
        //todo!()
    }
    async fn get_key(&self, index: usize) -> Vec<u8> {
        /*
                 async getKey (index) {
          const key = this.keys[index]
          if (key.value) return key.value
          const k = (key.seq === this.block.seq) ?
          this.block.key :
          await this.block.tree.getKey(key.seq);

          return (key.value = k)
        }
              */
        let key = self.keys[index].clone();
        if let Some(value) = key.value {
            return value;
        }
        todo!()
    }

    async fn get_block(&self, _seq: u64) -> BlockEntry {
        todo!()
    }
}

impl BlockEntry {
    fn new(seq: u64, entry: Node) -> Self {
        BlockEntry {
            seq,
            index: Option::None,
            index_buffer: entry.index.into(),
            key: entry.key.into(),
            value: entry.value.map(|x| x.into()),
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
    fn get_tree_node(&self, offset: u64) -> TreeNode {
        let buf: Vec<u8> = self.index_buffer.clone().into();
        let index = Pointers::new(&buf[..]).unwrap();
        let entry = index.get(offset as usize);
        TreeNode::new(
            self.clone(),
            entry.keys.clone(),
            entry.children.clone(),
            offset,
        )
    }
}

// TODO use builder pattern macros for Hyperbee opts

impl<M: CoreMem> Hyperbee<M> {
    /// trying to duplicate Js Hb.versinon
    pub fn version(&self) -> u64 {
        self.core.info().length
    }
    /// Gets the root of the tree
    pub async fn get_root(&mut self, _ensure_header: bool) -> TreeNode {
        let block: BlockEntry = self.get_block(self.version() - 1).await;
        block.get_tree_node(0)
    }
    pub async fn get_block(&mut self, seq: u64) -> BlockEntry {
        let x = self.core.get(seq).await.unwrap().unwrap();
        let node = Node::decode(&x[..]).unwrap();
        BlockEntry::new(seq, node)
    }

    pub async fn get(&mut self, key: Vec<u8>) -> Result<Option<Vec<u8>>, HypercoreError> {
        let node = self.get_root(false).await;
        dbg!(&node.keys);
        dbg!(&node.children);
        loop {
            // check if this is our guy
            if node.block.is_target(&key) {
                return Ok(Some(node.block.index_buffer.clone().into()));
            }

            // find
            let mut ki: isize = -1;
            for i in 0..node.keys.len() {
                let val = node.get_key(i).await;
                if val > key {
                    ki = i as isize;
                    break;
                }
            }
            dbg!(ki);
            /*
            let s = 0
            let e = node.keys.length
            let c

            while (s < e) {
              const mid = (s + e) >> 1

              c = b4a.compare(key, await node.getKey(mid))

              /// found it
              if (c === 0) return (await this.getBlock(node.keys[mid].seq)).final(encoding)

              if (c < 0) e = mid
              else s = mid + 1
            }

            if (!node.children.length) return null

            const i = c < 0 ? e : s
            node = await node.getChildNode(i)
            */

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
