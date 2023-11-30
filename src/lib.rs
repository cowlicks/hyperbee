pub mod messages {
    include!(concat!(env!("OUT_DIR"), "/_.rs"));
}

use derive_builder::Builder;
use hypercore::{Hypercore, HypercoreError, Info};
use messages::{Node, YoloIndex};
use prost::{bytes::Buf, DecodeError, EncodeError, Message};

pub trait BytesLike: Into<Vec<u8>> + From<Vec<u8>> + Clone + PartialEq + Buf {}
impl<T: Into<Vec<u8>> + From<Vec<u8>> + Clone + PartialEq + Buf> BytesLike for T {}

pub trait CoreMem: random_access_storage::RandomAccess + std::fmt::Debug + Send {}
impl<T: random_access_storage::RandomAccess + std::fmt::Debug + Send> CoreMem for T {}

struct Key<T>
where
    T: BytesLike,
{
    seq: u64,
    value: Option<T>,
}

impl<T: BytesLike> Key<T> {
    fn new(seq: u64, value: Option<T>) -> Self {
        Key { seq, value }
    }
}

struct Child<T: BytesLike> {
    seq: u64,
    offset: u64,      // correct?
    value: Option<T>, // correct?
}
impl<T: BytesLike> Child<T> {
    fn new(seq: u64, offset: u64, value: Option<T>) -> Self {
        Child { seq, offset, value }
    }
}

// NB: this is a smart wrapper around the proto_buf messages::yolo_index::Level;
struct Level<T: BytesLike> {
    keys: Vec<Key<T>>,
    children: Vec<Child<T>>,
}

impl<T: BytesLike> Level<T> {
    fn new(keys: Vec<Key<T>>, children: Vec<Child<T>>) -> Self {
        Level { keys, children }
    }
}

struct Pointers<T: BytesLike> {
    levels: Vec<Level<T>>,
}

impl<T: BytesLike> Pointers<T> {
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

    fn get(&self, i: usize) -> &Level<T> {
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

fn inflate<T: BytesLike>(buf: T) -> Result<Pointers<T>, DecodeError> {
    return Pointers::new(buf);
}

fn deflate<T: BytesLike>(index: Vec<Level<T>>) -> Result<Vec<u8>, EncodeError> {
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

/// A node in the tree
pub struct TreeNode<T: BytesLike> {
    block: BlockEntry<T>,
    keys: Vec<Key<T>>,
    children: Vec<Child<T>>,
    offset: u64,
    changed: bool,
}

impl<T: BytesLike> TreeNode<T> {
    fn new(block: BlockEntry<T>, keys: Vec<Key<T>>, children: Vec<Child<T>>, offset: u64) -> Self {
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
        todo!()
    }
    async fn get_key(&self, _seq: u64) -> T {
        todo!()
    }

    async fn get_block(&self, _seq: u64) -> BlockEntry<T> {
        todo!()
    }
}

//pub struct BlockEntry<T: BytesLike, M: CoreMem> {
pub struct BlockEntry<T: BytesLike> {
    seq: u64,
    index: Option<Pointers<T>>,
    index_buffer: T,
    key: T,
    value: Option<T>,
}

impl<T: BytesLike> BlockEntry<T> {
    fn new(seq: u64, entry: Node) -> Self {
        BlockEntry {
            seq,
            index: Option::None,
            index_buffer: entry.index.into(),
            key: entry.key.into(),
            value: entry.value.map(|x| x.into()),
        }
    }

    fn is_target(&self, key: &T) -> bool {
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
    fn get_tree_node(&self, offset: u64) -> TreeNode<T> {
        let buf: Vec<u8> = self.index_buffer.clone().into();
        let index = Pointers::<T>::new(&buf[..]);
        todo!()
    }
}

// TODO use builder pattern macros for Hyperbee opts
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

impl<M: CoreMem> Hyperbee<M> {
    /// trying to duplicate Js Hb.versinon
    pub fn version(&self) -> u64 {
        self.core.info().length
    }
    /// Gets the root of the tree
    pub async fn get_root<T: BytesLike>(&mut self, _ensure_header: bool) -> TreeNode<T> {
        let block: BlockEntry<T> = self.get_block(self.version() - 1).await;
        block.get_tree_node(0)
    }
    pub async fn get_block<T: BytesLike>(&mut self, seq: u64) -> BlockEntry<T> {
        let x = self.core.get(seq).await.unwrap().unwrap();
        let node = Node::decode(&x[..]).unwrap();
        BlockEntry::new(seq, node)
    }

    pub async fn get<T: BytesLike>(&mut self, key: T) -> Result<Option<Vec<u8>>, HypercoreError> {
        //let node = await this.getRoot(false)
        let node = self.get_root::<T>(false).await;
        //while (true) {
        loop {
            //  if (node.block.isTarget(key)) {
            //    return node.block.isDeletion() ? null : node.block.final(encoding)
            //  }
            if node.block.is_target(&key) {
                return Ok(Some(node.block.index_buffer.clone().into()));
            }
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
