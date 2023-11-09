pub mod messages {
    include!(concat!(env!("OUT_DIR"), "/_.rs"));
}

use hypercore::Hypercore;
use messages::{Node, YoloIndex};
use prost::{bytes::Buf, DecodeError, EncodeError, Message};

pub trait BytesLike: Into<Vec<u8>> + From<Vec<u8>> {}
impl<T: Into<Vec<u8>> + From<Vec<u8>>> BytesLike for T {}

pub trait CoreMem: random_access_storage::RandomAccess + std::fmt::Debug {}
impl<T: random_access_storage::RandomAccess + std::fmt::Debug> CoreMem for T {}

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

fn inflate<B: Buf, T: BytesLike>(buf: B) -> Result<Pointers<T>, DecodeError> {
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

struct TreeNode<T: BytesLike, M: CoreMem> {
    block: BlockEntry<T, M>,
    keys: Vec<Key<T>>,
    children: Vec<Child<T>>,
    offset: u64,
    changed: bool,
}

impl<T: BytesLike, M: CoreMem> TreeNode<T, M> {
    fn new(
        block: BlockEntry<T, M>,
        keys: Vec<Key<T>>,
        children: Vec<Child<T>>,
        offset: u64,
    ) -> Self {
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
}

struct BlockEntry<T: BytesLike, M: CoreMem> {
    seq: u64,
    tree: Batch<M>,
    index: Option<Pointers<T>>,
    index_buffer: T,
    key: T,
    value: Option<T>,
}

// What is an entry
impl<T: BytesLike, M: CoreMem> BlockEntry<T, M> {
    fn new(seq: u64, tree: Batch<M>, entry: Node) -> Self {
        BlockEntry {
            seq,
            tree,
            index: Option::None,
            index_buffer: entry.index.into(),
            key: entry.key.into(),
            value: entry.value.map(|x| x.into()),
        }
    }
}

// TODO this next
struct Batch<M: CoreMem> {
    tree: Hyperbee,
    core: Hypercore<M>,
    //this.index = tree._batches.push(this) - 1
    //this.blocks = cache ? new Map() : null
    //this.autoFlush = !batchLock
    //this.rootSeq = 0
    //this.root = null
    //this.length = 0
    //this.options = options
    //this.locked = null
    //this.batchLock = batchLock
    //this.onseq = this.options.onseq || noop
    //this.appending = null
    //this.isSnapshot = this.core !== this.tree.core
    //this.shouldUpdate = this.options.update !== false
    //this.updating = null
    //this.encoding = {
    //  key: options.keyEncoding ? codecs(options.keyEncoding) : tree.keyEncoding,
    //  value: options.valueEncoding ? codecs(options.valueEncoding) : tree.valueEncoding
    //}
}

impl<T: CoreMem> Batch<T> {
    fn new(
        tree: Hyperbee,
        core: Hypercore<T>,
        //batchLock: Option<()>, cache: ()
    ) -> Self {
        Batch { tree, core }
    }
}

struct Hyperbee {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn construct_key() {
        Key {
            seq: 0,
            value: "foo".to_string(),
        };
    }

    #[test]
    fn construct_child() {
        Child {
            seq: 0,
            offset: 0,
            value: "foo".to_string(),
        };
    }
    #[test]
    fn foo() {
        let buf = vec![10, 2, 1, 2, 18, 2, 3, 4];
        let level = messages::yolo_index::Level::decode(&buf[..]);
        dbg!(&level);
    }
}
