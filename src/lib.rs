//! # ⚠️  WARNING 🚧 API unstable ⚒️  and still in development 👷
//! Rust version of [hyperbee](https://github.com/holepunchto/hyperbee)
//! A B-tree built on top of Hypercore.

mod messages {
    include!(concat!(env!("OUT_DIR"), "/_.rs"));
}
mod blocks;
mod changes;
mod del;
mod error;
mod keys;
pub mod prefixed;
mod put;
mod test;
pub mod traverse;
mod tree;

use std::{
    fmt::Debug,
    ops::{Range, RangeBounds},
    path::Path,
    sync::Arc,
};

use derive_builder::Builder;
use hypercore::AppendOutcome;
use prefixed::{Prefixed, PrefixedConfig};
use prost::{bytes::Buf, DecodeError, Message};
use random_access_storage::RandomAccess;
use tokio::sync::RwLock;
use tracing::trace;

use blocks::Blocks;
use error::HyperbeeError;
use messages::{header::Metadata, yolo_index, YoloIndex};
use traverse::{Traverse, TraverseConfig};
use tree::Tree;

pub trait CoreMem: RandomAccess + Debug + Send {}
impl<T: RandomAccess + Debug + Send> CoreMem for T {}

/// Same value as JS hyperbee https://github.com/holepunchto/hyperbee/blob/e1b398f5afef707b73e62f575f2b166bcef1fa34/index.js#L663
static PROTOCOL: &str = "hyperbee";
/// Same value as JS hyperbee https://github.com/holepunchto/hyperbee/blob/e1b398f5afef707b73e62f575f2b166bcef1fa34/index.js#L16-L18
static MAX_KEYS: usize = 8;

fn min_keys(max_keys: usize) -> usize {
    max_keys >> 1
}

// TODO make not pub
#[derive(Clone, Debug)]
/// Reference used within a [`Node`] of the [Hypercore](hypercore::Hypercore) block where a
/// key-value  pair is stored.
pub struct KeyValue {
    /// Index of key value pair within the [`hypercore::Hypercore`].
    seq: u64,
}

#[derive(Clone, Debug)]
pub struct KeyValueData {
    pub seq: u64,
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
}

// TODO make not pub
#[derive(Debug)]
/// Pointer used within a [`Node`] to reference to it's child nodes.
pub struct Child<M: CoreMem> {
    /// Index of the [`BlockEntry`]within the [`hypercore::Hypercore`] that contains the [`Node`]
    pub seq: u64,
    /// Index of the `Node` within the [`BlockEntry`] referenced by [`Child::seq`]
    pub offset: u64,
    /// Cache of the child node
    cached_node: Option<SharedNode<M>>,
}

//TODO make not pub
#[derive(Clone, Debug)]
/// A "block" from a [`Hypercore`](hypercore::Hypercore) deserialized into the form used in
/// Hyperbee
pub struct BlockEntry<M: CoreMem> {
    /// Pointers::new(NodeSchema::new(hypercore.get(seq)).index))
    nodes: Vec<SharedNode<M>>,
    /// NodeSchema::new(hypercore.get(seq)).key
    key: Vec<u8>,
    /// NodeSchema::new(hypercore.get(seq)).value
    value: Option<Vec<u8>>,
}

type Shared<T> = Arc<RwLock<T>>;
type SharedNode<T> = Shared<Node<T>>;
type NodePath<T> = Vec<(SharedNode<T>, usize)>;

#[derive(Debug)]
struct Children<M: CoreMem> {
    blocks: Shared<Blocks<M>>,
    children: RwLock<Vec<Child<M>>>,
}

// TODO make not pub
/// A node of the B-Tree within the [`Hyperbee`]
#[derive(Debug)]
pub struct Node<M: CoreMem> {
    pub keys: Vec<KeyValue>,
    children: Children<M>,
    blocks: Shared<Blocks<M>>,
}

/// An append only B-Tree built on [`Hypercore`](hypercore::Hypercore). It provides a key-value
/// store API, with methods for [inserting](Hyperbee::put), [getting](Hyperbee::get), and
/// [deleting](Hyperbee::del) key-value pair. As well as creating [sorted
/// iterators](Hyperbee::traverse), and ["sub" B-Trees](Hyperbee::sub) for grouping related data.
#[derive(Debug, Builder)]
#[builder(pattern = "owned", derive(Debug))]
pub struct Hyperbee<M: CoreMem> {
    tree: Shared<Tree<M>>,
}

impl<M: CoreMem> Hyperbee<M> {
    /// The number of blocks in the hypercore.
    /// The first block is always the header block so:
    /// `version` would be the `seq` of the next block
    /// `version - 1` is most recent block
    pub async fn version(&self) -> u64 {
        self.tree.read().await.version().await
    }
    /// Gets the root of the tree.
    /// When `ensure_header == true` write the hyperbee header onto the hypercore if it does not exist.
    pub async fn get_root(
        &self,
        ensure_header: bool,
    ) -> Result<Option<Shared<Node<M>>>, HyperbeeError> {
        self.tree.read().await.get_root(ensure_header).await
    }

    /// Create the header for the Hyperbee. This must be done before writing anything else to the
    /// tree.
    pub async fn create_header(
        &self,
        metadata: Option<Metadata>,
    ) -> Result<AppendOutcome, HyperbeeError> {
        self.tree.read().await.create_header(metadata).await
    }
    /// Returs a string representing the structure of the tree showing the keys in each node
    pub async fn print(&self) -> Result<String, HyperbeeError> {
        self.tree.read().await.print().await
    }

    /// Get the value corresponding to the provided `key` from the Hyperbee
    /// # Errors
    /// When `Hyperbee.get_root` fails
    pub async fn get(&self, key: &[u8]) -> Result<Option<(u64, Option<Vec<u8>>)>, HyperbeeError> {
        self.tree.read().await.get(key).await
    }

    /// Insert the given key and value into the tree
    /// Returs the `seq` of the new key, and `Option<u64>` which contains the `seq` of the old key
    /// if it was replaced.
    #[tracing::instrument(level = "trace", skip(self), ret)]
    pub async fn put(
        &self,
        key: &[u8],
        value: Option<&[u8]>,
    ) -> Result<(Option<u64>, u64), HyperbeeError> {
        self.tree.read().await.put(key, value).await
    }

    /// Like [`Hyperbee::put`] but takes a `compare_and_swap` function.
    /// The `compared_and_swap` function is called with the old key (if present), and the new key.
    /// The new key is only inserted if `compare_and_swap` returns true.
    /// Returs two `Option<u64>`s. The first is the old key, the second is the new key.
    pub async fn put_compare_and_swap(
        &self,
        key: &[u8],
        value: Option<&[u8]>,
        cas: impl FnOnce(Option<&KeyValueData>, &KeyValueData) -> bool,
    ) -> Result<(Option<u64>, Option<u64>), HyperbeeError> {
        self.tree
            .read()
            .await
            .put_compare_and_swap(key, value, cas)
            .await
    }

    /// Delete the given key from the tree.
    /// Returns the `seq` from the key if it was deleted.
    pub async fn del(&self, key: &[u8]) -> Result<Option<u64>, HyperbeeError> {
        self.tree.read().await.del(key).await
    }

    /// Like [`Hyperbee::del`] but takes a `compare_and_swap` function.
    /// Before deleting the function is called with existing key's [`KeyValueData`].
    /// The key is only deleted if `compare_and_swap` returs true.
    /// Returns the `bool` representing the result of `compare_and_swap`, and the `seq` for the
    /// key.
    pub async fn del_compare_and_swap(
        &self,
        key: &[u8],
        cas: impl FnOnce(&KeyValueData) -> bool,
    ) -> Result<Option<(bool, u64)>, HyperbeeError> {
        self.tree.read().await.del_compare_and_swap(key, cas).await
    }

    /// Create a new tree with all it's operation's prefixed by the provided `prefix`.
    pub fn sub(&self, prefix: &[u8], config: PrefixedConfig) -> Prefixed<M> {
        Prefixed::new(prefix, self.tree.clone(), config)
    }

    /// Traverse the tree based on the given [`TraverseConfig`]
    pub async fn traverse<'a>(
        &self,
        conf: TraverseConfig,
    ) -> Result<Traverse<'a, M>, HyperbeeError> {
        self.tree.read().await.traverse(conf).await
    }
}

impl Hyperbee<random_access_disk::RandomAccessDisk> {
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
    ) -> Result<Hyperbee<random_access_disk::RandomAccessDisk>, HyperbeeError> {
        let tree = tree::Tree::from_storage_dir(path_to_storage_dir).await?;
        Ok(HyperbeeBuilder::default()
            .tree(Arc::new(RwLock::new(tree)))
            .build()?)
    }
}

impl Hyperbee<random_access_memory::RandomAccessMemory> {
    /// Helper for creating a Hyperbee in RAM
    pub async fn from_ram(
    ) -> Result<Hyperbee<random_access_memory::RandomAccessMemory>, HyperbeeError> {
        let tree = tree::Tree::from_ram().await?;
        Ok(HyperbeeBuilder::default()
            .tree(Arc::new(RwLock::new(tree)))
            .build()?)
    }
}

impl KeyValue {
    fn new(seq: u64) -> Self {
        KeyValue { seq }
    }
}

impl<M: CoreMem> Child<M> {
    fn new(seq: u64, offset: u64, node: Option<SharedNode<M>>) -> Self {
        Child {
            seq,
            offset,
            cached_node: node,
        }
    }
}

impl<M: CoreMem> Clone for Child<M> {
    fn clone(&self) -> Self {
        Self::new(self.seq, self.offset, self.cached_node.clone())
    }
}

/// Deserialize bytes from a Hypercore block into [`Node`]s.
fn make_node_vec<B: Buf, M: CoreMem>(
    buf: B,
    blocks: Shared<Blocks<M>>,
) -> Result<Vec<SharedNode<M>>, DecodeError> {
    Ok(YoloIndex::decode(buf)?
        .levels
        .iter()
        .map(|level| {
            let keys = level.keys.iter().map(|k| KeyValue::new(*k)).collect();
            let mut children = vec![];
            for i in (0..(level.children.len())).step_by(2) {
                children.push(Child::new(
                    level.children[i],
                    level.children[i + 1],
                    Option::None,
                ));
            }
            Arc::new(RwLock::new(Node::new(keys, children, blocks.clone())))
        })
        .collect())
}

impl<M: CoreMem> Children<M> {
    fn new(blocks: Shared<Blocks<M>>, children: Vec<Child<M>>) -> Self {
        Self {
            blocks,
            children: RwLock::new(children),
        }
    }

    #[tracing::instrument(skip(self))]
    async fn insert(&self, index: usize, new_children: Vec<Child<M>>) {
        if new_children.is_empty() {
            trace!("no children to insert, do nothing");
            return;
        }

        let replace_split_child = match new_children.is_empty() {
            true => 0,
            false => 1,
        };
        trace!(
            "replacing child @ [{}] with [{}] children.",
            index,
            new_children.len()
        );
        self.children
            .write()
            .await
            .splice(index..(index + replace_split_child), new_children);
    }

    #[tracing::instrument(skip(self))]
    async fn get_child(&self, index: usize) -> Result<Shared<Node<M>>, HyperbeeError> {
        let (seq, offset) = {
            let child_ref = &self.children.read().await[index];
            if let Some(node) = &child_ref.cached_node {
                return Ok(node.clone());
            }
            (child_ref.seq, child_ref.offset)
        };
        let block = self
            .blocks
            .read()
            .await
            .get(&seq, self.blocks.clone())
            .await?;
        let node = block.read().await.get_tree_node(offset)?;
        self.children.write().await[index].cached_node = Some(node.clone());
        Ok(node)
    }

    async fn len(&self) -> usize {
        self.children.read().await.len()
    }

    async fn splice<R: RangeBounds<usize>, I: IntoIterator<Item = Child<M>>>(
        &self,
        range: R,
        replace_with: I,
    ) -> Vec<Child<M>> {
        // Leaf node do nothing. Should we Err instead?
        if self.children.read().await.is_empty() {
            return vec![];
        }
        self.children
            .write()
            .await
            .splice(range, replace_with)
            .collect()
    }
}

#[tracing::instrument(skip(node))]
/// Find the `key` in the `node` with a binary search
///
/// # Returns (`matched`, `index`)
///
/// `matched` is Some means we found the `key`.
///
/// if `matched` is None:
///     if `node` is not a leaf:
///         index of the child within the `node` where the `key` could be
///     if `node` is a leaf:
///         the index within this `node`'s keys where the `key` wolud be inserted
/// if `matched` is Some:
///     the index within this `node`'s keys of the `key`
async fn get_index_of_key<M: CoreMem, T>(
    node: SharedNode<M>,
    key: &T,
) -> Result<(Option<u64>, usize), HyperbeeError>
where
    T: PartialOrd<[u8]> + Debug + ?Sized,
{
    let child_index: usize = 'found: {
        // Binary search current node for matching key, or index of next child
        let n_keys = node.read().await.keys.len();
        if n_keys == 0 {
            break 'found n_keys;
        }
        let mut low = 0;
        let mut high = n_keys - 1;

        while low <= high {
            let mid = low + ((high - low) >> 1);
            let KeyValueData {
                seq,
                key: other_key,
                ..
            } = node.read().await.get_key_value(mid).await?;

            // if matching key, we are done!
            if key == &other_key[..] {
                trace!(
                    "key {:?} == other_key {:?} at index {}",
                    key,
                    other_key,
                    mid
                );
                return Ok((Some(seq), mid));
            }

            if key < &other_key[..] {
                if mid == 0 {
                    break;
                }
                // look lower
                high = mid - 1;
            } else {
                // look higher
                low = mid + 1;
            }
        }
        break 'found low;
    };
    Ok((None, child_index))
}

/// Descend through tree to the node nearest (or matching) the provided key
/// Return value describes the path to the key. It looks like:
/// `(matched, path: Vec<(node, index)>)`
///
/// Here `matched` is a bool that indicates if the key was matched.
/// The `path` is a `Vec` that describes the path to the key. Each item is a tuple `(node, inde)`.
/// `path[0]` is the root of tree, and the last element would be final node,
/// which is always a leaf if `matched == false`.
/// In the `path` the `node` is a referenece to the node we passed through.
/// The `index` is the child index to the next node in the path.
/// In a leaf node, the `index` could be thought of as the gap between the node's keys where the provided
/// `key` would be ineserted. Or for `matched = true` the index of the matched key in the nodes's
/// keys.
#[tracing::instrument(skip(node))]
async fn nearest_node<M: CoreMem, T>(
    node: SharedNode<M>,
    key: &T,
) -> Result<(Option<u64>, NodePath<M>), HyperbeeError>
where
    T: PartialOrd<[u8]> + Debug + ?Sized,
{
    let mut current_node = node;
    let mut out_path: NodePath<M> = vec![];
    loop {
        let next_node = {
            let (matched, child_index) = get_index_of_key(current_node.clone(), key).await?;
            out_path.push((current_node.clone(), child_index));

            // found match or reached leaf
            if matched.is_some() || current_node.read().await.is_leaf().await {
                return Ok((matched, out_path));
            }

            // continue to next node
            current_node.read().await.get_child(child_index).await?
        };
        current_node = next_node;
    }
}

impl<M: CoreMem> Node<M> {
    fn new(keys: Vec<KeyValue>, children: Vec<Child<M>>, blocks: Shared<Blocks<M>>) -> Self {
        Node {
            keys,
            children: Children::new(blocks.clone(), children),
            blocks,
        }
    }

    pub async fn n_children(&self) -> usize {
        self.children.len().await
    }

    async fn is_leaf(&self) -> bool {
        self.n_children().await == 0
    }

    /// The number of children between this node and a leaf + 1
    pub async fn height(&self) -> Result<usize, HyperbeeError> {
        if self.is_leaf().await {
            Ok(1)
        } else {
            let mut out = 1;
            let mut cur_child = self.get_child(0).await?;
            loop {
                out += 1;
                if cur_child.read().await.n_children().await == 0 {
                    return Ok(out);
                }
                let next_child = cur_child.read().await.get_child(0).await?;
                cur_child = next_child;
            }
        }
    }

    /// Serialize this node
    async fn to_level(&self) -> yolo_index::Level {
        let mut children = vec![];
        for c in self.children.children.read().await.iter() {
            children.push(c.seq);
            children.push(c.offset);
        }
        yolo_index::Level {
            keys: self.keys.iter().map(|k| k.seq).collect(),
            children,
        }
    }

    #[tracing::instrument(skip(self))]
    async fn get_key_value(&self, index: usize) -> Result<KeyValueData, HyperbeeError> {
        let KeyValue { seq, .. } = self.keys[index];
        let key = self
            .blocks
            .read()
            .await
            .get(&seq, self.blocks.clone())
            .await?
            .read()
            .await
            .key
            .clone();
        let value = self
            .blocks
            .read()
            .await
            .get(&seq, self.blocks.clone())
            .await?
            .read()
            .await
            .value
            .clone();
        Ok(KeyValueData { seq, key, value })
    }

    /// Get the child at the provided index
    async fn get_child(&self, index: usize) -> Result<Shared<Node<M>>, HyperbeeError> {
        self.children.get_child(index).await
    }

    /// Insert a key and it's children into [`self`].
    #[tracing::instrument(skip(self))]
    async fn insert(&mut self, key_ref: KeyValue, children: Vec<Child<M>>, range: Range<usize>) {
        trace!("inserting [{}] children", children.len());
        self.keys.splice(range.clone(), vec![key_ref]);
        self.children.insert(range.start, children).await;
    }
}

impl<M: CoreMem> BlockEntry<M> {
    fn new(entry: messages::Node, blocks: Shared<Blocks<M>>) -> Result<Self, HyperbeeError> {
        Ok(BlockEntry {
            nodes: make_node_vec(&entry.index[..], blocks)?,
            key: entry.key,
            value: entry.value,
        })
    }

    /// Get a [`Node`] from this [`BlockEntry`] at the provided `offset`.
    /// offset is the offset of the node within the hypercore block
    fn get_tree_node(&self, offset: u64) -> Result<SharedNode<M>, HyperbeeError> {
        Ok(self
            .nodes
            .get(
                usize::try_from(offset)
                    .map_err(|e| HyperbeeError::U64ToUsizeConversionError(offset, e))?,
            )
            .expect("offset *should* always point to a real node")
            .clone())
    }
}
