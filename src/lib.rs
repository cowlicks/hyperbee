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
mod prefixed;
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
use prost::{bytes::Buf, DecodeError, Message};
use random_access_storage::RandomAccess;
use tokio::sync::RwLock;
use tracing::trace;

use blocks::Blocks;
use error::HyperbeeError;
use messages::{header::Metadata, yolo_index, YoloIndex};
use traverse::{Traverse, TraverseConfig};
use tree::Tree;

pub use prefixed::Prefixed;

pub trait CoreMem: RandomAccess + Debug + Send {}
impl<T: RandomAccess + Debug + Send> CoreMem for T {}

/// Same value as JS hyperbee https://github.com/holepunchto/hyperbee/blob/e1b398f5afef707b73e62f575f2b166bcef1fa34/index.js#L663
static PROTOCOL: &str = "hyperbee";
/// Same value as JS hyperbee https://github.com/holepunchto/hyperbee/blob/e1b398f5afef707b73e62f575f2b166bcef1fa34/index.js#L16-L18
static MAX_KEYS: usize = 8;

fn min_keys(max_keys: usize) -> usize {
    max_keys >> 1
}

#[derive(Clone, Debug)]
/// Pointer used within a [`Node`] to point to the block where a (key, value) pair is stored.
/// A key can be inserted without a value, so it's value is optional.
pub struct KeyValue {
    /// Index of key value pair within the [`hypercore::Hypercore`].
    seq: u64,
    /// Key of the key value pair
    cached_key: Option<Vec<u8>>,
    /// Value of the key value pair.
    cached_value: Option<Option<Vec<u8>>>,
}
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

#[derive(Clone, Debug)]
/// A block off the hypercore deserialized into the form we use in the BTree
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

/// A node in the tree
#[derive(Debug)]
pub struct Node<M: CoreMem> {
    pub keys: Vec<KeyValue>,
    children: Children<M>,
    blocks: Shared<Blocks<M>>,
}

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
    #[tracing::instrument(level = "trace", skip(self), ret)]
    pub async fn put(
        &self,
        key: &[u8],
        value: Option<&[u8]>,
    ) -> Result<(bool, u64), HyperbeeError> {
        self.tree.read().await.put(key, value).await
    }

    /// Delete the given key from the tree
    pub async fn del(&self, key: &[u8]) -> Result<bool, HyperbeeError> {
        self.tree.read().await.del(key).await
    }

    pub fn sub(&self, prefix: &[u8]) -> Prefixed<M> {
        Prefixed::new(prefix, self.tree.clone())
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
    fn new(seq: u64, keys_key: Option<Vec<u8>>, keys_value: Option<Option<Vec<u8>>>) -> Self {
        KeyValue {
            seq,
            cached_key: keys_key,
            cached_value: keys_value,
        }
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
            let keys = level
                .keys
                .iter()
                .map(|k| KeyValue::new(*k, Option::None, Option::None))
                .collect();
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
/// `match` == true means we found the `key`.
///
/// if `matched` false:
///     if `node` is not a leaf:
///         index of the child within the `node` where the `key` could be
///     if `node` is a leaf:
///         the index within this `node`'s keys where the `key` wolud be inserted
/// if `matched` is true:
///     the index within this `node`'s keys of the `key`
// TODO rename me because it is not just child index
// TODO return Result<(Option<KeyValue>, usize), HyperbeeError>
// TODO rewrite doc
async fn get_child_index<M: CoreMem, T>(
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
            let (seq, other_key) = node.write().await.get_seq_and_key(mid).await?;

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
            let (matched, child_index) = get_child_index(current_node.clone(), key).await?;
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
    async fn get_key_value(
        &mut self,
        index: usize,
        pull_key: bool,
        pull_value: bool,
    ) -> Result<KeyValue, HyperbeeError> {
        let key = &mut self.keys[index];
        if pull_key && key.cached_key.is_none() {
            key.cached_key = Some(
                self.blocks
                    .read()
                    .await
                    .get(&key.seq, self.blocks.clone())
                    .await?
                    .read()
                    .await
                    .key
                    .clone(),
            );
        }
        if pull_value && key.cached_value.is_none() {
            key.cached_value = Some(
                self.blocks
                    .read()
                    .await
                    .get(&key.seq, self.blocks.clone())
                    .await?
                    .read()
                    .await
                    .value
                    .clone(),
            );
        }
        Ok(key.clone())
    }

    /// Get the key at the provided index
    #[tracing::instrument(skip(self))]
    async fn get_key(&mut self, index: usize) -> Result<Vec<u8>, HyperbeeError> {
        Ok(self
            .get_key_value(index, true, false)
            .await?
            .cached_key
            .expect("cached_key pulled in get_key_value"))
    }

    /// Get the key at the provided index
    #[tracing::instrument(skip(self))]
    async fn get_seq_and_key(&mut self, index: usize) -> Result<(u64, Vec<u8>), HyperbeeError> {
        let kv = self.get_key_value(index, true, false).await?;
        Ok((
            kv.seq,
            kv.cached_key.expect("cached key pulled in get_key_value"),
        ))
    }

    // Use given index to get Key.seq, which points to the block in the core where this value
    // lives. Load that BlockEntry and return (Key.seq, BlockEntry.value)
    /// Get the value for the key at the provided index
    async fn get_value(&mut self, index: usize) -> Result<(u64, Option<Vec<u8>>), HyperbeeError> {
        let kv = self.get_key_value(index, false, true).await?;
        Ok((
            kv.seq,
            kv.cached_value
                .expect("cached value pulled in get_key_value"),
        ))
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
