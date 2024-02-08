/// Rust version of [hyperbee](https://github.com/holepunchto/hyperbee)
/// A B-tree built on top of Hypercore.
pub mod messages {
    include!(concat!(env!("OUT_DIR"), "/_.rs"));
}
pub mod blocks;
mod changes;
pub mod del;
pub mod put;
mod test;
pub mod traverse;

use blocks::{Blocks, BlocksBuilder};
use derive_builder::Builder;
use hypercore::{HypercoreBuilder, HypercoreError, Storage};
use messages::{yolo_index, Header, Node as NodeSchema, YoloIndex};
use prost::{bytes::Buf, DecodeError, EncodeError, Message};
use random_access_storage::RandomAccess;
use thiserror::Error;

use std::{
    cmp::Ordering,
    fmt::Debug,
    num::TryFromIntError,
    ops::{Range, RangeBounds},
    path::Path,
    string::FromUtf8Error,
    sync::Arc,
};
use tokio::sync::RwLock;
use tracing::trace;

pub trait CoreMem: RandomAccess + Debug + Send {}
impl<T: RandomAccess + Debug + Send> CoreMem for T {}

static PROTOCOL: &str = "hyperbee";
static MAX_KEYS: usize = 4;

fn min_keys(max_keys: usize) -> usize {
    max_keys >> 1
}

// TODO are all these used
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
    #[error("There was an error encoding a YoloIndex {0}")]
    YoloIndexEncodingError(EncodeError),
    #[error("There was an error encoding a messages::Node {0}")]
    NodeEncodingError(EncodeError),
    #[error("There was an error decoding a key")]
    KeyFromUtf8Error(#[from] FromUtf8Error),
    #[error("The tree has no root so this operation failed")]
    NoRootError,
}

#[derive(Clone, Debug)]
// TODO rename to keyvalue
/// Pointer used within a [`Node`] to point to the block where the Key's (key, value) pair is stored.
pub struct Key {
    /// Index of the key's "key" within the [`hypercore::Hypercore`].
    seq: u64,
    /// Value of the key's "key". NB: it is not the "value" corresponding to the value in a `(key,
    /// value)` pair
    keys_key: Option<Vec<u8>>,
    /// Value of the key's "Value"
    keys_value: Option<Option<Vec<u8>>>,
}
#[derive(Debug)]
/// Pointer used within a [`Node`] to point to it's child nodes.
pub struct Child<M: CoreMem> {
    /// Index of the `Node` within the [`hypercore::Hypercore`].
    pub seq: u64,
    /// Index of the `Node` within the [`messages::Node::index`].
    /// NB: offset = 0, is the topmost node
    pub offset: u64,
    node: Option<SharedNode<M>>,
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
    pub keys: Vec<Key>,
    children: Children<M>,
    blocks: Shared<Blocks<M>>,
}

/// TODO document me
#[derive(Debug, Builder)]
#[builder(pattern = "owned", derive(Debug))]
pub struct Hyperbee<M: CoreMem> {
    pub blocks: Shared<Blocks<M>>,
}

// TODO only used twice. Delete?
impl Key {
    fn new(seq: u64, keys_key: Option<Vec<u8>>, keys_value: Option<Option<Vec<u8>>>) -> Self {
        Key {
            seq,
            keys_key,
            keys_value,
        }
    }
}

impl<M: CoreMem> Child<M> {
    fn new(seq: u64, offset: u64, node: Option<SharedNode<M>>) -> Self {
        Child { seq, offset, node }
    }
}

impl<M: CoreMem> Clone for Child<M> {
    fn clone(&self) -> Self {
        Self::new(self.seq, self.offset, self.node.clone())
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
                .map(|k| Key::new(*k, Option::None, Option::None))
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

// TODO look at all references of Children.children and simplify them with methods, or remove
// methods here.
impl<M: CoreMem> Children<M> {
    fn new(blocks: Shared<Blocks<M>>, children: Vec<Child<M>>) -> Self {
        Self {
            blocks,
            children: RwLock::new(children),
        }
    }
    // TODO only used once
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

    // TODO only used twice
    #[tracing::instrument(skip(self))]
    async fn get_child(&self, index: usize) -> Result<Shared<Node<M>>, HyperbeeError> {
        let (seq, offset) = {
            let child_ref = &self.children.read().await[index];
            if let Some(node) = &child_ref.node {
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
        self.children.write().await[index].node = Some(node.clone());
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

    // TODO used once, but should prob be used other places
    async fn is_empty(&self) -> bool {
        self.children.read().await.is_empty()
    }
}

// TODO move to own module file
#[derive(Debug)]
pub enum InfiniteKeys {
    Positive,
    Negative,
}
use InfiniteKeys::{Negative, Positive};

impl PartialEq<[u8]> for InfiniteKeys {
    fn eq(&self, _other: &[u8]) -> bool {
        false
    }
}

impl PartialEq<InfiniteKeys> for [u8] {
    fn eq(&self, _other: &InfiniteKeys) -> bool {
        false
    }
}

impl PartialOrd<[u8]> for InfiniteKeys {
    fn partial_cmp(&self, _other: &[u8]) -> Option<std::cmp::Ordering> {
        Some(match self {
            Positive => Ordering::Greater,
            Negative => Ordering::Less,
        })
    }
}

impl PartialOrd<InfiniteKeys> for [u8] {
    fn partial_cmp(&self, other: &InfiniteKeys) -> Option<Ordering> {
        Some(match other {
            Positive => Ordering::Less,
            Negative => Ordering::Greater,
        })
    }
}

#[test]
fn test_inf() {
    let a: Vec<u8> = vec![1, 2, 3];
    let b: &[u8] = &[5, 6, 7];
    assert!(a[..] < InfiniteKeys::Positive);
    assert!(*b < InfiniteKeys::Positive);
    assert!(a[..] >= InfiniteKeys::Negative);
    assert!(*b >= InfiniteKeys::Negative);
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
// TODO use binary search instead of iterating over keys
#[tracing::instrument(skip(node))]
async fn nearest_node<M: CoreMem, T>(
    node: SharedNode<M>,
    key: &T,
) -> Result<(bool, NodePath<M>), HyperbeeError>
where
    T: PartialOrd<[u8]> + Debug + ?Sized,
{
    let mut current_node = node;
    let mut out_path: NodePath<M> = vec![];
    loop {
        let next_node = {
            let child_index: usize = 'found: {
                let n_keys = current_node.read().await.keys.len();
                for i in 0..n_keys {
                    let val = current_node.write().await.get_key(i).await?;
                    // found matching child
                    if key < &val[..] {
                        trace!("key {:?} < val {:?} at index {}", key, val, i);
                        out_path.push((current_node.clone(), i));
                        break 'found i;
                    }
                    // found matching key
                    if key == &val[..] {
                        trace!("key {:?} == val {:?} at index {}", key, val, i);
                        out_path.push((current_node.clone(), i));
                        return Ok((true, out_path));
                    }
                }
                // key is greater than all of this nodes keys, take last child, which has index
                // of node.keys.len()
                trace!(
                    "new key {:?} greater than all in this node index {}",
                    key,
                    n_keys
                );
                out_path.push((current_node.clone(), n_keys));
                n_keys
            };

            // leaf node with no match
            if current_node.read().await.children.is_empty().await {
                trace!("Reached leaf. Returning");
                return Ok((false, out_path));
            }

            // continue to next node
            current_node.read().await.get_child(child_index).await?
        };
        current_node = next_node;
    }
}

impl<M: CoreMem> Node<M> {
    fn new(keys: Vec<Key>, children: Vec<Child<M>>, blocks: Shared<Blocks<M>>) -> Self {
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

    /// Get the key at the provided index
    #[tracing::instrument(skip(self))]
    async fn get_key(&mut self, index: usize) -> Result<Vec<u8>, HyperbeeError> {
        let key = &mut self.keys[index];
        if let Some(value) = &key.keys_key {
            trace!("has cached value");
            return Ok(value.clone());
        }
        trace!("no cached value");
        let value = self
            .blocks
            .read()
            .await
            .get(&key.seq, self.blocks.clone())
            .await?
            .read()
            .await
            .key
            .clone();
        key.keys_key = Some(value.clone());
        Ok(value)
    }

    // Use given index to get Key.seq, which points to the block in the core where this value
    // lives. Load that BlockEntry and return (Key.seq, BlockEntry.value)
    /// Get the value for the key at the provided index
    async fn get_value_of_key(
        &self,
        index: usize,
    ) -> Result<(u64, Option<Vec<u8>>), HyperbeeError> {
        match &self.keys[index] {
            Key {
                seq,
                keys_value: Some(value),
                ..
            } => Ok((*seq, value.clone())),
            Key {
                seq,
                keys_value: None,
                ..
            } => Ok((
                *seq,
                self.blocks
                    .read()
                    .await
                    .get(seq, self.blocks.clone())
                    .await?
                    .read()
                    .await
                    .value
                    .clone(),
            )),
        }
    }

    /// Get the child at the provided index
    async fn get_child(&self, index: usize) -> Result<Shared<Node<M>>, HyperbeeError> {
        self.children.get_child(index).await
    }

    /// Insert a key and it's children into [`self`].
    #[tracing::instrument(skip(self))]
    async fn _insert(&mut self, key_ref: Key, children: Vec<Child<M>>, range: Range<usize>) {
        trace!("inserting [{}] children", children.len());
        self.keys.splice(range.clone(), vec![key_ref]);
        self.children.insert(range.start, children).await;
    }
}

impl<M: CoreMem> BlockEntry<M> {
    fn new(entry: NodeSchema, blocks: Shared<Blocks<M>>) -> Result<Self, HyperbeeError> {
        Ok(BlockEntry {
            nodes: make_node_vec(&entry.index[..], blocks)?,
            key: entry.key,
            value: entry.value,
        })
    }

    /// Get a [`Node`] from this [`BlockEntry`] at the provided `offset`.
    /// offset is the offset of the node within the hypercore block
    // TODO rename get_node
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

impl<M: CoreMem> Hyperbee<M> {
    /// The number of blocks in the hypercore.
    /// The first block is always the header block so:
    /// `version` would be the `seq` of the next block
    /// `version - 1` is most recent block
    pub async fn version(&self) -> u64 {
        self.blocks.read().await.info().await.length
    }
    /// Gets the root of the tree.
    /// When `ensure_header == true` write the hyperbee header onto the hypercore if it does not exist.
    pub async fn get_root(
        &mut self,
        ensure_header: bool,
    ) -> Result<Option<Shared<Node<M>>>, HyperbeeError> {
        let blocks = self.blocks.read().await;
        let version = self.version().await;
        if version == 0 {
            if ensure_header {
                self.ensure_header().await?;
            }
            return Ok(None);
        }
        let root = blocks
            .get(&(version - 1), self.blocks.clone())
            .await?
            .read()
            .await
            .get_tree_node(0)?;
        Ok(Some(root))
    }

    /// Get the value corresponding to the provided `key` from the Hyperbee
    /// # Errors
    /// When `Hyperbee.get_root` fails
    pub async fn get(
        &mut self,
        key: &[u8],
    ) -> Result<Option<(u64, Option<Vec<u8>>)>, HyperbeeError> {
        let node = match self.get_root(false).await? {
            None => return Ok(None),
            Some(node) => node,
        };
        let (matched, path) = nearest_node(node, key).await?;
        if matched {
            let (node, key_index) = path
                .last()
                .expect("Since `matched` was true, there must be at least one node in `path`");
            return Ok(Some(node.read().await.get_value_of_key(*key_index).await?));
        }
        Ok(None)
    }

    /// Write the header for the tree
    async fn ensure_header(&self) -> Result<bool, HyperbeeError> {
        if self.blocks.read().await.info().await.length != 0 {
            return Ok(false);
        }
        let header = Header {
            protocol: PROTOCOL.to_string(),
            metadata: None, // TODO this is this.tree.metadata in js. What should go here.
        };
        let mut buf = vec![];
        header.encode(&mut buf).unwrap();
        let _ = self.blocks.read().await.append(&buf).await?;
        // write header
        Ok(true)
    }

    /// Returs a string representing the structure of the tree showing the keys in each node
    pub async fn print(&mut self) -> Result<String, HyperbeeError> {
        let root = self
            .get_root(false)
            .await?
            .ok_or(HyperbeeError::NoRootError)?;
        let out = traverse::print(root).await?;
        Ok(out)
    }
}

impl<M: CoreMem> Clone for Hyperbee<M> {
    fn clone(&self) -> Self {
        Self {
            blocks: self.blocks.clone(),
        }
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
// TODO move to tests/
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
