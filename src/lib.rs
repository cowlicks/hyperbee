//! Rust version of [hyperbee](https://github.com/holepunchto/hyperbee)
//! A [B-tree](https://en.wikipedia.org/wiki/B-tree) built on top of [Hypercore](https://docs.pears.com/building-blocks/hypercore).

mod messages {
    include!(concat!(env!("OUT_DIR"), "/_.rs"));
}
mod blocks;
mod changes;
mod del;
mod error;
mod hb;
mod keys;
pub mod prefixed;
mod put;
mod test;
pub mod traverse;
mod tree;

#[cfg(feature = "clib")]
mod external;

use std::{
    fmt::Debug,
    ops::{Range, RangeBounds},
    sync::Arc,
};

use prost::{bytes::Buf, DecodeError, Message};
use tokio::sync::RwLock;
use tracing::trace;

use blocks::Blocks;
use messages::{yolo_index, YoloIndex};

use tree::Tree;

pub use error::HyperbeeError;
pub use hb::{Hyperbee, HyperbeeBuilder, HyperbeeBuilderError};
pub use messages::header::Metadata;

/// Same value as JS hyperbee https://github.com/holepunchto/hyperbee/blob/e1b398f5afef707b73e62f575f2b166bcef1fa34/index.js#L663
static PROTOCOL: &str = "hyperbee";
/// Same value as JS hyperbee https://github.com/holepunchto/hyperbee/blob/e1b398f5afef707b73e62f575f2b166bcef1fa34/index.js#L16-L18
static MAX_KEYS: usize = 8;

fn min_keys(max_keys: usize) -> usize {
    max_keys >> 1
}

#[derive(Clone, Debug)]
/// Reference used within a [`Node`] of the [Hypercore](hypercore::Hypercore) block where a
/// key-value  pair is stored.
struct KeyValue {
    /// Index of key value pair within the [`hypercore::Hypercore`].
    seq: u64,
}

#[derive(Clone, Debug)]
/// Data related to a key value pair within the [`Hyperbee`].
pub struct KeyValueData {
    /// The index of the block within the [`Hypercore`](hypercore::Hypercore) where this data is stored.
    pub seq: u64,
    /// The key. The data by which the [`Hyperbee`] is ordered.
    pub key: Vec<u8>,
    /// The value.
    pub value: Option<Vec<u8>>,
}

#[derive(Debug)]
/// Pointer used within a [`Node`] to reference to it's child nodes.
struct Child {
    /// Index of the [`BlockEntry`] within the [`hypercore::Hypercore`] that contains the [`Node`]
    pub seq: u64,
    /// Index of the `Node` within the [`BlockEntry`] referenced by [`Child::seq`]
    pub offset: u64,
    /// Cache of the child node
    cached_node: Option<SharedNode>,
}

#[derive(Clone, Debug)]
/// A "block" from a [`Hypercore`](hypercore::Hypercore) deserialized into the form used in
/// Hyperbee
struct BlockEntry {
    /// Pointers::new(NodeSchema::new(hypercore.get(seq)).index))
    nodes: Vec<SharedNode>,
    /// NodeSchema::new(hypercore.get(seq)).key
    key: Vec<u8>,
    /// NodeSchema::new(hypercore.get(seq)).value
    value: Option<Vec<u8>>,
}

type Shared<T> = Arc<RwLock<T>>;
type SharedNode = Shared<Node>;
type NodePath = Vec<(SharedNode, usize)>;

#[derive(Debug)]
struct Children {
    blocks: Shared<Blocks>,
    children: RwLock<Vec<Child>>,
}

/// A node of the B-Tree within the [`Hyperbee`]
#[derive(Debug)]
struct Node {
    keys: Vec<KeyValue>,
    children: Children,
    blocks: Shared<Blocks>,
}

impl KeyValue {
    fn new(seq: u64) -> Self {
        KeyValue { seq }
    }
}

impl Child {
    fn new(seq: u64, offset: u64, node: Option<SharedNode>) -> Self {
        Child {
            seq,
            offset,
            cached_node: node,
        }
    }
}

impl Clone for Child {
    fn clone(&self) -> Self {
        Self::new(self.seq, self.offset, self.cached_node.clone())
    }
}

/// Deserialize bytes from a Hypercore block into [`Node`]s.
fn make_node_vec<B: Buf>(buf: B, blocks: Shared<Blocks>) -> Result<Vec<SharedNode>, DecodeError> {
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

impl Children {
    fn new(blocks: Shared<Blocks>, children: Vec<Child>) -> Self {
        Self {
            blocks,
            children: RwLock::new(children),
        }
    }

    #[tracing::instrument(skip(self))]
    async fn insert(&self, index: usize, new_children: Vec<Child>) {
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
    async fn get_child(&self, index: usize) -> Result<Shared<Node>, HyperbeeError> {
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

    async fn splice<R: RangeBounds<usize>, I: IntoIterator<Item = Child>>(
        &self,
        range: R,
        replace_with: I,
    ) -> Vec<Child> {
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
async fn get_index_of_key<T>(
    node: SharedNode,
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
async fn nearest_node<T>(
    node: SharedNode,
    key: &T,
) -> Result<(Option<u64>, NodePath), HyperbeeError>
where
    T: PartialOrd<[u8]> + Debug + ?Sized,
{
    let mut current_node = node;
    let mut out_path: NodePath = vec![];
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

impl Node {
    fn new(keys: Vec<KeyValue>, children: Vec<Child>, blocks: Shared<Blocks>) -> Self {
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
    async fn get_child(&self, index: usize) -> Result<Shared<Node>, HyperbeeError> {
        self.children.get_child(index).await
    }

    /// Insert a key and it's children into [`self`].
    #[tracing::instrument(skip(self))]
    async fn insert(&mut self, key_ref: KeyValue, children: Vec<Child>, range: Range<usize>) {
        trace!("inserting [{}] children", children.len());
        self.keys.splice(range.clone(), vec![key_ref]);
        self.children.insert(range.start, children).await;
    }
}

impl BlockEntry {
    fn new(entry: messages::Node, blocks: Shared<Blocks>) -> Result<Self, HyperbeeError> {
        Ok(BlockEntry {
            nodes: make_node_vec(&entry.index[..], blocks)?,
            key: entry.key,
            value: entry.value,
        })
    }

    /// Get a [`Node`] from this [`BlockEntry`] at the provided `offset`.
    /// offset is the offset of the node within the hypercore block
    fn get_tree_node(&self, offset: u64) -> Result<SharedNode, HyperbeeError> {
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
