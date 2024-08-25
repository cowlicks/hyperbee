use std::{collections::BTreeMap, fmt::Debug, sync::Arc};

use derive_builder::Builder;
use hypercore::{AppendOutcome, Hypercore};
use prost::{bytes::Buf, DecodeError, Message};
use tokio::sync::{Mutex, RwLock};
use tracing::trace;

use crate::{
    changes::Changes,
    messages::{Node as NodeSchema, YoloIndex},
    wchildren, Child, HyperbeeError, KeyValue, Node, Shared, SharedNode,
};

#[derive(Builder, Debug)]
#[builder(pattern = "owned", derive(Debug))]
/// Interface to the underlying Hypercore
pub struct Blocks {
    #[builder(default)]
    // TODO make the cache smarter. Allow setting max size and strategy
    cache: Shared<BTreeMap<u64, Shared<BlockEntry>>>,
    core: Arc<Mutex<Hypercore>>,
}

impl Blocks {
    /// Get a BlockEntry for the given `seq`
    /// # Errors
    /// when the provided `seq` is not in the Hypercore
    /// when the data in the Hypercore block cannot be decoded
    #[tracing::instrument(skip(self, blocks))]
    pub async fn get(
        &self,
        seq: &u64,
        blocks: Shared<Self>,
    ) -> Result<Shared<BlockEntry>, HyperbeeError> {
        if let Some(block) = self.get_from_cache(seq).await {
            trace!("from cache");
            Ok(block)
        } else {
            trace!("from Hypercore");
            let block_entry = self
                .get_from_core(seq, blocks)
                .await?
                .ok_or(HyperbeeError::NoBlockAtSeqError(*seq))?;
            let block_entry = Arc::new(RwLock::new(block_entry));
            self.cache.write().await.insert(*seq, block_entry.clone());
            Ok(block_entry)
        }
    }
    async fn get_from_cache(&self, seq: &u64) -> Option<Shared<BlockEntry>> {
        self.cache.read().await.get(seq).cloned()
    }

    async fn get_from_core(
        &self,
        seq: &u64,
        blocks: Shared<Self>,
    ) -> Result<Option<BlockEntry>, HyperbeeError> {
        match self.core.lock().await.get(*seq).await? {
            Some(core_block) => {
                let node = NodeSchema::decode(&core_block[..])?;
                Ok(Some(BlockEntry::new(node, blocks)?))
            }
            None => Ok(None),
        }
    }

    pub async fn info(&self) -> hypercore::Info {
        self.core.lock().await.info()
    }

    pub async fn append(&self, value: &[u8]) -> Result<AppendOutcome, HyperbeeError> {
        Ok(self.core.lock().await.append(value).await?)
    }

    #[tracing::instrument(skip(self, changes))]
    /// Commit [`Changes`](crate::changes::Changes) to the Hypercore
    pub async fn add_changes(&self, changes: Changes) -> Result<AppendOutcome, HyperbeeError> {
        let Changes {
            key,
            value,
            nodes,
            seq,
            ..
        } = changes;
        trace!("Adding changes with # nodes [{}]", nodes.len());
        let reordered_nodes = reorder_nodes(seq, &nodes).await;

        let mut levels = vec![];
        for node in reordered_nodes {
            levels.push(node.read().await.to_level().await)
        }

        let index = YoloIndex { levels };

        let mut index_buf = Vec::with_capacity(index.encoded_len());
        YoloIndex::encode(&index, &mut index_buf).map_err(HyperbeeError::YoloIndexEncodingError)?;

        let node_schema = NodeSchema {
            key,
            value,
            index: index_buf,
        };

        let mut node_schema_buf = Vec::with_capacity(node_schema.encoded_len());
        NodeSchema::encode(&node_schema, &mut node_schema_buf)
            .map_err(HyperbeeError::NodeEncodingError)?;
        self.append(&node_schema_buf).await
    }
}

/// Gets the references to the children for the provided `seq` from the given `node`.
/// NB: we `.rev()` this because we're inserting these into a stack, and want children poped from
/// the stack to be in order.
async fn take_children_with_seq(node: &SharedNode, seq: u64) -> Vec<(SharedNode, usize)> {
    node.read()
        .await
        .children
        .children
        .read()
        .await
        .iter()
        .enumerate()
        .filter(|(_i, c)| c.seq == seq)
        .rev()
        .map(|(i, _)| (node.clone(), i))
        .collect()
}

/// To get the same on-disk binary data as JavaScript Hyperbee, we reorder the nodes.
/// via a depth-first search.
/// https://github.com/holepunchto/hyperbee/blob/e1b398f5afef707b73e62f575f2b166bcef1fa34/index.js#L237-L249
async fn reorder_nodes(seq: u64, nodes: &[SharedNode]) -> Vec<SharedNode> {
    let root = &nodes[nodes.len() - 1];
    let mut child_stack = vec![];
    let mut out = vec![root.clone()];

    child_stack.append(&mut take_children_with_seq(root, seq).await);

    while let Some((node, child_index)) = child_stack.pop() {
        // Get the next child, update it's offset

        // The get the childs old offset, so we can get the node it points to
        let old_offset = node.read().await.children.children.read().await[child_index].offset;
        // The child's node is pushed into `out` so it's offset will be `out.len()`
        wchildren!(node)[child_index].offset = out.len() as u64;

        let childs_node = nodes[old_offset as usize].clone();
        // Push the child's node into the output
        out.push(childs_node.clone());
        // stage the child's nodes children to be reordered
        child_stack.append(&mut take_children_with_seq(&childs_node, seq).await);
    }
    out
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
                children.push(Child::new(level.children[i], level.children[i + 1]));
            }
            Arc::new(RwLock::new(Node::new(keys, children, blocks.clone())))
        })
        .collect())
}

/// A "block" from a [`Hypercore`](hypercore::Hypercore) deserialized into the form used in
/// Hyperbee
pub(crate) struct BlockEntry {
    /// Pointers::new(NodeSchema::new(hypercore.get(seq)).index))
    nodes: Vec<SharedNode>,
    /// NodeSchema::new(hypercore.get(seq)).key
    pub key: Vec<u8>,
    /// NodeSchema::new(hypercore.get(seq)).value
    pub value: Option<Vec<u8>>,
}

impl BlockEntry {
    fn new(entry: NodeSchema, blocks: Shared<Blocks>) -> Result<Self, HyperbeeError> {
        Ok(BlockEntry {
            nodes: make_node_vec(&entry.index[..], blocks)?,
            key: entry.key,
            value: entry.value,
        })
    }

    /// Get a [`Node`] from this [`BlockEntry`] at the provided `offset`.
    /// offset is the offset of the node within the hypercore block
    pub fn get_tree_node(&self, offset: u64) -> Result<SharedNode, HyperbeeError> {
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

impl Debug for BlockEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "BlockEntry {{ ")?;
        let mut nodes = vec![];
        for node in self.nodes.iter() {
            nodes.push(node.try_read().unwrap());
        }
        f.debug_list().entries(nodes).finish()?;
        write!(f, "}}")
    }
}
