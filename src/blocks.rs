use std::{collections::BTreeMap, fmt::Debug, sync::Arc};

use async_trait::async_trait;
use derive_builder::Builder;
use hypercore::{AppendOutcome, Hypercore, Info};
use prost::Message;
use random_access_storage::RandomAccess;
use tokio::sync::{Mutex, RwLock};
use tracing::trace;

use crate::{
    changes::Changes,
    messages::{Node as NodeSchema, YoloIndex},
    BlockEntry, HyperbeeError, Shared, SharedNode,
};

#[derive(Builder, Debug)]
#[builder(pattern = "owned", derive(Debug))]
/// Interface to the underlying Hypercore
pub struct Blocks {
    #[builder(default)]
    // TODO make the cache smarter. Allow setting max size and strategy
    cache: Shared<BTreeMap<u64, Shared<BlockEntry>>>,
    core: Arc<Mutex<dyn HypercoreAcces>>,
}

#[async_trait]
pub trait HypercoreAcces: Debug + Send {
    async fn _get(&mut self, index: u64) -> Result<Option<Vec<u8>>, HyperbeeError>;
    fn _info(&self) -> Info;
    async fn _append(&mut self, data: &[u8]) -> Result<AppendOutcome, HyperbeeError>;
}

#[async_trait]
impl<M: RandomAccess + Debug + Send> HypercoreAcces for Hypercore<M> {
    async fn _get(&mut self, index: u64) -> Result<Option<Vec<u8>>, HyperbeeError> {
        Ok(self.get(index).await?)
    }

    fn _info(&self) -> Info {
        self.info()
    }

    async fn _append(&mut self, data: &[u8]) -> Result<AppendOutcome, HyperbeeError> {
        Ok(self.append(data).await?)
    }
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
        match self.core.lock().await._get(*seq).await? {
            Some(core_block) => {
                let node = NodeSchema::decode(&core_block[..])?;
                Ok(Some(BlockEntry::new(node, blocks)?))
            }
            None => Ok(None),
        }
    }

    pub async fn info(&self) -> hypercore::Info {
        self.core.lock().await._info()
    }

    pub async fn append(&self, value: &[u8]) -> Result<AppendOutcome, HyperbeeError> {
        self.core.lock().await._append(value).await
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
        trace!("Adding changes with # non-root nodes [{}]", nodes.len());
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
        node.read().await.children.children.write().await[child_index].offset = out.len() as u64;

        let childs_node = nodes[old_offset as usize].clone();
        // Push the child's node into the output
        out.push(childs_node.clone());
        // stage the child's nodes children to be reordered
        child_stack.append(&mut take_children_with_seq(&childs_node, seq).await);
    }
    out
}

// NB: reverse_nodes was the way we originally reordered the nodes so that the root would be at
// index 0.
mod _unused {
    #![allow(dead_code)]
    use crate::SharedNode;

    async fn reverse_nodes(seq: u64, nodes: &[SharedNode]) -> Vec<SharedNode> {
        let mut nodes = nodes.to_owned();
        let mut out = vec![];
        // Could # nodes be greater than u64? No way.
        let n_nodes_in_block: u64 = nodes.len() as u64;

        // re-order nodes to match js hyperbee
        // and update their offset
        let mut node_reorder = vec![];
        node_reorder.append(&mut nodes);

        node_reorder.reverse();
        for node in node_reorder.into_iter() {
            update_offsets(node.clone(), seq, n_nodes_in_block).await;
            out.push(node);
        }
        out
    }

    async fn update_offsets(node: SharedNode, seq: u64, n_nodes_in_block: u64) {
        for child in node
            .write()
            .await
            .children
            .children
            .write()
            .await
            .iter_mut()
        {
            if child.seq == seq {
                child.offset = n_nodes_in_block - child.offset - 1;
            }
        }
    }
}
