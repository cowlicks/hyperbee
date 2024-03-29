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
    BlockEntry, Child, HyperbeeError, Shared, SharedNode,
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

    pub async fn get_from_core(
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
    // TODO create a BlockEntry from changes and add it to self.cache
    pub async fn add_changes(&self, changes: Changes) -> Result<AppendOutcome, HyperbeeError> {
        let Changes {
            key,
            value,
            mut nodes,
            seq,
            ..
        } = changes;
        trace!("Adding changes with # non-root nodes [{}]", nodes.len());
        let mut new_nodes = vec![];
        // Could # nodes be greater than u64? No way.
        let n_nodes_in_block: u64 = nodes.len() as u64;

        // re-order nodes to match js hyperbee
        // and update their offset
        let mut node_reorder = vec![];
        node_reorder.append(&mut nodes);

        node_reorder.reverse();
        for node in node_reorder.into_iter() {
            node.write()
                .await
                .children
                .update_offsets(seq, n_nodes_in_block)
                .await;
            new_nodes.push(node.read().await.to_level().await);
        }

        let index = YoloIndex { levels: new_nodes };

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
