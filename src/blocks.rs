use std::{collections::BTreeMap, sync::Arc};

use derive_builder::Builder;
use hypercore::{AppendOutcome, Hypercore};
use prost::Message;
use tokio::sync::{Mutex, RwLock};
use tracing::trace;

use crate::{
    changes::Changes,
    messages::{Node as NodeSchema, YoloIndex},
    BlockEntry, CoreMem, HyperbeeError, Shared,
};

#[derive(Builder, Debug)]
#[builder(pattern = "owned", derive(Debug))]
/// Interface to the underlying Hypercore
pub struct Blocks<M: CoreMem> {
    #[builder(default)]
    // TODO make the cache smarter. Allow setting max size and strategy
    cache: Shared<BTreeMap<u64, Shared<BlockEntry>>>,
    core: Arc<Mutex<Hypercore>>,
}

impl<M: CoreMem> Blocks {
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
    // TODO create a BlockEntry from changes and add it to self.cache
    pub async fn add_changes(&self, changes: Changes) -> Result<AppendOutcome, HyperbeeError> {
        let Changes {
            key,
            value,
            nodes,
            root,
            ..
        } = changes;

        trace!("adding changes with n_nodes = {}", nodes.len());
        let mut new_nodes = vec![];
        // encode nodes
        new_nodes.push(
            root.expect("Root *should* always be added in the put/del logic")
                .read()
                .await
                .to_level()
                .await,
        );
        for node in nodes.into_iter() {
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
