use derive_builder::Builder;
use hypercore::{AppendOutcome, Hypercore};
use tokio::sync::RwLock;
use tracing::trace;

use crate::{
    changes::Changes,
    messages::{Node as NodeSchema, YoloIndex},
    BlockEntry, CoreMem, HyperbeeError, Shared, SharedBlock,
};
use prost::Message;
use std::{collections::BTreeMap, sync::Arc};

#[derive(Builder, Debug)]
#[builder(pattern = "owned", derive(Debug))]
pub struct Blocks<M: CoreMem> {
    #[builder(default)]
    cache: Shared<BTreeMap<u64, SharedBlock<M>>>,
    core: Shared<Hypercore<M>>,
}

impl<M: CoreMem> Blocks<M> {
    /// # Errors
    /// when the provided `seq` is not in the Hypercore
    /// when the data in the Hypercore block cannot be decoded
    #[tracing::instrument(skip(self))]
    pub async fn get(
        &self,
        seq: &u64,
        blocks: Shared<Self>,
    ) -> Result<Shared<BlockEntry<M>>, HyperbeeError> {
        // check if seq is == self.core.info.length + 1
        // if so take changes and do something like:
        // changes.clone().to_block_entry()
        if let Some(block) = self._get_from_cache(seq).await {
            trace!("from cache");
            Ok(block)
        } else {
            trace!("from core");
            let block_entry = self
                ._get_from_core(seq, blocks)
                .await?
                .ok_or(HyperbeeError::NoBlockAtSeqError(*seq))?;
            let block_entry = Arc::new(RwLock::new(block_entry));
            self.cache.write().await.insert(*seq, block_entry.clone());
            Ok(block_entry)
        }
    }
    async fn _get_from_cache(&self, seq: &u64) -> Option<Shared<BlockEntry<M>>> {
        self.cache.read().await.get(seq).cloned()
    }

    pub async fn _get_from_core(
        &self,
        seq: &u64,
        blocks: Shared<Self>,
    ) -> Result<Option<BlockEntry<M>>, HyperbeeError> {
        match self.core.write().await.get(*seq).await? {
            Some(core_block) => {
                let node = NodeSchema::decode(&core_block[..])?;
                Ok(Some(BlockEntry::new(node, blocks)?))
            }
            None => Ok(None),
        }
    }

    pub async fn info(&self) -> hypercore::Info {
        self.core.read().await.info()
    }
    pub async fn append(&self, value: &[u8]) -> Result<AppendOutcome, HyperbeeError> {
        Ok(self.core.write().await.append(value).await?)
    }

    #[tracing::instrument(skip(self, changes))]
    pub async fn add_changes(&self, changes: Changes<M>) -> Result<AppendOutcome, HyperbeeError> {
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

        let mut index_buf = vec![];
        YoloIndex::encode(&index, &mut index_buf).map_err(HyperbeeError::YoloIndexEncodingError)?;

        let node_schema = NodeSchema {
            key,
            value,
            index: index_buf,
        };

        let mut node_schema_buf = vec![];
        NodeSchema::encode(&node_schema, &mut node_schema_buf)
            .map_err(HyperbeeError::NodeEncodingError)?;
        self.append(&node_schema_buf).await
    }
}
