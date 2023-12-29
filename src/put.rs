use std::sync::Arc;

use crate::SharedNode;

use super::{
    messages::{yolo_index, Node as NodeSchema, YoloIndex},
    nearest_node, Child, CoreMem, Hyperbee, HyperbeeError, Key, Node, MAX_KEYS,
};
use prost::Message;
use tokio::sync::RwLock;

/*
const propagateChangesUpTree = (path, children, batch) => {
  const [node, ni] = path.shift()
  node.children.splice(ni + 1, 1, ...children)
  const newNodeRef = node.store(batch);
  if (path.length > 0) {
    return propagateChangesUpTree(path, [newNodeRef], batch)
  }
  return batch
}

  This can't actually follow trees/appendtree::propagateChangesUpTree
  because all nodes up a branch must be contained in a NodeSchema

  it should
  take a node from node_schema:
    node = node_path.pop()
  insert children_seq:
    node.children.insert(index_path.pop(), children_seq)
  insert this node:
    node_schema.index.push(node)
  if paths.length != 0:
    return propagate_changes_up_tree(node_schema, node_path, index_path, chidren_seq (TODO)).await;
 */

struct Changes<M: CoreMem> {
    seq: u64,
    key: Vec<u8>,
    value: Option<Vec<u8>>,
    nodes: Vec<SharedNode<M>>,
    root: Option<SharedNode<M>>,
}

impl<M: CoreMem> Changes<M> {
    fn new(seq: u64, key: Vec<u8>, value: Option<Vec<u8>>) -> Self {
        Self {
            seq,
            key,
            value,
            nodes: vec![],
            root: None,
        }
    }
    fn add_node(&mut self, node: SharedNode<M>) -> Child {
        self.nodes.push(node);
        Child {
            seq: self.seq,
            offset: self.nodes.len().try_into().expect("TODO"),
        }
    }
}
async fn propagate_changes_up_tree<M: CoreMem>(
    node_schema: NodeSchema,
    mut node_path: Vec<SharedNode<M>>,
    mut index_path: Vec<usize>,
    _children_seq: Vec<u64>,
) -> NodeSchema {
    let _node = node_path.pop().expect("should be checked before call ");
    let _index = index_path.pop().expect("should be checked before call ");

    return node_schema;
}

impl<M: CoreMem> Node<M> {
    async fn split(&mut self) -> (Node<M>, Key, Node<M>) {
        let median_index = self.keys.len() >> 1;

        let left = Node::new(
            self.keys.splice(0..median_index, vec![]).collect(),
            self.children
                .splice(0..median_index, vec![])
                .await
                .into_iter()
                .map(|x| x.0)
                .collect(),
            self.blocks.clone(),
        );
        let mid_key = self.keys.remove(0);
        let right = Node::new(
            self.keys.drain(..).collect(),
            self.children
                .splice(0.., vec![])
                .await
                .into_iter()
                .map(|x| x.0)
                .collect(),
            self.blocks.clone(),
        );
        (left, mid_key, right)
    }
}
impl<M: CoreMem> Hyperbee<M> {
    pub async fn put(
        &mut self,
        key: &Vec<u8>,
        value: Option<Vec<u8>>,
    ) -> Result<(bool, u64), HyperbeeError> {
        let root = match self.get_root(true).await? {
            // No root, create it. Insert key & value. Return.
            // NB: we could do two things here:
            // 1 Create root with the provided key/value and add to HC
            // 2 Create empty root add to HC then Do HB.put which uses the empty root to add new
            // key/value
            // We do 1.
            None => {
                let p = YoloIndex {
                    levels: vec![yolo_index::Level {
                        keys: vec![1],
                        children: vec![],
                    }],
                };
                let mut index = vec![];
                YoloIndex::encode(&p, &mut index)
                    .map_err(|e| HyperbeeError::YoloIndexEncodingError(e))?;
                let node_schema = NodeSchema {
                    key: key.clone(),
                    value,
                    index,
                };
                let mut block = vec![];
                NodeSchema::encode(&node_schema, &mut block)
                    .map_err(|e| HyperbeeError::NodeEncodingError(e))?;
                self.blocks.read().await.append(&block).await?;
                return Ok((false, 1));
            }
            Some(node) => node,
        };

        let (matched, mut node_path, mut index_path) = nearest_node(root, key).await?;

        let seq = self.version().await;
        let changes: Changes<M> = Changes::new(seq, key.clone(), value.clone());

        // TODO get this when me make NodeSchema
        let mut cur_key = Key {
            seq,
            value: Some(key.clone()),
        };
        let mut children: Vec<SharedNode<M>> = vec![];

        loop {
            let cur_node = match node_path.pop() {
                None => {
                    // create a new root
                    // put chlidren in node_schema then put the below thing
                    //let r = Node::new(vec![cur_key], children, self.blocks.clone());
                    todo!();
                }
                Some(cur_node) => cur_node,
            };
            let cur_index = index_path.pop().unwrap();

            // If this is a replacemet but we have not replaced yet
            // OR there is room on this node to insert the current key
            let room_for_more_keys = cur_node.read().await.keys.len() < MAX_KEYS;
            if matched || room_for_more_keys {
                let stop = match matched {
                    true => cur_index + 1,
                    false => cur_index,
                };
                cur_node
                    .write()
                    .await
                    ._insert(cur_key, vec![], cur_index..stop)
                    .await;

                let p = YoloIndex {
                    levels: vec![cur_node.read().await.to_level().await],
                };

                let mut index = vec![];
                YoloIndex::encode(&p, &mut index)
                    .map_err(|e| HyperbeeError::YoloIndexEncodingError(e))?;
                let node_schema = NodeSchema {
                    key: key.clone(),
                    value,
                    index,
                };

                let node_schema = if !node_path.is_empty() {
                    propagate_changes_up_tree(node_schema, node_path, index_path, vec![]).await
                } else {
                    node_schema
                };

                let mut block = vec![];
                NodeSchema::encode(&node_schema, &mut block)
                    .map_err(|e| HyperbeeError::NodeEncodingError(e))?;

                let outcome = self.blocks.read().await.append(&block).await?;
                // TODO propagateChangesUpTree
                return Ok((matched, outcome.length));
            }

            cur_node
                .write()
                .await
                ._insert(cur_key, vec![], cur_index..cur_index)
                .await;
            let (left, mid_key, right) = cur_node.write().await.split().await;
            // add left/right to node_schema and get child pointers
            cur_key = mid_key;
            children = vec![Arc::new(RwLock::new(left)), Arc::new(RwLock::new(right))];

            // must split node
            todo!()
        }
    }
}
