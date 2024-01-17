use std::sync::Arc;

use crate::SharedNode;

use super::{
    messages::{yolo_index, Node as NodeSchema, YoloIndex},
    nearest_node, Child, CoreMem, Hyperbee, HyperbeeError, Key, Node, MAX_KEYS,
};
use prost::Message;
use tokio::sync::RwLock;

#[derive(Debug, Default)]
pub struct Changes<M: CoreMem> {
    seq: u64,
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
    pub nodes: Vec<SharedNode<M>>,
    pub root: Option<SharedNode<M>>,
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
        let offset: u64 = self
            .nodes
            .len()
            .try_into()
            .expect("TODO usize to seq (u64)");
        Child {
            seq: self.seq,
            offset,
        }
    }

    fn add_root(&mut self, root: SharedNode<M>) -> Child {
        if self.root.is_some() {
            panic!("We should never be replacing a root on a changes");
        }
        self.root = Some(root);
        Child {
            seq: self.seq,
            offset: 0,
        }
    }
}

/// Add the given `children` to the next node in `node_path` at the next index in `index_path`.
/// This creates a new node, with which we call:
/// propagate_changes_up_tree(changes, node_path, node_index, vec![new_node]);
/// This continues until we reach the root.
async fn propagate_changes_up_tree<M: CoreMem>(
    mut changes: Changes<M>,
    mut node_path: Vec<SharedNode<M>>,
    mut index_path: Vec<usize>,
    children: Vec<Child>,
) -> Changes<M> {
    let mut cur_children = children;
    loop {
        if node_path.is_empty() {
            break;
        }
        // this should add children to node
        // add node to changes, as root or node, and redo loop if not root
        let node = node_path.pop().expect("should be checked before call ");
        let index = index_path.pop().expect("should be checked before call ");
        cur_children = node
            .read()
            .await
            .children
            .splice(
                (index)..(index + 1),
                // TODO is there a way to use Some(node) here?
                cur_children.into_iter().map(|c| (c, None)).collect(),
            )
            .await
            .into_iter()
            .map(|(c, _)| c)
            .collect();
        if node_path.is_empty() {
            changes.add_root(node);
        } else {
            changes.add_node(node);
        }
    }
    changes
}

impl<M: CoreMem> Node<M> {
    async fn split(&mut self) -> (SharedNode<M>, Key, SharedNode<M>) {
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
        (
            Arc::new(RwLock::new(left)),
            mid_key,
            Arc::new(RwLock::new(right)),
        )
    }
}
impl<M: CoreMem> Hyperbee<M> {
    #[tracing::instrument(skip(self))]
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
                YoloIndex::encode(&p, &mut index).map_err(HyperbeeError::YoloIndexEncodingError)?;
                let node_schema = NodeSchema {
                    key: key.clone(),
                    value,
                    index,
                };
                let mut block = vec![];
                NodeSchema::encode(&node_schema, &mut block)
                    .map_err(HyperbeeError::NodeEncodingError)?;
                self.blocks.read().await.append(&block).await?;
                return Ok((false, 1));
            }
            Some(node) => node,
        };

        let (matched, mut node_path, mut index_path) = nearest_node(root, key).await?;

        let seq = self.version().await;
        let mut changes: Changes<M> = Changes::new(seq, key.clone(), value.clone());

        // TODO get this when me make NodeSchema
        let mut cur_key = Key::new(seq, Some(key.clone()), Some(value.clone()));
        let mut children: Vec<Child> = vec![];

        loop {
            let cur_node = match node_path.pop() {
                None => {
                    let new_root = Arc::new(RwLock::new(Node::new(
                        vec![cur_key.clone()],
                        children,
                        self.blocks.clone(),
                    )));

                    self.root = Some(new_root.clone());
                    // create a new root
                    // put chlidren in node_schema then put the below thing
                    changes.add_root(new_root);
                    let outcome = self.blocks.read().await.add_changes(changes).await?;

                    return Ok((true, outcome.length));
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
                    ._insert(cur_key, children, cur_index..stop)
                    .await;

                if !node_path.is_empty() {
                    let child = changes.add_node(cur_node);
                    let changes =
                        propagate_changes_up_tree(changes, node_path, index_path, vec![child])
                            .await;
                    let outcome = self.blocks.read().await.add_changes(changes).await?;
                    return Ok((matched, outcome.length));
                } else {
                    changes.add_root(cur_node);
                };

                let outcome = self.blocks.read().await.add_changes(changes).await?;
                // TODO propagateChangesUpTree
                return Ok((matched, outcome.length));
            }

            // No room in leaf for another key. So we split.
            cur_node
                .write()
                .await
                ._insert(cur_key, children, cur_index..cur_index)
                .await;

            let (left, mid_key, right) = cur_node.write().await.split().await;
            // add left/right to node_schema and get child pointers

            children = vec![
                changes.add_node(left.clone()),
                changes.add_node(right.clone()),
            ];
            cur_key = mid_key;
        }
    }
}
