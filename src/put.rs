use std::sync::Arc;

use crate::SharedNode;

use super::{
    messages::{yolo_index, Node as NodeSchema, YoloIndex},
    nearest_node, Child, CoreMem, Hyperbee, HyperbeeError, Key, Node, MAX_KEYS,
};
use prost::Message;
use tokio::sync::RwLock;
use tracing::trace;

// TODO move this to own module
#[derive(Debug, Default)]
pub struct Changes<M: CoreMem> {
    seq: u64,
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
    pub nodes: Vec<SharedNode<M>>,
    pub root: Option<SharedNode<M>>,
}

impl<M: CoreMem> Changes<M> {
    pub fn new(seq: u64, key: Vec<u8>, value: Option<Vec<u8>>) -> Self {
        Self {
            seq,
            key,
            value,
            nodes: vec![],
            root: None,
        }
    }

    // use the result of this to insert the child into a shared node
    pub fn add_node(&mut self, node: SharedNode<M>) -> Child<M> {
        self.nodes.push(node.clone());
        let offset: u64 = self
            .nodes
            .len()
            .try_into()
            .expect("TODO usize to seq (u64)");
        Child {
            seq: self.seq,
            offset,
            child_node: Some(node.clone()),
        }
    }

    pub fn add_root(&mut self, root: SharedNode<M>) -> Child<M> {
        if self.root.is_some() {
            panic!("We should never be replacing a root on a changes");
        }
        self.root = Some(root.clone());
        Child {
            seq: self.seq,
            offset: 0,
            child_node: Some(root.clone()),
        }
    }

    pub fn add_changed_node(&mut self, path_len: usize, node: SharedNode<M>) -> Child<M> {
        if path_len == 0 {
            self.add_root(node.clone())
        } else {
            self.add_node(node.clone())
        }
    }
}

/// Add the given `children` to the next node in `node_path` at the next index in `index_path`.
/// This creates a new node, with which we call:
/// propagate_changes_up_tree(changes, node_path, node_index, vec![new_node]);
/// This continues until we reach the root.
#[tracing::instrument(skip(changes, path))]
pub async fn propagate_changes_up_tree<M: CoreMem>(
    mut changes: Changes<M>,
    mut path: Vec<(SharedNode<M>, usize)>,
    new_child: Child<M>,
) -> Changes<M> {
    let mut cur_child = new_child;
    loop {
        // this should add children to node
        // add node to changes, as root or node, and redo loop if not root
        let (node, index) = path.pop().expect("should be checked before call ");
        node.read().await.children.children.write().await[index] = cur_child;
        cur_child = changes.add_changed_node(path.len(), node.clone());
        if path.is_empty() {
            return changes;
        }
    }
}

impl<M: CoreMem> Node<M> {
    #[tracing::instrument(skip(self))]
    async fn split(&mut self) -> (SharedNode<M>, Key, SharedNode<M>) {
        let key_median_index = self.keys.len() >> 1;
        let children_median_index = self.children.len().await >> 1;
        trace!(
            "
    splitting at key index: {key_median_index}
    splitting at child index: {children_median_index}
"
        );
        let left = Node::new(
            self.keys.splice(0..key_median_index, vec![]).collect(),
            self.children.splice(0..children_median_index, vec![]).await,
            self.blocks.clone(),
        );
        let mid_key = self.keys.remove(0);
        let right = Node::new(
            self.keys.drain(..).collect(),
            self.children.splice(0.., vec![]).await,
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
    #[tracing::instrument(level = "trace", skip(self), ret)]
    pub async fn put(
        &mut self,
        key: Vec<u8>,
        value: Option<Vec<u8>>,
    ) -> Result<(bool, u64), HyperbeeError> {
        // Get root and handle when it don't exist
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
                let node_schema = NodeSchema { key, value, index };
                let mut block = vec![];
                NodeSchema::encode(&node_schema, &mut block)
                    .map_err(HyperbeeError::NodeEncodingError)?;
                self.blocks.read().await.append(&block).await?;
                return Ok((false, 1));
            }
            Some(node) => node,
        };

        let (matched, mut path) = nearest_node(root, &key[..]).await?;

        let seq = self.version().await;
        let mut changes: Changes<M> = Changes::new(seq, key.clone(), value.clone());

        let mut cur_key = Key::new(seq, Some(key), Some(value.clone()));
        let mut children: Vec<Child<M>> = vec![];

        loop {
            let (cur_node, cur_index) = match path.pop() {
                None => {
                    trace!(
                        "creating a new root with key = [{}] and children = [{:#?}]",
                        &cur_key,
                        &children
                    );
                    let new_root = Arc::new(RwLock::new(Node::new(
                        vec![cur_key.clone()],
                        children,
                        self.blocks.clone(),
                    )));

                    // create a new root
                    // put chlidren in node_schema then put the below thing
                    changes.add_root(new_root);
                    let outcome = self.blocks.read().await.add_changes(changes).await?;

                    return Ok((true, outcome.length));
                }
                Some(cur) => cur,
            };

            // If this is a replacemet but we have not replaced yet
            // OR there is room on this node to insert the current key
            let room_for_more_keys = cur_node.read().await.keys.len() < MAX_KEYS;
            if matched || room_for_more_keys {
                trace!("room for more keys or key matched");
                let stop = match matched {
                    true => cur_index + 1,
                    false => cur_index,
                };
                cur_node
                    .write()
                    .await
                    ._insert(cur_key, children, cur_index..stop)
                    .await;

                let child = changes.add_changed_node(path.len(), cur_node.clone());
                if !path.is_empty() {
                    trace!("inserted into some child");
                    let changes = propagate_changes_up_tree(changes, path, child).await;
                    let outcome = self.blocks.read().await.add_changes(changes).await?;
                    return Ok((matched, outcome.length));
                } else {
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

            children = vec![
                changes.add_node(left.clone()),
                changes.add_node(right.clone()),
            ];
            cur_key = mid_key;
        }
    }
}

#[cfg(test)]
mod test {
    use crate::test::{check_tree, in_memory_hyperbee, Rand};

    #[tokio::test]
    async fn basic_put() -> Result<(), Box<dyn std::error::Error>> {
        let mut hb = in_memory_hyperbee().await?;
        for i in 0..4 {
            let key = vec![i];
            let val = vec![i];
            hb.put(key, Some(val.clone())).await?;
            for j in 0..(i + 1) {
                let key = vec![j];
                let val = Some(key.clone());
                let res = hb.get(&key).await?.unwrap();
                dbg!(&res);
                assert_eq!(res.1, val);
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn basic_put_with_replace() -> Result<(), Box<dyn std::error::Error>> {
        let mut hb = in_memory_hyperbee().await?;
        for i in 0..4 {
            let key = vec![i];
            let val = vec![i];
            // initial values
            hb.put(key.clone(), Some(val.clone())).await?;
            // replace replace with val + 1
            let val = vec![i + 1_u8];
            hb.put(key, Some(val.clone())).await?;
            for j in 0..(i + 1) {
                let key = vec![j];
                let val = Some(vec![j + 1]);
                let res = hb.get(&key).await?.unwrap();
                dbg!(&res, &key, &val);
                assert_eq!(res.1, val);
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn print_put() -> Result<(), Box<dyn std::error::Error>> {
        let mut hb = in_memory_hyperbee().await?;
        for i in 0..3 {
            let is = i.to_string();
            let key = is.clone().as_bytes().to_vec();
            let val = is.clone().as_bytes().to_vec();
            hb.put(key, Some(val.clone())).await?;
        }
        let tree = hb.print().await?;
        assert_eq!(
            tree,
            "0
1
2
"
        );
        Ok(())
    }

    #[tokio::test]
    async fn multi_put() -> Result<(), Box<dyn std::error::Error>> {
        let mut hb = in_memory_hyperbee().await?;
        for i in 0..100 {
            let is = i.to_string();
            let key = is.clone().as_bytes().to_vec();
            let val = Some(key.clone());
            hb.put(key, val).await?;
            hb = check_tree(hb).await?;
            let _ = hb.print().await?;

            for j in 0..(i + 1) {
                let js = j.to_string();
                let key = js.clone().as_bytes().to_vec();
                let val = Some(key.clone());
                let res = hb.get(&key).await?.unwrap();
                assert_eq!(res.1, val);
            }
        }
        Ok(())
    }
    fn i32_key_vec(i: &i32) -> Vec<u8> {
        i.clone().to_string().as_bytes().to_vec()
    }

    #[tokio::test]
    async fn shuffled_put() -> Result<(), Box<dyn std::error::Error>> {
        let rand = Rand::default();
        let mut hb = in_memory_hyperbee().await?;

        let keys: Vec<i32> = (0..100).collect();
        let keys = rand.shuffle(keys);
        let mut used: Vec<i32> = vec![];

        for i in keys {
            used.push(i);

            let key = i32_key_vec(&i);
            let val = Some(key.clone());
            hb.put(key, val).await?;

            for j in used.iter() {
                let key = i32_key_vec(j);
                let val = Some(key.clone());
                let res = hb.get(&key).await?.unwrap();
                assert_eq!(res.1, val);
            }

            hb = check_tree(hb).await?;
            let _ = hb.print().await?;
        }
        println!("{}", hb.print().await?);
        Ok(())
    }
}
