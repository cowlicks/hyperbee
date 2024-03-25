use std::sync::Arc;

use tokio::sync::RwLock;
use tracing::trace;

use crate::{
    changes::Changes, nearest_node, Child, HyperbeeError, KeyValue, KeyValueData, Node, NodePath,
    SharedNode, Tree, MAX_KEYS,
};

/// After making changes to a tree, this function updates parent references all the way to the
/// root.
#[tracing::instrument(skip(changes, path))]
pub async fn propagate_changes_up_tree(
    mut changes: Changes,
    mut path: NodePath,
    new_child: Child,
) -> Changes {
    let mut cur_child = new_child;
    loop {
        // this should add children to node
        // add node to changes, as root or node, and redo loop if not root
        let (node, index) = match path.pop() {
            None => return changes,
            Some(x) => x,
        };
        node.read().await.children.children.write().await[index] = cur_child;
        cur_child = changes.add_changed_node(path.len(), node.clone());
    }
}

impl Node {
    /// Split an overfilled node into two nodes and a a key.
    /// Returning: `(left_lower_node, middle_key, right_higher_node)`
    #[tracing::instrument(skip(self))]
    async fn split(&mut self) -> (SharedNode, KeyValue, SharedNode) {
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

fn cas_always_true(_prev: Option<&KeyValueData>, _next: &KeyValueData) -> bool {
    true
}
impl Tree {
    #[tracing::instrument(level = "trace", skip(self, cas), ret)]
    pub async fn put_compare_and_swap(
        &self,
        key: &[u8],
        value: Option<&[u8]>,
        cas: impl FnOnce(Option<&KeyValueData>, &KeyValueData) -> bool,
    ) -> Result<(Option<u64>, Option<u64>), HyperbeeError> {
        // NB: do this before we call `version` because it can add the header block
        let maybe_root = self.get_root(true).await?;

        let seq = self.version().await;
        let mut changes: Changes = Changes::new(seq, key, value);
        let mut cur_key = KeyValue::new(seq);
        let mut children: Vec<Child> = vec![];

        let new_key_data = KeyValueData {
            seq,
            key: key.to_vec(),
            value: value.map(|v| v.to_vec()),
        };

        let matched = 'new_root: {
            // Get root and handle when it don't exist
            let root = match maybe_root {
                None => {
                    if !cas(None, &new_key_data) {
                        return Ok((None, None));
                    }
                    break 'new_root None;
                }
                Some(node) => node,
            };

            let (matched, mut path) = nearest_node(root, key).await?;

            let old_key_data = if matched.is_some() {
                let (node, index) = &path[path.len() - 1];
                Some(node.read().await.get_key_value(*index).await?)
            } else {
                None
            };

            if !cas(old_key_data.as_ref(), &new_key_data) {
                return Ok((matched, None));
            }

            loop {
                let (cur_node, cur_index) = match path.pop() {
                    None => break 'new_root matched,
                    Some(cur) => cur,
                };

                // If this is a replacemet but we have not replaced yet
                // OR there is room on this node to insert the current key
                let room_for_more_keys = cur_node.read().await.keys.len() < MAX_KEYS;
                if matched.is_some() || room_for_more_keys {
                    trace!("room for more keys or key matched");
                    let stop = match matched.is_some() {
                        true => cur_index + 1,
                        false => cur_index,
                    };

                    cur_node
                        .write()
                        .await
                        .insert(cur_key, children, cur_index..stop)
                        .await;

                    let child = changes.add_changed_node(path.len(), cur_node.clone());
                    if !path.is_empty() {
                        trace!("inserted into some child");
                        let changes = propagate_changes_up_tree(changes, path, child).await;
                        let _ = self.blocks.read().await.add_changes(changes).await?;
                        return Ok((matched, Some(seq)));
                    };

                    let _ = self.blocks.read().await.add_changes(changes).await?;
                    return Ok((matched, Some(seq)));
                }

                // No room in leaf for another key. So we split and continue.
                cur_node
                    .write()
                    .await
                    .insert(cur_key, children, cur_index..cur_index)
                    .await;

                let (left, mid_key, right) = cur_node.write().await.split().await;

                children = vec![
                    changes.add_node(left.clone()),
                    changes.add_node(right.clone()),
                ];
                cur_key = mid_key;
            }
        };

        trace!(
            "creating a new root with key = [{:#?}] and # children = [{}]",
            &cur_key,
            children.len(),
        );
        let new_root = Arc::new(RwLock::new(Node::new(
            vec![cur_key.clone()],
            children,
            self.blocks.clone(),
        )));

        // create a new root
        // put chlidren in node_schema then put the below thing
        changes.add_root(new_root);
        let _ = self.blocks.read().await.add_changes(changes).await?;

        Ok((matched, Some(seq)))
    }

    /// Insert the provide key and value into the tree.
    /// # Returns
    /// Result<(
    ///     Option<u64>,    # `seq` for old value, if replaced
    ///     u64,            # `seq` of the inserted key
    /// ),
    ///  HyperbeeError>
    #[tracing::instrument(level = "trace", skip(self), ret)]
    pub async fn put(
        &self,
        key: &[u8],
        value: Option<&[u8]>,
    ) -> Result<(Option<u64>, u64), HyperbeeError> {
        let (old, new) = self
            .put_compare_and_swap(key, value, cas_always_true)
            .await?;
        return Ok((
            old,
            new.expect("with cas_always_true this should never be none"),
        ));
    }
}

#[cfg(test)]
mod test {
    use crate::{
        test::{check_tree, i32_key_vec, Rand},
        Hyperbee, Tree,
    };

    #[tokio::test]
    async fn test_cas() -> Result<(), Box<dyn std::error::Error>> {
        let hb = Hyperbee::from_ram().await?;
        let k = b"foo";
        let res = hb.put_compare_and_swap(k, None, |_old, _new| false).await?;
        assert_eq!(res, (None, None));

        let res = hb.put_compare_and_swap(k, None, |_old, _new| true).await?;
        assert_eq!(res, (None, Some(1)));

        let res = hb.put_compare_and_swap(k, None, |_old, _new| false).await?;
        assert_eq!(res, (Some(1), None));

        let res = hb.put_compare_and_swap(k, None, |_old, _new| true).await?;
        assert_eq!(res, (Some(1), Some(2)));
        Ok(())
    }

    #[tokio::test]
    async fn test_old_seq() -> Result<(), Box<dyn std::error::Error>> {
        let hb = Tree::from_ram().await?;
        let (None, first_seq) = hb.put(b"a", None).await? else {
            panic!("should be None")
        };
        assert_eq!(first_seq, hb.version().await - 1);

        let (Some(old_seq), _second_seq) = hb.put(b"a", None).await? else {
            panic!("should be Some")
        };
        assert_eq!(first_seq, old_seq);
        Ok(())
    }

    #[tokio::test]
    async fn basic_put() -> Result<(), Box<dyn std::error::Error>> {
        let hb = Tree::from_ram().await?;
        for i in 0..4 {
            let key = vec![i];
            let val = vec![i];
            hb.put(&key, Some(&val)).await?;
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
        let hb = Tree::from_ram().await?;
        for i in 0..4 {
            let key = vec![i];
            let val = vec![i];
            // initial values
            hb.put(&key.clone(), Some(&val)).await?;
            // replace replace with val + 1
            let val = vec![i + 1_u8];
            hb.put(&key, Some(&val)).await?;
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
        let hb = Tree::from_ram().await?;
        for i in 0..3 {
            let is = i.to_string();
            let key = is.clone().as_bytes().to_vec();
            let val: Option<&[u8]> = Some(&key);
            hb.put(&key, val).await?;
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
        let mut hb = Tree::from_ram().await?;
        for i in 0..100 {
            let is = i.to_string();
            let key = is.clone().as_bytes().to_vec();
            let val = Some(key.clone());
            hb.put(&key, val.as_deref()).await?;
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

    #[tokio::test]
    async fn shuffled_put() -> Result<(), Box<dyn std::error::Error>> {
        let rand = Rand::default();
        let mut hb = Tree::from_ram().await?;

        let keys: Vec<Vec<u8>> = (0..100).map(i32_key_vec).collect();
        let keys = rand.shuffle(keys);
        let mut used: Vec<Vec<u8>> = vec![];

        for k in keys {
            used.push(k.clone());

            let val: Option<&[u8]> = Some(&k);
            hb.put(&k, val).await?;

            for kj in used.iter() {
                let val = Some(kj.clone());
                let res = hb.get(kj).await?.unwrap();
                assert_eq!(res.1, val);
            }

            hb = check_tree(hb).await?;
            let _ = hb.print().await?;
        }
        Ok(())
    }
}
