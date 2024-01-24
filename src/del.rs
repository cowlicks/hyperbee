use crate::{nearest_node, put::Changes, CoreMem, Hyperbee, HyperbeeError, Key, Node};

impl<M: CoreMem> Hyperbee<M> {
    pub async fn del(&mut self, key: &[u8]) -> Result<bool, HyperbeeError> {
        let root = match self.get_root(false).await? {
            Some(r) => r,
            None => return Ok(false),
        };

        let (matched, mut node_path, mut index_path) = nearest_node(root, key).await?;

        if !matched {
            return Ok(false);
        }

        // NB: js hyperbee stores the "key" the deleted "key" in the created BlockEntry
        let mut changes: Changes<M> =
            Changes::new(self.version().await, key.clone().to_vec(), None);

        dbg!(&node_path, &index_path);
        // remove the key from the node
        let cur_node = match node_path.pop() {
            Some(node) => node,
            None => todo!(),
        };
        let cur_index = index_path
            .pop()
            .expect("node_path and index_path *should* always have the same length");

        cur_node.write().await.remove_key(cur_index).await;
        changes.add_root(cur_node);
        self.blocks.read().await.add_changes(changes).await?;
        Ok(true)
    }
}

impl<M: CoreMem> Node<M> {
    async fn remove_key(&mut self, index: usize) -> Key {
        self.keys.remove(index)
    }
}

#[cfg(test)]
mod test {
    use crate::test::in_memory_hyperbee;

    #[tokio::test]
    async fn empty_tree_no_key() -> Result<(), Box<dyn std::error::Error>> {
        let mut hb = in_memory_hyperbee().await?;
        let key = vec![1];
        let res = hb.del(&key).await?;
        assert!(!res);
        Ok(())
    }

    #[tokio::test]
    async fn no_key() -> Result<(), Box<dyn std::error::Error>> {
        let (mut hb, ..) = crate::test::hb_put!(10).await?;
        let key = vec![1];
        let res = hb.del(&key).await?;
        assert!(!res);
        Ok(())
    }

    #[tokio::test]
    async fn delete_from_root_that_is_leaf() -> Result<(), Box<dyn std::error::Error>> {
        let (mut hb, keys) = crate::test::hb_put!(4).await?;
        let k = &keys[0].clone();
        let res = hb.del(&k).await?;
        assert!(res);

        let res = hb.get(&k).await?;
        assert_eq!(res, None);

        let res = hb.get(&keys[1].clone()).await?;
        assert!(res.is_some());
        Ok(())
    }
}
