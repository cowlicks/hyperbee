use crate::{nearest_node, CoreMem, Hyperbee, HyperbeeError};

impl<M: CoreMem> Hyperbee<M> {
    async fn del(&mut self, key: &[u8]) -> Result<bool, HyperbeeError> {
        let root = match self.get_root(false).await? {
            Some(r) => r,
            None => return Ok(false),
        };
        let (matched, node_path, index_path) = nearest_node(root, key).await?;

        if !matched {
            return Ok(false);
        }
        todo!()
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
}
