use crate::{CoreMem, Hyperbee, HyperbeeError};

impl<M: CoreMem> Hyperbee<M> {
    async fn del(&self, key: &Vec<u8>) -> Result<bool, HyperbeeError> {
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
        assert_eq!(res, false);
        Ok(())
    }
}
