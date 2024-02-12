use crate::{error::HyperbeeError, CoreMem, Shared, Tree};

pub struct Prefixed<M: CoreMem> {
    prefix: Vec<u8>,
    tree: Shared<Tree<M>>,
}

impl<M: CoreMem> Prefixed<M> {
    pub fn new(prefix: &[u8], tree: Shared<Tree<M>>) -> Self {
        Self {
            prefix: prefix.to_vec(),
            tree,
        }
    }

    /// Get the value corresponding to the provided `key` from the Hyperbee
    /// # Errors
    /// When `Hyperbee.get_root` fails
    pub async fn get(&self, key: &[u8]) -> Result<Option<(u64, Option<Vec<u8>>)>, HyperbeeError> {
        let prefixed_key: &[u8] = &[&self.prefix, key].concat();
        self.tree.write().await.get(prefixed_key).await
    }

    /// Insert the given key and value into the tree
    #[tracing::instrument(level = "trace", skip(self), ret)]
    pub async fn put(
        &self,
        key: &[u8],
        value: Option<&[u8]>,
    ) -> Result<(bool, u64), HyperbeeError> {
        let prefixed_key: &[u8] = &[&self.prefix, key].concat();
        self.tree.write().await.put(prefixed_key, value).await
    }

    /// Delete the given key from the tree
    pub async fn del(&self, key: &[u8]) -> Result<bool, HyperbeeError> {
        let prefixed_key: &[u8] = &[&self.prefix, key].concat();
        self.tree.write().await.del(prefixed_key).await
    }
}

#[cfg(test)]
mod test {
    use crate::Hyperbee;

    #[tokio::test]
    async fn prefix() -> Result<(), Box<dyn std::error::Error>> {
        let hb = Hyperbee::from_ram().await?;

        let prefix = b"my_prefix";
        let key = b"hello";

        let prefixed_hb = hb.sub(prefix);

        hb.put(key, Some(b"no prefix")).await?;

        prefixed_hb.put(key, Some(b"with prefix")).await?;

        // regular no prefix
        let Some((_, Some(res))) = hb.get(key).await? else {
            panic!("could not get key")
        };
        assert_eq!(res, b"no prefix");
        // with prefix
        let Some((_, Some(res))) = prefixed_hb.get(key).await? else {
            panic!("could not get key")
        };
        assert_eq!(res, b"with prefix");
        // regular no prefix
        // with prefix
        let manually_prefixed_key = vec![prefix.to_vec(), key.to_vec()].concat();

        // with prefix from regular
        let Some((_, Some(res))) = hb.get(&manually_prefixed_key).await? else {
            panic!("could not get key")
        };
        assert_eq!(res, b"with prefix");

        // reg delete does not delete prefixed
        assert!(hb.del(key).await?);
        // reg is gone
        assert!(hb.get(key).await?.is_none());
        // prefixed still there, accessible by reg hb
        assert!(hb.get(&manually_prefixed_key).await?.is_some());
        // prefixed hb still gets key
        assert!(prefixed_hb.get(key).await?.is_some());
        // prefixed hb delete works
        assert!(prefixed_hb.del(key).await?);
        // it's gone now
        assert!(prefixed_hb.get(key).await?.is_none());
        Ok(())
    }
}
