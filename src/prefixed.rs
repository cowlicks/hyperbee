use crate::{
    error::HyperbeeError,
    traverse::{
        LimitValue::{Finite, Infinite},
        Traverse, TraverseConfig,
    },
    CoreMem, Shared, Tree,
};

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
        // TODO rm write?
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
        // TODO rm write?
        self.tree.write().await.put(prefixed_key, value).await
    }

    /// Delete the given key from the tree
    pub async fn del(&self, key: &[u8]) -> Result<bool, HyperbeeError> {
        let prefixed_key: &[u8] = &[&self.prefix, key].concat();
        // TODO rm write?
        self.tree.write().await.del(prefixed_key).await
    }

    /// Travese prefixed keys. If you provide [`TraverseConfig::min_value`] or
    /// [`TraverseConfig::max_value`] it should not include the prefix.
    pub async fn traverse<'a>(
        &self,
        conf: &TraverseConfig,
    ) -> Result<Traverse<'a, M>, HyperbeeError> {
        let end_of_prefix = increment_bytes(&self.prefix);

        let (min_value, min_inclusive) = match &conf.min_value {
            Infinite(_) => (Finite(self.prefix.clone()), true),
            Finite(key) => (
                Finite([self.prefix.clone(), key.clone()].concat().to_vec()),
                conf.min_inclusive,
            ),
        };

        let (max_value, max_inclusive) = match &conf.max_value {
            // inclusive = false because we don't want to include end_of_prefix if it is a key
            Infinite(_) => (Finite(end_of_prefix.clone()), false),
            Finite(key) => (
                Finite([self.prefix.clone(), key.clone()].concat().to_vec()),
                conf.max_inclusive,
            ),
        };
        let bounded_conf = TraverseConfig {
            min_value,
            min_inclusive,
            max_value,
            max_inclusive,
            reversed: conf.reversed,
        };
        self.tree.read().await.traverse(bounded_conf).await
    }
}

fn increment_bytes(pref: &[u8]) -> Vec<u8> {
    let len = pref.len();
    if len == 0 {
        return vec![0];
    }

    let mut out = pref.to_vec();

    let last_byte = &mut out[len - 1];

    if *last_byte == u8::MAX {
        out.push(0u8);
        return out;
    }
    *last_byte += 1;
    out
}
#[cfg(test)]
mod test {
    use super::{increment_bytes, Finite};
    use crate::{
        traverse::{Traverse, TraverseConfig, TraverseConfigBuilder, TreeItem},
        CoreMem, Hyperbee,
    };

    #[tokio::test]
    async fn test_increment_bytes() -> Result<(), Box<dyn std::error::Error>> {
        let orig: Vec<u8> = "foo".into();
        let res = increment_bytes(&orig);
        assert_eq!(res, vec![102, 111, 112]);

        let orig: Vec<u8> = vec![];
        let res = increment_bytes(&orig);
        assert_eq!(res, vec![0]);

        let orig: Vec<u8> = vec![1, 2, u8::MAX];
        let res = increment_bytes(&orig);
        assert_eq!(res, vec![1, 2, u8::MAX, 0]);
        Ok(())
    }

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
        let manually_prefixed_key = [prefix.to_vec(), key.to_vec()].concat();

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

    use tokio_stream::StreamExt;
    #[tokio::test]
    async fn prefixed_traverse_basic() -> Result<(), Box<dyn std::error::Error>> {
        let hb = Hyperbee::from_ram().await?;

        let prefix = b"p:";

        let prefixed_hb = hb.sub(prefix);

        hb.put(b"a", None).await?;
        hb.put(b"b", None).await?;

        prefixed_hb.put(b"c", None).await?;
        prefixed_hb.put(b"a", None).await?;
        prefixed_hb.put(b"b", None).await?;
        prefixed_hb.put(b"e", None).await?;
        prefixed_hb.put(b"f", None).await?;

        hb.put(b"d", None).await?;
        hb.put(b"e", None).await?;
        // prefix + 1
        hb.put(&increment_bytes(prefix), None).await?;

        let mut expected: Vec<Vec<u8>> = vec![b"a", b"b", b"c", b"e", b"f"]
            .into_iter()
            .map(|x| [prefix.to_vec(), x.to_vec()].concat())
            .collect();

        async fn collect<'a, M: CoreMem + 'a>(x: Traverse<'a, M>) -> Vec<Vec<u8>> {
            x.collect::<Vec<TreeItem<M>>>()
                .await
                .into_iter()
                .map(|x| x.0.unwrap().0)
                .collect()
        }

        let stream = prefixed_hb.traverse(TraverseConfig::default()).await?;
        let res = collect(stream).await;
        assert_eq!(res, expected);

        // with lower bound
        let conf = TraverseConfigBuilder::default()
            .min_value(Finite(b"b".into()))
            .build()?;
        let stream = prefixed_hb.traverse(conf).await?;
        let res = collect(stream).await;
        assert_eq!(res, expected[1..]);

        // with lower bound exclusive
        let conf = TraverseConfigBuilder::default()
            .min_value(Finite(b"a".into()))
            .min_inclusive(false)
            .build()?;
        let stream = prefixed_hb.traverse(conf).await?;
        let res = collect(stream).await;
        assert_eq!(res, expected[1..]);

        // with upper bound
        let conf = TraverseConfigBuilder::default()
            .max_value(Finite(b"e".into()))
            .build()?;
        let stream = prefixed_hb.traverse(conf).await?;
        let res = collect(stream).await;
        assert_eq!(res, expected[..4]);

        // exclusive upper bound
        let conf = TraverseConfigBuilder::default()
            .max_value(Finite(b"f".into()))
            .max_inclusive(false)
            .build()?;
        let stream = prefixed_hb.traverse(conf).await?;
        let res = collect(stream).await;
        assert_eq!(res, expected[..4]);

        expected.reverse();
        // reversed
        let conf = TraverseConfigBuilder::default().reversed(true).build()?;
        let stream = prefixed_hb.traverse(conf).await?;
        let res = collect(stream).await;
        assert_eq!(res, expected);

        // with lower bound
        let conf = TraverseConfigBuilder::default()
            .reversed(true)
            .min_value(Finite(b"b".into()))
            .build()?;
        let stream = prefixed_hb.traverse(conf).await?;
        let res = collect(stream).await;
        assert_eq!(res, expected[..4]);

        // with lower bound exclusive
        let conf = TraverseConfigBuilder::default()
            .reversed(true)
            .min_value(Finite(b"a".into()))
            .min_inclusive(false)
            .build()?;
        let stream = prefixed_hb.traverse(conf).await?;
        let res = collect(stream).await;
        assert_eq!(res, expected[..4]);

        // with upper bound
        let conf = TraverseConfigBuilder::default()
            .reversed(true)
            .max_value(Finite(b"e".into()))
            .build()?;
        let stream = prefixed_hb.traverse(conf).await?;
        let res = collect(stream).await;
        assert_eq!(res, expected[1..]);

        // exclusive upper bound
        let conf = TraverseConfigBuilder::default()
            .reversed(true)
            .max_value(Finite(b"f".into()))
            .max_inclusive(false)
            .build()?;
        let stream = prefixed_hb.traverse(conf).await?;
        let res = collect(stream).await;
        assert_eq!(res, expected[1..]);
        Ok(())
    }
}
