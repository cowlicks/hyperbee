use derive_builder::Builder;
use futures_lite::{Stream, StreamExt};

use crate::{
    error::HyperbeeError,
    traverse::{
        LimitValue::{Finite, Infinite},
        TraverseConfig, TreeItem,
    },
    CoreMem, KeyValueData, Shared, Tree,
};

pub static DEFAULT_PREFIXED_SEPERATOR: &[u8; 1] = b"\0";
#[derive(Builder, Debug, Clone)]
#[builder(derive(Debug))]
pub struct PrefixedConfig {
    #[builder(default = "DEFAULT_PREFIXED_SEPERATOR.to_vec()")]
    /// The seperator between the prefix and the key. The default is the NULL byte `b"\0"` which is
    /// the same as the JavaScript implementation
    seperator: Vec<u8>,
}
impl Default for PrefixedConfig {
    fn default() -> Self {
        Self {
            seperator: DEFAULT_PREFIXED_SEPERATOR.to_vec(),
        }
    }
}

/// A "sub" [`Hyperbee`](crate::Hyperbee), which can be used for grouping data. When inserted keyss are automatically prefixed
/// with [`Prefixed::prefix`].
pub struct Prefixed<M: CoreMem> {
    /// All keys inserted with [`Prefixed::put`] are prefixed with this value
    pub prefix: Vec<u8>,
    tree: Shared<Tree<M>>,
    conf: PrefixedConfig,
}

// We use this to DRY the code for getting a prefixed key.
// The prefixed key is a slice, so we can't build it within a func and return it
// (because we cant return a reference to a value made within a function)
macro_rules! with_key_prefix {
    ($self:ident, $key:expr) => {
        &[&$self.prefix, &$self.conf.seperator, $key].concat()
    };
}
impl<M: CoreMem> Prefixed<M> {
    pub fn new(prefix: &[u8], tree: Shared<Tree<M>>, conf: PrefixedConfig) -> Self {
        Self {
            prefix: prefix.to_vec(),
            tree,
            conf,
        }
    }

    /// Get the value corresponding to the provided `key` from the Hyperbee
    /// # Errors
    /// When `Hyperbee.get_root` fails
    pub async fn get(&self, key: &[u8]) -> Result<Option<(u64, Option<Vec<u8>>)>, HyperbeeError> {
        let prefixed_key: &[u8] = with_key_prefix!(self, key);
        self.tree.read().await.get(prefixed_key).await
    }

    /// Insert the given key and value into the tree
    /// Returs the `seq` of the new key, and `Option<u64>` which contains the `seq` of the old key
    /// if it was replaced.
    #[tracing::instrument(level = "trace", skip(self), ret)]
    pub async fn put(
        &self,
        key: &[u8],
        value: Option<&[u8]>,
    ) -> Result<(Option<u64>, u64), HyperbeeError> {
        let prefixed_key: &[u8] = with_key_prefix!(self, key);
        self.tree.read().await.put(prefixed_key, value).await
    }

    /// Like [`Prefixed::put`] but takes a `compare_and_swap` function.
    /// The `compared_and_swap` function is called with the old key (if present), and the new key.
    /// The new key is only inserted if `compare_and_swap` returns true.
    /// Returs two `Option<u64>`s. The first is the old key, the second is the new key.
    pub async fn put_compare_and_swap(
        &self,
        key: &[u8],
        value: Option<&[u8]>,
        cas: impl FnOnce(Option<&KeyValueData>, &KeyValueData) -> bool,
    ) -> Result<(Option<u64>, Option<u64>), HyperbeeError> {
        let prefixed_key: &[u8] = with_key_prefix!(self, key);
        self.tree
            .read()
            .await
            .put_compare_and_swap(prefixed_key, value, cas)
            .await
    }

    /// Delete the given key from the tree
    /// Returns the `seq` from the key if it was deleted.
    pub async fn del(&self, key: &[u8]) -> Result<Option<u64>, HyperbeeError> {
        let prefixed_key: &[u8] = with_key_prefix!(self, key);
        self.tree.read().await.del(prefixed_key).await
    }

    /// Like [`Prefixed::del`] but takes a `compare_and_swap` function.
    /// Before deleting the function is called with existing key's [`KeyValueData`].
    /// The key is only deleted if `compare_and_swap` returs true.
    /// Returns the `bool` representing the result of `compare_and_swap`, and the `seq` for the
    /// key.
    pub async fn del_compare_and_swap(
        &self,
        key: &[u8],
        cas: impl FnOnce(&KeyValueData) -> bool,
    ) -> Result<Option<(bool, u64)>, HyperbeeError> {
        let prefixed_key: &[u8] = with_key_prefix!(self, key);
        self.tree
            .read()
            .await
            .del_compare_and_swap(prefixed_key, cas)
            .await
    }

    /// Travese prefixed keys. If you provide [`TraverseConfig::min_value`] or
    /// [`TraverseConfig::max_value`] it should not include the prefix.
    pub async fn traverse<'a>(
        &self,
        conf: &TraverseConfig,
    ) -> Result<impl Stream<Item = TreeItem<M>> + 'a, HyperbeeError>
    where
        M: 'a,
    {
        let end_of_prefix = increment_bytes(&self.prefix);

        let (min_value, min_inclusive) = match &conf.min_value {
            Infinite(_) => (Finite(self.prefix.clone()), true),
            Finite(key) => (
                Finite(with_key_prefix!(self, key.as_slice()).to_vec()),
                conf.min_inclusive,
            ),
        };

        let (max_value, max_inclusive) = match &conf.max_value {
            // inclusive = false because we don't want to include end_of_prefix if it is a key
            Infinite(_) => (Finite(end_of_prefix.clone()), false),
            Finite(key) => (
                Finite(with_key_prefix!(self, key.as_slice()).to_vec()),
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

        let stream = self.tree.read().await.traverse(bounded_conf).await?;
        let len_drain = self.prefix.len() + self.conf.seperator.len();
        // strip `self.prefix + self.conf.seperator` from the beggining of the key
        Ok(stream.map(move |res| {
            let len_drain = len_drain;
            let stripped_kv = res.0.map(|mut x| {
                x.key.drain(..len_drain);
                x
            });
            (stripped_kv, res.1)
        }))
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
    use super::{
        increment_bytes, Finite, PrefixedConfig, PrefixedConfigBuilder, DEFAULT_PREFIXED_SEPERATOR,
    };
    use crate::{
        traverse::{TraverseConfig, TraverseConfigBuilder, TreeItem},
        CoreMem, Hyperbee,
    };

    #[test]
    fn prefixed_conf() -> Result<(), Box<dyn std::error::Error>> {
        let p = PrefixedConfigBuilder::default().build()?;
        assert_eq!(p.seperator, b"\0");
        let p = PrefixedConfig::default();
        assert_eq!(p.seperator, b"\0");
        Ok(())
    }

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

        let prefixed_hb = hb.sub(prefix, Default::default());

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
        let manually_prefixed_key = [
            prefix.to_vec(),
            DEFAULT_PREFIXED_SEPERATOR.to_vec(),
            key.to_vec(),
        ]
        .concat();

        // with prefix from regular
        let Some((_, Some(res))) = hb.get(&manually_prefixed_key).await? else {
            panic!("could not get key")
        };
        assert_eq!(res, b"with prefix");

        // reg delete does not delete prefixed
        assert!(hb.del(key).await?.is_some());
        // reg is gone
        assert!(hb.get(key).await?.is_none());
        // prefixed still there, accessible by reg hb
        assert!(hb.get(&manually_prefixed_key).await?.is_some());
        // prefixed hb still gets key
        assert!(prefixed_hb.get(key).await?.is_some());
        // prefixed hb delete works
        assert!(prefixed_hb.del(key).await?.is_some());
        // it's gone now
        assert!(prefixed_hb.get(key).await?.is_none());
        Ok(())
    }

    use futures_lite::Stream;
    use tokio_stream::StreamExt;
    #[tokio::test]
    async fn prefixed_traverse_basic() -> Result<(), Box<dyn std::error::Error>> {
        let hb = Hyperbee::from_ram().await?;

        let prefix = b"p:";

        let prefixed_hb = hb.sub(prefix, Default::default());

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
            .map(|x| x.to_vec())
            .collect();

        async fn collect<'a, M: CoreMem + 'a>(x: impl Stream<Item = TreeItem<M>>) -> Vec<Vec<u8>> {
            x.collect::<Vec<TreeItem<M>>>()
                .await
                .into_iter()
                .map(|x| x.0.unwrap().key)
                .collect()
        }

        let stream = prefixed_hb.traverse(&TraverseConfig::default()).await?;
        let res = collect(stream).await;
        assert_eq!(res, expected);

        // with lower bound
        let conf = TraverseConfigBuilder::default()
            .min_value(Finite(b"b".into()))
            .build()?;
        let stream = prefixed_hb.traverse(&conf).await?;
        let res = collect(stream).await;
        assert_eq!(res, expected[1..]);

        // with lower bound exclusive
        let conf = TraverseConfigBuilder::default()
            .min_value(Finite(b"a".into()))
            .min_inclusive(false)
            .build()?;
        let stream = prefixed_hb.traverse(&conf).await?;
        let res = collect(stream).await;
        assert_eq!(res, expected[1..]);

        // with upper bound
        let conf = TraverseConfigBuilder::default()
            .max_value(Finite(b"e".into()))
            .build()?;
        let stream = prefixed_hb.traverse(&conf).await?;
        let res = collect(stream).await;
        assert_eq!(res, expected[..4]);

        // exclusive upper bound
        let conf = TraverseConfigBuilder::default()
            .max_value(Finite(b"f".into()))
            .max_inclusive(false)
            .build()?;
        let stream = prefixed_hb.traverse(&conf).await?;
        let res = collect(stream).await;
        assert_eq!(res, expected[..4]);

        expected.reverse();
        // reversed
        let conf = TraverseConfigBuilder::default().reversed(true).build()?;
        let stream = prefixed_hb.traverse(&conf).await?;
        let res = collect(stream).await;
        assert_eq!(res, expected);

        // with lower bound
        let conf = TraverseConfigBuilder::default()
            .reversed(true)
            .min_value(Finite(b"b".into()))
            .build()?;
        let stream = prefixed_hb.traverse(&conf).await?;
        let res = collect(stream).await;
        assert_eq!(res, expected[..4]);

        // with lower bound exclusive
        let conf = TraverseConfigBuilder::default()
            .reversed(true)
            .min_value(Finite(b"a".into()))
            .min_inclusive(false)
            .build()?;
        let stream = prefixed_hb.traverse(&conf).await?;
        let res = collect(stream).await;
        assert_eq!(res, expected[..4]);

        // with upper bound
        let conf = TraverseConfigBuilder::default()
            .reversed(true)
            .max_value(Finite(b"e".into()))
            .build()?;
        let stream = prefixed_hb.traverse(&conf).await?;
        let res = collect(stream).await;
        assert_eq!(res, expected[1..]);

        // exclusive upper bound
        let conf = TraverseConfigBuilder::default()
            .reversed(true)
            .max_value(Finite(b"f".into()))
            .max_inclusive(false)
            .build()?;
        let stream = prefixed_hb.traverse(&conf).await?;
        let res = collect(stream).await;
        assert_eq!(res, expected[1..]);
        Ok(())
    }
}
