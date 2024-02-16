use std::{
    fmt::Debug,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use derive_builder::Builder;
use futures_lite::{future::FutureExt, StreamExt};
use tokio_stream::Stream;

use crate::{get_child_index, keys::InfiniteKeys, CoreMem, HyperbeeError, SharedNode};

type PinnedFut<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;

/// Result<(Key's key, (seq, Key's value))>
type KeyData = Result<(Vec<u8>, (u64, Option<Vec<u8>>)), HyperbeeError>;
type TreeItem<M> = (KeyData, SharedNode<M>);

#[derive(Clone, Debug)]
pub enum LimitValue {
    Finite(Vec<u8>),
    Infinite(InfiniteKeys),
}

impl PartialEq<[u8]> for LimitValue {
    fn eq(&self, other: &[u8]) -> bool {
        match &self {
            LimitValue::Finite(vec) => vec.eq(other),
            LimitValue::Infinite(inf) => inf.eq(other),
        }
    }
}
impl PartialOrd<[u8]> for LimitValue {
    fn partial_cmp(&self, other: &[u8]) -> Option<std::cmp::Ordering> {
        match &self {
            LimitValue::Finite(vec) => {
                let slice: &[u8] = vec.as_ref();
                slice.partial_cmp(other)
            }
            LimitValue::Infinite(inf) => inf.partial_cmp(other),
        }
    }
}

// TODO assert that max > min when building
// TODO make using min_value/max_value setters take a Bound instead of Arce<Bound>
#[derive(Builder, Debug, Clone)]
#[builder(derive(Debug))]
pub struct TraverseConfig {
    #[builder(default = "LimitValue::Infinite(InfiniteKeys::Negative)")]
    min_value: LimitValue,
    #[builder(default = "true")]
    greter_than_or_equal_to: bool,
    #[builder(default = "LimitValue::Infinite(InfiniteKeys::Positive)")]
    max_value: LimitValue,
    #[builder(default = "true")]
    less_than_or_equal_to: bool,
    #[builder(default = "false")]
    reversed: bool,
}
impl Default for TraverseConfig {
    fn default() -> Self {
        Self {
            min_value: LimitValue::Infinite(InfiniteKeys::Negative),
            greter_than_or_equal_to: true,
            max_value: LimitValue::Infinite(InfiniteKeys::Positive),
            less_than_or_equal_to: true,
            reversed: false,
        }
    }
}
async fn make_child_key_index_iter<M: CoreMem>(
    conf: TraverseConfig,
    node: SharedNode<M>,
    n_keys: usize,
    n_children: usize,
) -> Result<Box<dyn DoubleEndedIterator<Item = usize>>, HyperbeeError> {
    let is_leaf = n_children == 0;
    let step_by = if is_leaf { 2 } else { 1 };

    let (starting_key, inclusive) = if conf.reversed {
        (conf.max_value.clone(), conf.less_than_or_equal_to)
    } else {
        (conf.min_value.clone(), conf.greter_than_or_equal_to)
    };

    let (matched, index) = get_child_index(node, &starting_key).await?;
    let start = if matched {
        let key_index = index * 2 + 1;
        if !conf.reversed {
            if inclusive {
                // start at the key
                key_index
            } else {
                // start at the next child
                // TODO handle key_index + step_by > n_keys + n_children - 1
                key_index + step_by
            }
        } else {
            if inclusive {
                key_index
            } else {
                // TODO handle key_index - step_by < 0
                key_index - step_by
            }
        }
    } else {
        if !conf.reversed {
            if is_leaf {
                index * 2 + 1
            } else {
                index * 2
            }
        } else {
            if is_leaf {
                if index == 0 {
                    0
                } else {
                    index * 2 - 1
                }
            } else {
                index * 2
            }
        }
    };

    let (start, stop) = if !conf.reversed {
        (start, (n_keys * 2 + 1))
    } else {
        if is_leaf {
            (1, start + 1)
        } else {
            (0, start + 1)
        }
    };

    let iter = (start..stop).step_by(step_by);

    if conf.reversed {
        let x: Vec<usize> = iter.rev().collect();
        Ok(Box::new(x.into_iter()))
    } else {
        Ok(Box::new(iter))
    }
}

impl TraverseConfig {
    fn in_bounds(&self, value: &Vec<u8>) -> bool {
        match self.min_value.partial_cmp(value) {
            None => todo!(),
            Some(res) => match res {
                std::cmp::Ordering::Greater => false,
                std::cmp::Ordering::Equal => self.greter_than_or_equal_to,
                std::cmp::Ordering::Less => match self.max_value.partial_cmp(value) {
                    None => todo!(),
                    Some(res) => match res {
                        std::cmp::Ordering::Greater => true,
                        std::cmp::Ordering::Equal => self.less_than_or_equal_to,
                        std::cmp::Ordering::Less => false,
                    },
                },
            },
        }
    }
}

// TODO add options for gt lt gte lte, reverse, and versions for just key/seq
// TODO add options for just yielding keys without value
/// Struct used for iterating over hyperbee with a Stream.
/// Each iteration yields the key it's value, and the "seq" for the value (the index of the value
/// in the hypercore).
pub struct Traverse<'a, M: CoreMem> {
    /// Configuration for the traversal
    config: TraverseConfig,
    /// The current node
    root: SharedNode<M>,
    /// Option holding (number_of_keys, number_of_children) for this node
    n_keys_and_children_fut: Option<PinnedFut<'a, (usize, usize)>>,
    n_keys_and_children: Option<(usize, usize)>,

    /// Iterator over this node's keys and children.
    /// For a yielded value `i`. Even `i`'s are for children, odd are for keys.
    /// The index to the current key/child within the keys/children is geven by `i >> 1`.
    /// Leaf nodes get an iterator that on yields odd values.
    iter_fut:
        Option<PinnedFut<'a, Result<Box<dyn DoubleEndedIterator<Item = usize>>, HyperbeeError>>>,
    iter: Option<Pin<Box<dyn DoubleEndedIterator<Item = usize> + Unpin>>>,

    /// Future holding the next key
    next_key: Option<PinnedFut<'a, KeyData>>,
    /// Future holding the next child
    next_child: Option<PinnedFut<'a, Result<Traverse<'a, M>, HyperbeeError>>>,
    /// Another instance of [`Traverse`] from a child node.
    child_stream: Option<Pin<Box<Traverse<'a, M>>>>,
}

impl<M: CoreMem> Traverse<'_, M> {
    fn new(root: SharedNode<M>, config: TraverseConfig) -> Self {
        Traverse {
            config,
            root,
            n_keys_and_children_fut: Option::None,
            n_keys_and_children: Option::None,
            iter_fut: Option::None,
            iter: Option::None,
            next_key: Option::None,
            next_child: Option::None,
            child_stream: Option::None,
        }
    }
}

/// Return the tuple (number_of_keys, number_of_children) for the given node
async fn get_n_keys_and_children<M: CoreMem>(node: SharedNode<M>) -> (usize, usize) {
    (
        node.read().await.keys.len(),
        node.read().await.n_children().await,
    )
}

async fn get_key_and_value<M: CoreMem>(node: SharedNode<M>, index: usize) -> KeyData {
    let key = node.write().await.get_key(index).await?;
    let value = node.read().await.get_value_of_key(index).await?;
    Ok((key, value))
}

#[tracing::instrument]
async fn get_child_stream<'a, M: CoreMem>(
    node: SharedNode<M>,
    index: usize,
    config: TraverseConfig, // TODO should be reference
) -> Result<Traverse<'a, M>, HyperbeeError> {
    let child = node.read().await.get_child(index).await?;
    Ok(Traverse::new(child, config))
}

impl<'a, M: CoreMem + 'a> Stream for Traverse<'a, M> {
    type Item = TreeItem<M>;
    #[tracing::instrument(skip(self, cx))]
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // await next key & value. yield if ready
        if let Some(key_fut) = &mut self.next_key {
            match key_fut.poll(cx) {
                Poll::Ready(out) => {
                    if let Ok(res) = &out {
                        if !self.config.in_bounds(&res.0) {
                            cx.waker().wake_by_ref();
                            return Poll::Ready(None);
                        }
                    }
                    self.next_key = None;
                    return Poll::Ready(Some((out, self.root.clone())));
                }
                Poll::Pending => return Poll::Pending,
            }
        }

        // setting up next child stream
        if let Some(child_fut) = &mut self.next_child {
            if let Poll::Ready(out) = child_fut.poll(cx) {
                self.next_child = None;
                match out {
                    Ok(stream) => {
                        self.child_stream = Some(Box::pin(stream));
                    }
                    Err(e) => {
                        // Push error into stream
                        return Poll::Ready(Some((
                            Err(HyperbeeError::GetChildInTraverseError(Box::new(e))),
                            self.root.clone(),
                        )));
                    }
                }
            }
            // waiting for child stream to resolve
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }

        // await child stream and yield it's values
        if let Some(stream) = &mut self.child_stream {
            if let Poll::Ready(out_opt) = stream.poll_next(cx) {
                match out_opt {
                    None => self.child_stream = None,
                    Some(out) => return Poll::Ready(Some(out)),
                }
            }
            // waiting for next stream item to resolve
            return Poll::Pending;
        }

        let (n_keys, n_children) = match self.n_keys_and_children {
            Some(x) => x,
            None => match &mut self.n_keys_and_children_fut {
                None => {
                    self.n_keys_and_children_fut =
                        Some(Box::pin(get_n_keys_and_children(self.root.clone())));
                    cx.waker().wake_by_ref();
                    return Poll::Pending;
                }
                Some(fut) => {
                    match fut.poll(cx) {
                        Poll::Pending => (),
                        Poll::Ready(n_keys_and_children) => {
                            self.n_keys_and_children = Some(n_keys_and_children);
                        }
                    }
                    cx.waker().wake_by_ref();
                    return Poll::Pending;
                }
            },
        };
        // set up prerequisites to get the iterator over children & keys for this node
        match &self.iter {
            Some(_) => (),
            None => match &mut self.iter_fut {
                None => {
                    let conf = self.config.clone();
                    let node = self.root.clone();
                    let iter_fut = make_child_key_index_iter(conf, node, n_keys, n_children);
                    self.iter_fut = Some(Box::pin(iter_fut));
                    cx.waker().wake_by_ref();
                    return Poll::Pending;
                }
                Some(iter_fut) => match iter_fut.poll(cx) {
                    Poll::Ready(iter_result) => match iter_result {
                        Ok(iter) => {
                            self.iter = Some(Box::pin(iter));
                            cx.waker().wake_by_ref();
                            return Poll::Pending;
                        }
                        Err(_e) => todo!(),
                    },
                    Poll::Pending => {
                        cx.waker().wake_by_ref();
                        return Poll::Pending;
                    }
                },
            },
        };

        if let Some(iter) = &mut self.iter {
            if let Some(index) = iter.next() {
                if index % 2 == 0 {
                    self.next_child = Some(Box::pin(get_child_stream(
                        self.root.clone(),
                        index >> 1,
                        self.config.clone(),
                    )));
                } else {
                    self.next_key =
                        Some(Box::pin(get_key_and_value(self.root.clone(), index >> 1)));
                }
                cx.waker().wake_by_ref();
                // start waiting for next_key or next_child
                return Poll::Pending;
            }
        }
        // create future for next child or key
        // index iterator is exhausted. We're done.
        cx.waker().wake_by_ref();
        Poll::Ready(None)
    }
}

pub fn traverse<'a, M: CoreMem>(node: SharedNode<M>, config: TraverseConfig) -> Traverse<'a, M> {
    Traverse::new(node, config)
}

static LEADER: &str = "\t";

pub async fn print<M: CoreMem>(node: SharedNode<M>) -> Result<String, HyperbeeError> {
    let starting_height = node.read().await.height().await?;
    let mut out = "".to_string();
    let stream = traverse(node, TraverseConfig::default());
    tokio::pin!(stream);
    while let Some((key_data, node)) = stream.next().await {
        let h = node.read().await.height().await?;
        out += &LEADER.repeat(starting_height - h);
        let k = key_data?.0;
        let decoded_k = String::from_utf8(k)?;
        out += &decoded_k;
        out += "\n";
    }
    Ok(out)
}

#[cfg(test)]
mod test {
    macro_rules! traverse_check {
        ( $range:expr, $tarverse_conf:expr ) => {
            async move {
                let (mut hb, keys) = crate::test::hb_put!($range).await?;
                let root = hb.get_root(false).await?.unwrap();
                let stream = traverse(root, $tarverse_conf);
                tokio::pin!(stream);
                let mut res = vec![];
                while let Some((Ok(key_data), _node)) = stream.next().await {
                    res.push(key_data.0);
                }
                Ok::<(Vec<Vec<u8>>, Vec<Vec<u8>>), HyperbeeError>((keys, res))
            }
        };
    }
    use super::*;
    #[tokio::test]
    async fn forwards() -> Result<(), Box<dyn std::error::Error>> {
        let conf = TraverseConfig::default();
        let (input_keys, resulting_keys) = traverse_check!(0..10, conf).await?;
        assert_eq!(input_keys, resulting_keys);
        Ok(())
    }

    #[tokio::test]
    async fn backwards() -> Result<(), Box<dyn std::error::Error>> {
        let conf = TraverseConfigBuilder::default().reversed(true).build()?;
        let (mut input_keys, resulting_keys) = traverse_check!(0..10, conf).await?;
        input_keys.reverse();
        assert_eq!(input_keys, resulting_keys);
        Ok(())
    }

    #[tokio::test]
    async fn less_than_or_equal() -> Result<(), Box<dyn std::error::Error>> {
        let max = 5.to_string().clone().as_bytes().to_vec();
        let conf = TraverseConfigBuilder::default()
            .max_value(LimitValue::Finite(max))
            .build()?;
        let (input_keys, resulting_keys) = traverse_check!(0..10, conf).await?;
        assert_eq!(input_keys[..6], resulting_keys);
        Ok(())
    }

    #[tokio::test]
    async fn less_than() -> Result<(), Box<dyn std::error::Error>> {
        let lim = 5.to_string().clone().as_bytes().to_vec();
        let conf = TraverseConfigBuilder::default()
            .max_value(LimitValue::Finite(lim))
            .less_than_or_equal_to(false)
            .build()?;
        let (input_keys, resulting_keys) = traverse_check!(0..10, conf).await?;
        assert_eq!(input_keys[..5], resulting_keys);
        Ok(())
    }

    #[tokio::test]
    async fn greater_than_or_equal() -> Result<(), Box<dyn std::error::Error>> {
        let lim = 5.to_string().clone().as_bytes().to_vec();
        let conf = TraverseConfigBuilder::default()
            .min_value(LimitValue::Finite(lim))
            .build()?;
        let (input_keys, resulting_keys) = traverse_check!(0..10, conf).await?;
        assert_eq!(resulting_keys, input_keys[5..]);
        Ok(())
    }

    /*
    #[tokio::test]
    async fn greater_than() -> Result<(), Box<dyn std::error::Error>> {
        let lim = 5.to_string().clone().as_bytes().to_vec();
        let conf = TraverseConfigBuilder::default()
            .max_value(Arc::new(lim))
            .greter_than_or_equal_to(false)
            .build()?;
        let (input_keys, resulting_keys) = traverse_check!(0..10, conf).await?;
        assert_eq!(input_keys[6..], resulting_keys);
        Ok(())
    }
    */
}
