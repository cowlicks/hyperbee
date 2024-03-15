//! Implementation of the [`Stream`] trait for [`Hyperbee`](crate::Hyperbee). Which allows
//! iterating over data in key-order, between a range, or in reverse asynchronously.
use std::{
    fmt::Debug,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use derive_builder::Builder;
use futures_lite::{future::FutureExt, StreamExt};
use tokio_stream::Stream;

use crate::{get_index_of_key, keys::InfiniteKeys, HyperbeeError, KeyValueData, SharedNode};

type PinnedFut<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;

/// Value yielded from the [`Stream`] created by traverse methods
pub type KeyDataResult = Result<KeyValueData, HyperbeeError>;
/// Value yielded from the [`Stream`] created by [`Traverse`].
type TreeItem = (KeyDataResult, SharedNode);

// TODO rename BoundaryValue?
#[derive(Clone, Debug)]
pub enum LimitValue {
    Finite(Vec<u8>),
    Infinite(InfiniteKeys),
}
use InfiniteKeys::*;
use LimitValue::*;

impl From<usize> for LimitValue {
    fn from(value: usize) -> Self {
        Finite(value.to_string().clone().as_bytes().to_vec())
    }
}

impl PartialEq<[u8]> for LimitValue {
    fn eq(&self, other: &[u8]) -> bool {
        match &self {
            Finite(vec) => vec.eq(other),
            Infinite(inf) => inf.eq(other),
        }
    }
}

impl PartialOrd<[u8]> for LimitValue {
    fn partial_cmp(&self, other: &[u8]) -> Option<std::cmp::Ordering> {
        match &self {
            Finite(vec) => {
                let slice: &[u8] = vec.as_ref();
                slice.partial_cmp(other)
            }
            Infinite(inf) => inf.partial_cmp(other),
        }
    }
}

fn validate_traverse_config_builder(builder: &TraverseConfigBuilder) -> Result<(), String> {
    match (&builder.min_value, &builder.max_value) {
        (Some(min), Some(max)) => match (min, max) {
            (_, Infinite(Negative)) => return Err("Maximum value is negative infinity".to_string()),
            (Infinite(Positive), _) => return Err("Minimum value is positive infinity".to_string()),
            (Finite(min), Finite(max)) => {
                if max < min {
                    return Err(format!(
                        "Minimum value [{min:?}] is greater than max [{max:?}]"
                    ));
                }
                if min == max {
                    #[allow(clippy::match_like_matches_macro)]
                    if match (builder.min_inclusive, builder.max_inclusive) {
                        (Some(false), _) => true,
                        (_, Some(false)) => true,
                        _ => false,
                    } {
                        return Err(format!(
                        "Minimum and maximum are equal [{min:?}] but the bounds are not both both inclusive min_inclusive = [{:?}] max_inclusive = [{:?}]",
                        builder.min_inclusive,
                        builder.max_inclusive
                    ));
                    }
                    return Ok(());
                }
            }
            (min, max) => {
                return Err(format!(
                    "Min limit [{min:?}] is greater than max limit [{max:?}]!"
                ))
            }
        },
        (_, _) => return Ok(()),
    }
    Ok(())
}

#[derive(Builder, Debug, Clone)]
#[builder(derive(Debug), build_fn(validate = "validate_traverse_config_builder"))]
/// Configuration for traverse methods
pub struct TraverseConfig {
    #[builder(default = "LimitValue::Infinite(InfiniteKeys::Negative)")]
    /// lower bound for traversal
    pub min_value: LimitValue,
    #[builder(default = "true")]
    /// whether `min_value` is inclusive
    pub min_inclusive: bool,
    #[builder(default = "LimitValue::Infinite(InfiniteKeys::Positive)")]
    /// upper bound for traversal
    pub max_value: LimitValue,
    #[builder(default = "true")]
    /// whether `max_value` is inclusive
    pub max_inclusive: bool,
    #[builder(default = "false")]
    /// traverse in reverse
    pub reversed: bool,
}

impl Default for TraverseConfig {
    fn default() -> Self {
        Self {
            min_value: Infinite(Negative),
            min_inclusive: true,
            max_value: Infinite(Positive),
            max_inclusive: true,
            reversed: false,
        }
    }
}

/// This looks crazy but... An explanation will help.
/// We are creating the iterator over the children and keys that are in bounds (set by `conf`) for
/// this `node`. Recall that in the iterator, even values are nodes, and odd values are the keys
/// between the nodes. And for leaf nodes we just get an iterator over even numbers.
///
/// Our stratagey is to find the index (`start`) for the first item which we iterate over. Note
/// that for `conf.reverse == true` this would be the item bounded by `conf.max_value`.
/// Once we have this value, we just iterate in the correct direction, until the end of the node.
/// We don't care about the other bounding value here because that is handled within the Traverse
/// logic.
#[allow(clippy::collapsible_else_if)]
async fn make_child_key_index_iter(
    conf: TraverseConfig,
    node: SharedNode,
    n_keys: usize,
    n_children: usize,
) -> Result<Box<dyn DoubleEndedIterator<Item = usize>>, HyperbeeError> {
    let is_leaf = n_children == 0;
    let step_by = if is_leaf { 2 } else { 1 };

    let (starting_key, inclusive) = if conf.reversed {
        (conf.max_value.clone(), conf.max_inclusive)
    } else {
        (conf.min_value.clone(), conf.min_inclusive)
    };

    let (matched, index) = get_index_of_key(node, &starting_key).await?;
    let start = if matched.is_some() {
        let key_index = index * 2 + 1;
        if inclusive {
            key_index
        } else {
            // exclusive
            if !conf.reversed {
                key_index + step_by
            } else {
                // reversed
                if key_index == 1 && step_by == 2 {
                    // when reversed, and matched, and the max limit is exclusive
                    // and matched key is the lowest key and node is a leaf.
                    // Then we can't take any keys or children from this node
                    return Ok(Box::new(0..0));
                    // NB don't let clippy rustfmt rewrite this if/else it is easier to document
                    // this way
                } else {
                    key_index - step_by
                }
            }
        }
    } else {
        // not matched
        let child_index = index * 2;
        if !is_leaf {
            child_index
        } else {
            // is leaf
            if !conf.reversed {
                child_index + 1
            } else {
                if child_index == 0 {
                    0
                } else {
                    child_index - 1
                }
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
        // TODO impl Ord for LimitValue and remove the expects
        match self
            .min_value
            .partial_cmp(value)
            .expect("partial_cmp never returns none")
        {
            std::cmp::Ordering::Greater => false,
            std::cmp::Ordering::Equal => self.min_inclusive,
            std::cmp::Ordering::Less => match self
                .max_value
                .partial_cmp(value)
                .expect("partial_cmp never returns none")
            {
                std::cmp::Ordering::Greater => true,
                std::cmp::Ordering::Equal => self.max_inclusive,
                std::cmp::Ordering::Less => false,
            },
        }
    }
}

// TODO add options for just yielding keys without value
/// Struct used for iterating over hyperbee with a Stream.
/// Each iteration yields the key it's value, and the "seq" for the value (the index of the value
/// in the hypercore).
pub(crate) struct Traverse<'a> {
    /// Configuration for the traversal
    config: TraverseConfig,
    /// The current node
    root: SharedNode,
    /// Option holding (number_of_keys, number_of_children) for this node
    n_keys_and_children_fut: Option<PinnedFut<'a, (usize, usize)>>,
    n_keys_and_children: Option<(usize, usize)>,

    /// Iterator over this node's keys and children.
    /// For a yielded value `i`. Even `i`'s are for children, odd are for keys.
    /// The index to the current key/child within the keys/children is geven by `i >> 1`.
    /// Leaf nodes get an iterator that on yields odd values.
    #[allow(clippy::type_complexity)]
    iter_fut:
        Option<PinnedFut<'a, Result<Box<dyn DoubleEndedIterator<Item = usize>>, HyperbeeError>>>,
    iter: Option<Pin<Box<dyn DoubleEndedIterator<Item = usize> + Unpin>>>,

    /// Future holding the next key
    next_key: Option<PinnedFut<'a, KeyDataResult>>,
    /// Future holding the next child
    next_child: Option<PinnedFut<'a, Result<Traverse<'a>, HyperbeeError>>>,
    /// Another instance of [`Traverse`] from a child node.
    child_stream: Option<Pin<Box<Traverse<'a>>>>,
}

impl Traverse<'_> {
    /// Create [`Traverse`] struct and to traverse the provided `node` based on the provided
    /// [`TraverseConfig`]
    pub fn new(note: SharedNode, config: TraverseConfig) -> Self {
        Traverse {
            config,
            root: note,
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
async fn get_n_keys_and_children(node: SharedNode) -> (usize, usize) {
    (
        node.read().await.keys.len(),
        node.read().await.n_children().await,
    )
}

async fn get_key_and_value(node: SharedNode, index: usize) -> KeyDataResult {
    node.read().await.get_key_value(index).await
}

#[tracing::instrument]
async fn get_child_stream<'a>(
    node: SharedNode,
    index: usize,
    config: TraverseConfig, // TODO should be reference
) -> Result<Traverse<'a>, HyperbeeError> {
    let child = node.read().await.get_child(index).await?;
    Ok(Traverse::new(child, config))
}

impl<'a> Stream for Traverse<'a> {
    type Item = TreeItem;
    #[tracing::instrument(skip(self, cx))]
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // await next key & value. yield if ready
        if let Some(key_fut) = &mut self.next_key {
            match key_fut.poll(cx) {
                Poll::Ready(out) => {
                    if let Ok(res) = &out {
                        if !self.config.in_bounds(&res.key) {
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

        // if awaiting next child stream, and it is ready, set it up
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

        // if we have an active child stream yield values from it if ready
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

        // get the number of keys and children in this node.
        // This is needed to build the iterator over keys and children
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

        // Ensure we have the iterator over keys and children
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
                        Err(e) => {
                            // Push error into stream
                            return Poll::Ready(Some((
                                Err(HyperbeeError::BuildIteratorInTraverseError(Box::new(e))),
                                self.root.clone(),
                            )));
                        }
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
            } else {
                // This node is done!
                cx.waker().wake_by_ref();
                return Poll::Ready(None);
            }
        }
        panic!("This sholud never happen");
    }
}

static LEADER: &str = "\t";

/// Print the keys of the provided node and it's descendents as a tree
pub(crate) async fn print(node: SharedNode) -> Result<String, HyperbeeError> {
    let starting_height = node.read().await.height().await?;
    let mut out = "".to_string();
    let stream = Traverse::new(node, TraverseConfig::default());
    tokio::pin!(stream);
    while let Some((key_data, node)) = stream.next().await {
        let h = node.read().await.height().await?;
        out += &LEADER.repeat(starting_height - h);
        let k = key_data?.key;
        let decoded_k = String::from_utf8(k)?;
        out += &decoded_k;
        out += "\n";
    }
    Ok(out)
}

#[cfg(test)]
mod test {
    use once_cell::sync::Lazy;

    use super::*;

    macro_rules! traverse_check {
        ( $range:expr, $traverse_conf:expr ) => {
            async move {
                let (hb, keys) = crate::test::hb_put!($range).await?;
                let stream = hb.traverse($traverse_conf).await?;
                tokio::pin!(stream);
                let mut res = vec![];
                while let Some(Ok(key_data)) = stream.next().await {
                    res.push(key_data.key);
                }
                Ok::<(Vec<Vec<u8>>, Vec<Vec<u8>>), HyperbeeError>((keys, res))
            }
        };
    }

    macro_rules! call_attr {
        ( $traverse_conf:expr, $attr:ident, $val:expr ) => {
            $traverse_conf.$attr($val)
        };
    }

    macro_rules! multiple_attrs {
        ( $conf:expr$(,)?) => {
            $conf
        };
        ( $conf:expr,  $label:ident = $val:expr) => {
            call_attr!($conf, $label, $val)
        };
        ( $conf:expr,  $label:ident = $val:expr, $($tail:tt)+) => {{
            multiple_attrs!(call_attr!($conf, $label, $val), $($tail)*)
        }};
    }

    macro_rules! conf_with {
        () => {{
            let conf = TraverseConfigBuilder::default();
            conf.build()

        }};
        ( $($attrs:tt)+ ) => {{
            let mut conf = TraverseConfigBuilder::default();
            let conf = multiple_attrs!(conf, $($attrs)*);
            conf.build()
        }};
    }

    macro_rules! traverse_test {
        ($($attrs:tt)*) => {
            async move {
            let conf = conf_with!($($attrs)*)?;
            let out = traverse_check!(0..10, conf).await?;
            Ok::<(Vec<Vec<u8>>, Vec<Vec<u8>>), HyperbeeError>(out)
            }
        }
    }

    fn to_limit(x: usize) -> LimitValue {
        Finite(x.to_string().clone().as_bytes().to_vec())
    }

    #[tokio::test]
    async fn fix_usize_underflow_when_matched_max_val_inclusive_and_reversed(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (hb, mut keys) = crate::test::hb_put!(0..10).await?;
        let conf = TraverseConfigBuilder::default()
            .max_value(5.into())
            .max_inclusive(false)
            .reversed(true)
            .build()?;
        let stream = hb.traverse(conf).await?;
        let res: Vec<Vec<u8>> = stream
            .collect::<Vec<KeyDataResult>>()
            .await
            .into_iter()
            .map(|x| x.unwrap().key)
            .collect();
        keys.reverse();
        assert_eq!(res, keys[5..]);
        Ok(())
    }

    #[tokio::test]
    async fn fix_match_last_key_exclusive_in_leaf() -> Result<(), Box<dyn std::error::Error>> {
        let (hb, keys) = crate::test::hb_put!(0..10).await?;
        let conf = TraverseConfigBuilder::default()
            .min_value(3.into())
            .min_inclusive(false)
            .build()?;
        let stream = hb.traverse(conf).await?;
        let res: Vec<Vec<u8>> = stream
            .collect::<Vec<KeyDataResult>>()
            .await
            .into_iter()
            .map(|x| x.unwrap().key)
            .collect();
        assert_eq!(res, keys[4..]);
        Ok(())
    }

    #[tokio::test]
    async fn fix_match_last_key_exclusive_in_node() -> Result<(), Box<dyn std::error::Error>> {
        let (hb, keys) = crate::test::hb_put!(0..10).await?;
        let conf = TraverseConfigBuilder::default()
            .min_value(4.into())
            .min_inclusive(false)
            .build()?;
        let stream = hb.traverse(conf).await?;
        let res: Vec<Vec<u8>> = stream
            .collect::<Vec<KeyDataResult>>()
            .await
            .into_iter()
            .map(|x| x.unwrap().key)
            .collect();
        assert_eq!(res, keys[5..]);
        Ok(())
    }
    static MAX: Lazy<LimitValue> = Lazy::new(|| to_limit(7));
    static MIN: Lazy<LimitValue> = Lazy::new(|| to_limit(3));

    #[tokio::test]
    async fn min_eq_max_inf() -> Result<(), Box<dyn std::error::Error>> {
        let (_input_keys, resulting_keys) =
            traverse_test!(max_value = LimitValue::Infinite(InfiniteKeys::Negative)).await?;
        assert_eq!(resulting_keys, Vec::<Vec<u8>>::new());
        Ok(())
    }

    #[tokio::test]
    async fn min_eq_max_fin() -> Result<(), Box<dyn std::error::Error>> {
        let (input_keys, resulting_keys) =
            traverse_test!(min_value = 5.into(), max_value = 5.into()).await?;
        assert_eq!(resulting_keys, input_keys[5..6]);

        assert!(traverse_test!(
            min_value = 5.into(),
            max_value = 5.into(),
            min_inclusive = false
        )
        .await
        .is_err());

        assert!(traverse_test!(
            min_value = 5.into(),
            max_value = 5.into(),
            max_inclusive = false
        )
        .await
        .is_err());

        assert!(traverse_test!(
            min_value = 5.into(),
            max_value = 5.into(),
            min_inclusive = false,
            max_inclusive = false
        )
        .await
        .is_err());
        Ok(())
    }

    #[tokio::test]
    async fn forwards() -> Result<(), Box<dyn std::error::Error>> {
        let (input_keys, resulting_keys) = traverse_test!().await?;
        assert_eq!(input_keys, resulting_keys);
        Ok(())
    }

    #[tokio::test]
    async fn reversed() -> Result<(), Box<dyn std::error::Error>> {
        let (mut input_keys, resulting_keys) = traverse_test!(reversed = true).await?;
        input_keys.reverse();
        assert_eq!(input_keys, resulting_keys);
        Ok(())
    }

    #[tokio::test]
    async fn min_exclusive_max_exclusive_backwards() -> Result<(), Box<dyn std::error::Error>> {
        let (mut input_keys, resulting_keys) = traverse_test!(
            reversed = true,
            min_value = MIN.clone(),
            max_value = MAX.clone(),
            max_inclusive = false,
            min_inclusive = false
        )
        .await?;
        input_keys.reverse();
        assert_eq!(input_keys[3..6], resulting_keys);
        Ok(())
    }

    #[tokio::test]
    async fn min_max_exclusive_backwards() -> Result<(), Box<dyn std::error::Error>> {
        let (mut input_keys, resulting_keys) = traverse_test!(
            reversed = true,
            min_value = MIN.clone(),
            max_value = MAX.clone(),
            max_inclusive = false
        )
        .await?;
        input_keys.reverse();
        assert_eq!(input_keys[3..7], resulting_keys);
        Ok(())
    }

    #[tokio::test]
    async fn min_exclusive_max_backwards() -> Result<(), Box<dyn std::error::Error>> {
        let (mut input_keys, resulting_keys) = traverse_test!(
            reversed = true,
            min_value = MIN.clone(),
            max_value = MAX.clone(),
            min_inclusive = false
        )
        .await?;
        input_keys.reverse();
        assert_eq!(input_keys[2..6], resulting_keys);
        Ok(())
    }

    #[tokio::test]
    async fn max() -> Result<(), Box<dyn std::error::Error>> {
        let (input_keys, resulting_keys) = traverse_test!(max_value = MAX.clone()).await?;
        assert_eq!(input_keys[..8], resulting_keys);
        Ok(())
    }

    #[tokio::test]
    async fn max_exclusive() -> Result<(), Box<dyn std::error::Error>> {
        let (input_keys, resulting_keys) =
            traverse_test!(max_inclusive = false, max_value = MAX.clone()).await?;
        assert_eq!(input_keys[..7], resulting_keys);
        Ok(())
    }

    #[tokio::test]
    async fn min() -> Result<(), Box<dyn std::error::Error>> {
        let (input_keys, resulting_keys) = traverse_test!(min_value = MIN.clone()).await?;
        assert_eq!(resulting_keys, input_keys[3..]);
        Ok(())
    }

    #[tokio::test]
    async fn min_exclusive() -> Result<(), Box<dyn std::error::Error>> {
        let (input_keys, resulting_keys) =
            traverse_test!(min_value = MIN.clone(), min_inclusive = false).await?;
        assert_eq!(resulting_keys, input_keys[4..]);
        Ok(())
    }

    #[tokio::test]
    async fn min_max() -> Result<(), Box<dyn std::error::Error>> {
        let (input_keys, resulting_keys) =
            traverse_test!(min_value = MIN.clone(), max_value = MAX.clone()).await?;
        assert_eq!(resulting_keys, input_keys[3..8]);
        Ok(())
    }

    #[tokio::test]
    async fn min_max_inclusive() -> Result<(), Box<dyn std::error::Error>> {
        let (input_keys, resulting_keys) = traverse_test!(
            min_value = MIN.clone(),
            max_value = MAX.clone(),
            max_inclusive = false
        )
        .await?;
        assert_eq!(resulting_keys, input_keys[3..7]);
        Ok(())
    }

    #[tokio::test]
    async fn min_inclusive_max() -> Result<(), Box<dyn std::error::Error>> {
        let (input_keys, resulting_keys) = traverse_test!(
            min_value = MIN.clone(),
            max_value = MAX.clone(),
            min_inclusive = false
        )
        .await?;
        assert_eq!(resulting_keys, input_keys[4..8]);
        Ok(())
    }

    #[tokio::test]
    async fn min_exclusive_max_exclusive() -> Result<(), Box<dyn std::error::Error>> {
        let (input_keys, resulting_keys) = traverse_test!(
            min_value = MIN.clone(),
            max_value = MAX.clone(),
            max_inclusive = false,
            min_inclusive = false
        )
        .await?;
        assert_eq!(resulting_keys, input_keys[4..7]);
        Ok(())
    }
}
