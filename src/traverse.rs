use super::{CoreMem, HyperbeeError, SharedNode};
use futures_lite::{future::FutureExt, StreamExt};
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
use tokio_stream::Stream;
use tracing::debug;

type PinnedFut<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;

// TODO add options for gt lt gte lte, reverse, and versions for just key/seq
/// Struct used for iterating over hyperbee with a Stream.
/// Each iteration yields the key it's value, and the "seq" for the value (the index of the value
/// in the hypercore).
pub struct Traverse<'a, M: CoreMem> {
    root: SharedNode<M>,
    /// Option holding (number_of_keys, number_of_children) for this node
    n_keys_and_children: Option<PinnedFut<'a, (usize, usize)>>,

    /// Iterator over this node's keys and children.
    /// For a yielded value `i`. Even `i`'s are for children, odd are for keys.
    /// The index to the current key/child within the keys/children is geven by `i >> 1`.
    /// Leaf nodes get an iterator that on yields odd values.
    iter: Option<Box<dyn Iterator<Item = usize>>>,

    /// Future holding the next key
    next_key: Option<PinnedFut<'a, KeyData>>,
    /// Future holding the next child
    next_child: Option<PinnedFut<'a, Result<Traverse<'a, M>, HyperbeeError>>>,
    /// Another instance of [`Traverse`] from a child node.
    child_stream: Option<Pin<Box<Traverse<'a, M>>>>,
}

impl<M: CoreMem> Traverse<'_, M> {
    fn new(root: SharedNode<M>) -> Self {
        Traverse {
            root,
            n_keys_and_children: Option::None,
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
        node.read().await.n_keys().await,
        node.read().await.n_children().await,
    )
}

type KeyData = Result<(Vec<u8>, Option<(u64, Vec<u8>)>), HyperbeeError>;
///Result<(key, Option<(value_seq, value)>)>
type TreeItem<M> = (KeyData, SharedNode<M>);

async fn get_key_and_value<M: CoreMem>(node: SharedNode<M>, index: usize) -> KeyData {
    let key = node.read().await.get_key(index).await?;
    let value = node.read().await.get_value_of_key(index).await?;
    Ok((key, value))
}

async fn get_child_stream<'a, M: CoreMem>(
    node: SharedNode<M>,
    index: usize,
) -> Result<Traverse<'a, M>, HyperbeeError> {
    let child = node.read().await.get_child(index).await?;
    Ok(Traverse::new(child))
}

impl<'a, M: CoreMem + 'a> Stream for Traverse<'a, M> {
    type Item = TreeItem<M>;
    #[tracing::instrument(skip(self, cx))]
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // getting next key & value
        if let Some(key_fut) = &mut self.next_key {
            match key_fut.poll(cx) {
                Poll::Ready(out) => {
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

        // pull from child stream
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

        // set up prerequisites to get the iterator we need
        let iter = match &mut self.iter {
            None => {
                match &mut self.n_keys_and_children {
                    None => {
                        self.n_keys_and_children =
                            Some(Box::pin(get_n_keys_and_children(self.root.clone())));
                    }
                    Some(fut) => match fut.poll(cx) {
                        Poll::Ready((n_keys, n_children)) => {
                            let iter: Box<dyn Iterator<Item = usize>> = if n_children != 0 {
                                Box::new(0..(n_keys * 2 + 1))
                            } else {
                                Box::new((1..(n_keys * 2 + 1)).step_by(2))
                            };
                            self.iter = Some(iter);
                        }
                        Poll::Pending => (),
                    },
                }
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            Some(iter) => iter,
        };

        if let Some(index) = iter.next() {
            if index % 2 == 0 {
                self.next_child = Some(Box::pin(get_child_stream(self.root.clone(), index >> 1)));
            } else {
                self.next_key = Some(Box::pin(get_key_and_value(self.root.clone(), index >> 1)));
            }
            cx.waker().wake_by_ref();
            // start waiting for next_key or next_child
            return Poll::Pending;
        }
        cx.waker().wake_by_ref();
        Poll::Ready(None)
    }
}

pub fn traverse<'a, M: CoreMem>(node: SharedNode<M>) -> Traverse<'a, M> {
    Traverse::new(node)
}

static LEADER: &str = "\t";

pub async fn print<M: CoreMem>(node: SharedNode<M>) -> Result<String, HyperbeeError> {
    let starting_height = node.read().await.height().await?;
    let mut out = "".to_string();
    let stream = traverse(node);
    tokio::pin!(stream);
    while let Some((key_data, node)) = stream.next().await {
        let h = node.read().await.height().await?;
        out += &LEADER.repeat(starting_height - h);
        let k = key_data?.0;
        let decoded_k = String::from_utf8(k)?;
        out += &decoded_k;
        out += "\n";
    }
    return Ok(out);
}
