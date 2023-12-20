use super::{CoreMem, HyperbeeError, SharedNode};
use futures_lite::{future::FutureExt, StreamExt};
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
use tokio_stream::Stream;

type PinnedFut<T> = Pin<Box<dyn Future<Output = T>>>;

// TODO add options for gt lt gte lte, reverse, and versions for just key/seq
/// Struct used for iterating over hyperbee with a Stream
pub struct Traverse<M: CoreMem> {
    root: SharedNode<M>,
    n_leafs_and_children: Option<PinnedFut<(usize, usize)>>,
    iter: Option<Box<dyn Iterator<Item = usize>>>,
    next_leaf: Option<PinnedFut<TreeItem>>,
    next_child: Option<PinnedFut<Result<Traverse<M>, HyperbeeError>>>,
    child_stream: Option<Pin<Box<Traverse<M>>>>,
}

impl<M: CoreMem> Traverse<M> {
    fn new(root: SharedNode<M>) -> Self {
        Traverse {
            root,
            n_leafs_and_children: Option::None,
            iter: Option::None,
            next_leaf: Option::None,
            next_child: Option::None,
            child_stream: Option::None,
        }
    }
}

async fn n_leafs_and_children<M: CoreMem>(node: SharedNode<M>) -> (usize, usize) {
    (
        node.read().await.keys.len(),
        node.read().await.children.children.read().await.len(),
    )
}

type TreeItem = Result<(Vec<u8>, Option<(u64, Vec<u8>)>), HyperbeeError>;

async fn get_key_and_value<M: CoreMem>(node: SharedNode<M>, index: usize) -> TreeItem {
    let key = node.read().await.get_key(index).await?;
    let value = node.read().await.get_value_of_key(index).await?;
    Ok((key, value))
}

async fn get_child_stream<M: CoreMem>(
    node: SharedNode<M>,
    index: usize,
) -> Result<Traverse<M>, HyperbeeError> {
    let child = node.read().await.get_child(index).await?;
    Ok(Traverse::new(child))
}

impl<M: CoreMem + 'static> Stream for Traverse<M> {
    type Item = TreeItem;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // getting next leaf value
        if let Some(leaf_fut) = &mut self.next_leaf {
            match leaf_fut.poll(cx) {
                Poll::Ready(out) => {
                    self.next_leaf = None;
                    return Poll::Ready(Some(out));
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
                        return Poll::Ready(Some(Err(HyperbeeError::GetChildInTraverseError(
                            Box::new(e),
                        ))))
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
                match &mut self.n_leafs_and_children {
                    None => {
                        self.n_leafs_and_children =
                            Some(Box::pin(n_leafs_and_children(self.root.clone())));
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
                self.next_leaf = Some(Box::pin(get_key_and_value(self.root.clone(), index >> 1)));
            }
            cx.waker().wake_by_ref();
            // start waiting for next_leaf or next_child
            return Poll::Pending;
        }
        cx.waker().wake_by_ref();
        Poll::Ready(None)
    }
}

pub fn traverse<M: CoreMem>(node: SharedNode<M>) -> Traverse<M> {
    Traverse::new(node)
}
