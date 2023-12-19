use futures_lite::future::FutureExt;
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use super::{CoreMem, HyperbeeError, SharedNode};
use tokio_stream::Stream;

// TODO is this needed
#[pin_project::pin_project]
pub struct Traverse<M: CoreMem> {
    root: SharedNode<M>,
    n_leafs_and_children: (
        Option<Pin<Box<dyn Future<Output = (usize, usize)>>>>,
        Option<(usize, usize)>,
    ),
    next_leaf: Option<Pin<Box<dyn Future<Output = Result<Vec<u8>, HyperbeeError>>>>>,
    current_index: usize,
}
impl<M: CoreMem> Traverse<M> {
    fn new(root: SharedNode<M>) -> Self {
        Traverse {
            root,
            n_leafs_and_children: (Option::None, Option::None),
            next_leaf: Option::None,
            current_index: 0,
        }
    }
}

async fn n_leafs_and_children<M: CoreMem>(node: SharedNode<M>) -> (usize, usize) {
    (
        node.read().await.keys.len(),
        node.read().await.children.children.read().await.len(),
    )
}

async fn next_leaf<M: CoreMem>(
    node: SharedNode<M>,
    index: usize,
) -> Result<Vec<u8>, HyperbeeError> {
    node.read().await.get_key(index).await
}

impl<M: CoreMem + 'static> Stream for Traverse<M> {
    type Item = Result<Vec<u8>, HyperbeeError>;
    // create n_leafs_and_children future
    // wait for ^
    // store n_leafs_and_children

    // TODO use thunk pattern here
    // https://play.rust-lang.org/?version=stable&mode=debug&edition=2018&gist=8547cdc5267a4271812b2562483f2ceb
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Some((n_leafs, n_children)) = self.n_leafs_and_children.1 {
            if self.current_index == n_leafs {
                // done
                return Poll::Ready(None);
            }
            if self.next_leaf.is_none() {
                self.next_leaf = Some(Box::pin(next_leaf(self.root.clone(), self.current_index)));
            }
            // always true
            if let Some(next_leaf_fut) = &mut self.next_leaf {
                if let Poll::Ready(result) = next_leaf_fut.poll(cx) {
                    self.next_leaf = None;
                    self.current_index += 1;
                    return Poll::Ready(Some(result));
                }
            }
            return Poll::Pending;
        }

        if self.n_leafs_and_children.0.is_none() {
            self.n_leafs_and_children.0 = Some(Box::pin(n_leafs_and_children(self.root.clone())));
        }
        if let Some(n_leafs_and_children_fut) = &mut self.n_leafs_and_children.0 {
            // This future was completed but I polled it again :(
            match n_leafs_and_children_fut.poll(cx) {
                Poll::Ready(n_leafs_and_children_val) => {
                    println!("n_children future ready! {:?}", n_leafs_and_children_val);
                    self.n_leafs_and_children.1 = Some(n_leafs_and_children_val);
                    // TODO there must be another poll on a new future here or poll next wont be
                    // called
                    cx.waker().wake_by_ref();
                    return Poll::Pending;
                }
                Poll::Pending => {
                    println!("n_children future NOT ready!");
                    return Poll::Pending;
                }
            }
        }
        return Poll::Pending;
    }
}

pub fn traverse<M: CoreMem>(node: SharedNode<M>) -> Traverse<M> {
    Traverse::new(node)
}
