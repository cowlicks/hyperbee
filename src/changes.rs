use crate::{Child, CoreMem, SharedNode};

#[derive(Debug, Default)]
/// Structure to store in-progress changes to the [`Hyperbee`]
/// NB: because of how hyperbee-js works, we need to distinguish between root/non-root nodes.
pub struct Changes<M: CoreMem> {
    seq: u64,
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
    pub nodes: Vec<SharedNode<M>>,
    pub root: Option<SharedNode<M>>,
}

impl<M: CoreMem> Changes<M> {
    pub fn new(seq: u64, key: Vec<u8>, value: Option<Vec<u8>>) -> Self {
        Self {
            seq,
            key,
            value,
            nodes: vec![],
            root: None,
        }
    }

    // use the result of this to insert the child into a shared node
    pub fn add_node(&mut self, node: SharedNode<M>) -> Child<M> {
        self.nodes.push(node.clone());
        let offset: u64 = self
            .nodes
            .len()
            .try_into()
            .expect("TODO usize to seq (u64)");
        Child::new(self.seq, offset, Some(node))
    }

    /// Should only be used when [`Hyperbee::del`] causes a dangling root
    pub fn overwrite_root(&mut self, root: SharedNode<M>) -> Child<M> {
        self.root = Some(root.clone());
        Child::new(self.seq, 0, Some(root))
    }

    /// Add changed root
    pub fn add_root(&mut self, root: SharedNode<M>) -> Child<M> {
        if self.root.is_some() {
            panic!("We should never be replacing a root on a changes");
        }
        self.overwrite_root(root)
    }

    /// adds a changed node and handles when the node should be used as the root
    pub fn add_changed_node(&mut self, path_len: usize, node: SharedNode<M>) -> Child<M> {
        if path_len == 0 {
            self.add_root(node)
        } else {
            self.add_node(node)
        }
    }
}
