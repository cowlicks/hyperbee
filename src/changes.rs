use crate::{Child, SharedNode};

#[derive(Debug, Default)]
/// Structure to store in-progress changes to the [`Hyperbee`]
/// NB: because of how hyperbee-js works, we need to distinguish between root/non-root nodes.
pub(crate) struct Changes {
    seq: u64,
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
    pub nodes: Vec<SharedNode>,
    pub root: Option<SharedNode>,
}

impl Changes {
    pub fn new(seq: u64, key: &[u8], value: Option<&[u8]>) -> Self {
        Self {
            seq,
            key: key.to_vec(),
            value: value.map(<[u8]>::to_vec),
            nodes: vec![],
            root: None,
        }
    }

    /// Add a node that's changed. Returns the's stored node's reference
    pub fn add_node(&mut self, node: SharedNode) -> Child {
        self.nodes.push(node.clone());
        let offset: u64 = self
            .nodes
            .len()
            .try_into()
            .expect("this would happen when sizeof(usize) < sizeof(u64), lkey on 32bit. And when the offset (which is on the order of the height of the tree) is greater than usize::MAX. Well that would be crazy. We should Probably have a check for usize >= u64 on startup... or something... TODO");
        Child::new(self.seq, offset, Some(node))
    }

    /// Should only be used when [`Hyperbee::del`] causes a dangling root
    pub fn overwrite_root(&mut self, root: SharedNode) -> Child {
        self.root = Some(root.clone());
        Child::new(self.seq, 0, Some(root))
    }

    /// Add changed root
    pub fn add_root(&mut self, root: SharedNode) -> Child {
        if self.root.is_some() {
            panic!("We should never be replacing a root on a changes");
        }
        self.overwrite_root(root)
    }

    /// adds a changed node and handles when the node should be used as the root
    pub fn add_changed_node(&mut self, path_len: usize, node: SharedNode) -> Child {
        if path_len == 0 {
            self.add_root(node)
        } else {
            self.add_node(node)
        }
    }
}
