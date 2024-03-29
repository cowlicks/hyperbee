use crate::{Child, SharedNode};

#[derive(Debug, Default)]
/// Structure to store in-progress changes to the [`Hyperbee`]
/// NB: because of how hyperbee-js works, we need to distinguish between root/non-root nodes.
pub(crate) struct Changes {
    pub seq: u64,
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
    pub nodes: Vec<SharedNode>,
}

impl Changes {
    pub fn new(seq: u64, key: &[u8], value: Option<&[u8]>) -> Self {
        Self {
            seq,
            key: key.to_vec(),
            value: value.map(<[u8]>::to_vec),
            nodes: vec![],
        }
    }

    /// Add a node that's changed. Returns the's stored node's reference
    #[tracing::instrument(skip(self, node))]
    pub fn add_node(&mut self, node: SharedNode) -> Child {
        let offset: u64 = self
            .nodes
            .len()
            .try_into()
            .expect("this would happen when sizeof(usize) < sizeof(u64), lkey on 32bit. And when the offset (which is on the order of the height of the tree) is greater than usize::MAX. Well that would be crazy. We should Probably have a check for usize >= u64 on startup... or something... TODO");
        self.nodes.push(node.clone());
        Child::new(self.seq, offset, Some(node))
    }
}
