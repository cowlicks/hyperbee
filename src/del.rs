use crate::{
    changes::Changes, keys::InfiniteKeys, min_keys, nearest_node, put::propagate_changes_up_tree,
    Child, CoreMem, HyperbeeError, KeyValue, NodePath, SharedNode, Tree, MAX_KEYS,
};

use Side::{Left, Right};

/// When deleting from a B-Tree, we might need to [`Side::merge`] or [`Side::rotate`] to
/// maintain the invariants of the tree. These have a "side" or handedness depending on which
/// way we rotate/merge. The methods on this impl help simplify that.
/// The left side is smaller than the right side
#[derive(Debug)]
enum Side {
    /// Toward smaller values
    Left,
    /// Toward bigger values.
    Right,
}
// TODO consider some trait on  node/children/keys to make the playing with entries easier

/// Glossary
/// father - parent node with a deficient child
/// deficient_index - index in the father of the deficient child
/// donor - the child of the father that donates to the deficint child
impl Side {
    /// This is just:
    /// match self { Right => index + 1, Left => index - 1 }
    /// but with bounds checking
    async fn get_donor_index<M: CoreMem>(
        &self,
        father: SharedNode<M>,
        deficient_index: usize,
    ) -> Option<usize> {
        match *self {
            Left => {
                if deficient_index == 0 {
                    None
                } else {
                    Some(deficient_index - 1)
                }
            }
            Right => {
                let donor_index = deficient_index + 1;
                let n_children = father.read().await.n_children().await;
                if donor_index >= n_children {
                    None
                } else {
                    Some(donor_index)
                }
            }
        }
    }

    /// In both rotation and merge we need a key from the father
    fn get_key_index(&self, deficient_index: usize) -> usize {
        match self {
            Right => deficient_index,
            Left => deficient_index - 1,
        }
    }

    async fn get_donor_key<M: CoreMem>(&self, donor: SharedNode<M>) -> KeyValue {
        match self {
            Right => donor.write().await.keys.remove(0),
            Left => donor
                .write()
                .await
                .keys
                .pop()
                .expect("keys should not be empty"),
        }
    }

    async fn get_donor_child<M: CoreMem>(&self, donor: SharedNode<M>) -> Option<Child<M>> {
        if donor.read().await.is_leaf().await {
            return None;
        }
        Some(match self {
            Right => donor.read().await.children.children.write().await.remove(0),
            Left => donor
                .read()
                .await
                .children
                .children
                .write()
                .await
                .pop()
                .expect("children is non empty, checked above"),
        })
    }

    async fn swap_donor_key_in_father<M: CoreMem>(
        &self,
        father: SharedNode<M>,
        deficient_index: usize,
        key: KeyValue,
    ) -> KeyValue {
        let key_index = self.get_key_index(deficient_index);
        father
            .write()
            .await
            .keys
            .splice(key_index..=key_index, vec![key])
            .collect::<Vec<KeyValue>>()
            .pop()
            .expect("one val removed in splice")
    }

    async fn insert_donations_into_deficient_child<M: CoreMem>(
        &self,
        deficient_child: SharedNode<M>,
        key: KeyValue,
        child: Option<Child<M>>,
    ) {
        match self {
            Right => {
                deficient_child.write().await.keys.push(key);
                if let Some(child) = child {
                    deficient_child
                        .read()
                        .await
                        .children
                        .children
                        .write()
                        .await
                        .push(child);
                }
            }
            Left => {
                deficient_child.write().await.keys.insert(0, key);
                if let Some(child) = child {
                    deficient_child
                        .read()
                        .await
                        .children
                        .children
                        .write()
                        .await
                        .insert(0, child);
                }
            }
        }
    }

    async fn can_rotate<M: CoreMem>(
        &self,
        father: SharedNode<M>,
        deficient_index: usize,
        order: usize,
    ) -> Result<Option<usize>, HyperbeeError> {
        let Some(donor_index) = self.get_donor_index(father.clone(), deficient_index).await else {
            return Ok(None);
        };

        let can = min_keys(order)
            < (father
                .read()
                .await
                .get_child(donor_index)
                .await?
                .read()
                .await
                .keys
                .len());
        if can {
            Ok(Some(donor_index))
        } else {
            Ok(None)
        }
    }

    async fn rotate<M: CoreMem>(
        &self,
        father: SharedNode<M>,
        deficient_index: usize,
        donor_index: usize,
        changes: &mut Changes<M>,
    ) -> Result<SharedNode<M>, HyperbeeError> {
        let donor = father.read().await.get_child(donor_index).await?;
        // pull donated parts out of donor
        let donated_key = self.get_donor_key(donor.clone()).await;
        let donated_child = self.get_donor_child(donor.clone()).await;
        // create new ref for changed donor
        let donor_ref = changes.add_node(donor.clone());
        // replace donor that changed in the father
        father.read().await.children.children.write().await[donor_index] = donor_ref;

        // swap donated_key in father to get key for deficient_child
        let donated_key_from_father = self
            .swap_donor_key_in_father(father.clone(), deficient_index, donated_key)
            .await;

        let deficient_child = father.read().await.get_child(deficient_index).await?;
        self.insert_donations_into_deficient_child(
            deficient_child.clone(),
            donated_key_from_father,
            donated_child,
        )
        .await;

        let newly_non_deficient_child_ref = changes.add_node(deficient_child.clone());

        father.read().await.children.children.write().await[deficient_index] =
            newly_non_deficient_child_ref;

        Ok(father)
    }

    async fn maybe_rotate<M: CoreMem>(
        &self,
        father: SharedNode<M>,
        deficient_index: usize,
        order: usize,
        changes: &mut Changes<M>,
    ) -> Result<Option<SharedNode<M>>, HyperbeeError> {
        let Some(donor_index) = self
            .can_rotate(father.clone(), deficient_index, order)
            .await?
        else {
            return Ok(None);
        };
        Ok(Some(
            self.rotate(father, deficient_index, donor_index, changes)
                .await?,
        ))
    }

    #[tracing::instrument(skip(self, father, changes))]
    async fn maybe_merge<M: CoreMem>(
        &self,
        father: SharedNode<M>,
        deficient_index: usize,
        changes: &mut Changes<M>,
    ) -> Result<Option<SharedNode<M>>, HyperbeeError> {
        let Some(donor_index) = self.get_donor_index(father.clone(), deficient_index).await else {
            return Ok(None);
        };

        // Get donor child an deficient child  ordered from lowest to highest.
        // Left lower, right higher
        let (left, right) = {
            let donor_child = father.read().await.get_child(donor_index).await?;
            let deficient_child = father.read().await.get_child(deficient_index).await?;
            match self {
                Right => (deficient_child, donor_child),
                Left => (donor_child, deficient_child),
            }
        };

        let key_index = self.get_key_index(deficient_index);
        let donated_key_from_father = father.write().await.keys.remove(key_index);

        // remove RHS child from father
        let right_child_index = match self {
            Right => deficient_index + 1,
            Left => deficient_index,
        };
        father
            .read()
            .await
            .children
            .splice(right_child_index..=right_child_index, vec![])
            .await;

        // Move donated key from father and RHS keys into LHS
        let n_left_keys = left.read().await.keys.len();
        let mut keys_to_add = vec![donated_key_from_father];
        keys_to_add.append(&mut right.write().await.keys);
        left.write()
            .await
            .keys
            .splice(n_left_keys..n_left_keys, keys_to_add);
        // Move RHS children into LHS
        let n_left_children = left.read().await.n_children().await;
        left.write()
            .await
            .children
            .splice(
                n_left_children..n_left_children,
                right.read().await.children.children.write().await.drain(..),
            )
            .await;
        // Replace new LHS child in the father
        let left_ref = changes.add_node(left.clone());
        father
            .read()
            .await
            .children
            .splice((right_child_index - 1)..(right_child_index), vec![left_ref])
            .await;
        Ok(Some(father))
    }
}

#[tracing::instrument(skip(father, changes))]
/// The orering of the kinds of repairs here is choosen to match the Hyperbee-js implementation
async fn repair_one<M: CoreMem>(
    father: SharedNode<M>,
    deficient_index: usize,
    order: usize,
    changes: &mut Changes<M>,
) -> Result<SharedNode<M>, HyperbeeError> {
    if let Some(res) = Right
        .maybe_rotate(father.clone(), deficient_index, order, changes)
        .await?
    {
        return Ok(res);
    }
    if let Some(res) = Left
        .maybe_rotate(father.clone(), deficient_index, order, changes)
        .await?
    {
        return Ok(res);
    }
    if let Some(res) = Right
        .maybe_merge(father.clone(), deficient_index, changes)
        .await?
    {
        return Ok(res);
    }
    if let Some(res) = Left
        .maybe_merge(father.clone(), deficient_index, changes)
        .await?
    {
        return Ok(res);
    }
    panic!("this should never happen");
}
/// must be given a path who's last element has a deficient child...
#[tracing::instrument(skip(path, changes))]
async fn repair<M: CoreMem>(
    path: &mut NodePath<M>,
    order: usize,
    changes: &mut Changes<M>,
) -> Result<Child<M>, HyperbeeError> {
    let father_ref = loop {
        // next item, should be checked that it needs repair before
        let (father, deficient_index) =
            path.pop().expect("path.len() > 0 should be checked before");

        // Do repair
        let cur_father = repair_one(father, deficient_index, order, changes).await?;
        // if root empty use child
        if path.is_empty()
            && cur_father.read().await.keys.is_empty()
            && cur_father.read().await.n_children().await == 1
        {
            let new_root = cur_father.read().await.get_child(0).await?;
            break changes.add_root(new_root);
        }

        // store updated father
        let father_ref = changes.add_changed_node(path.len(), cur_father.clone());

        // if no more nodes, or father does not need repair, we are done
        if path.is_empty() || cur_father.read().await.keys.len() >= min_keys(MAX_KEYS) {
            break father_ref;
        }

        // add father_ref to next node in path and continue repair up tree
        let (grandpa, cur_deficient_index) = path
            .pop()
            .expect("path.is_empty branch above checks this exists");

        grandpa.read().await.children.children.write().await[cur_deficient_index] =
            father_ref.clone();
        path.push((grandpa, cur_deficient_index));
    };

    Ok(father_ref)
}

fn cas_always_true(_key: &[u8], _seq: u64, _val: &Option<Vec<u8>>) -> bool {
    true
}

impl<M: CoreMem> Tree<M> {
    pub async fn del_compare_and_swap(
        &self,
        key: &[u8],
        cas: impl FnOnce(&[u8], u64, &Option<Vec<u8>>) -> bool,
    ) -> Result<Option<bool>, HyperbeeError> {
        let Some(root) = self.get_root(false).await? else {
            return Ok(None);
        };

        let (matched, mut path) = nearest_node(root.clone(), key).await?;

        if matched.is_none() {
            return Ok(None);
        }

        // run user provided `cas` function
        {
            let len = path.len();
            let (node, index) = &mut path[len - 1];
            let kv = node.write().await.get_key_value(*index, true, true).await?;
            let result = cas(
                &kv.cached_key.expect("pulled in get_key_value"),
                kv.seq,
                &kv.cached_value.expect("pulled in get_key_value"),
            );
            if !result {
                // abort
                return Ok(Some(false));
            }
        }
        // NB: jS hyperbee stores the "key" the deleted "key" in the created BlockEntry. So we are
        // doing that too
        let mut changes: Changes<M> = Changes::new(self.version().await, key, None);

        let (cur_node, cur_index) = path
            .pop()
            .expect("nearest_node always returns at least one node");

        // remove the key from the node
        cur_node.write().await.keys.remove(cur_index);
        // handle leaf nodes
        let child = if cur_node.read().await.is_leaf().await {
            let child_ref = changes.add_changed_node(path.len(), cur_node.clone());
            path.push((cur_node.clone(), cur_index));
            child_ref
        // handle internal nodes
        } else {
            // We replace the deleted key with the largest key that is smaller than the deleted
            // value.
            let left_sub_tree = cur_node.read().await.get_child(cur_index).await?;
            let (_, mut left_path) =
                nearest_node(left_sub_tree.clone(), &InfiniteKeys::Positive).await?;

            let (bottom, bottom_index) = left_path
                .pop()
                .expect("There is always at least one node returned by `nearest_node`");
            let replacement_key = bottom
                .write()
                .await
                .keys
                .pop()
                .expect("The only possible node with zero keys is an empty root, which is not an internal node so we wouldn't be in this block.");

            // insert the replacement key into where the deleted key was
            cur_node
                .write()
                .await
                .keys
                .insert(cur_index, replacement_key);

            // now the bottom node where the replacement key came from could be deficient
            // so we add the path to the replacement key to our original `path` so we can repair
            // the tree if necessary
            path.push((cur_node.clone(), cur_index));
            path.append(&mut left_path);
            path.push((bottom.clone(), bottom_index));
            changes.add_node(bottom.clone())
        };

        let (bottom_node, _) = path.pop().expect("if/else above ensures path is not empty");
        // if node is not root and is deficient do repair. This repairs all deficient nodes in the
        // path
        let child = if !path.is_empty() && bottom_node.read().await.keys.len() < min_keys(MAX_KEYS)
        {
            repair(&mut path, MAX_KEYS, &mut changes).await?
        } else {
            child
        };

        // if not root propagate changes
        let mut changes = if path.is_empty() {
            changes
        } else {
            propagate_changes_up_tree(changes, path, child).await
        };

        let root = root.read().await;
        // If we removed all keys from the root but it still has a child, make the child the root
        if root.keys.is_empty() && root.n_children().await == 1 {
            changes.overwrite_root(root.get_child(0).await?);
        }

        self.blocks.read().await.add_changes(changes).await?;
        Ok(Some(true))
    }

    pub async fn del(&self, key: &[u8]) -> Result<bool, HyperbeeError> {
        Ok(self
            .del_compare_and_swap(key, cas_always_true)
            .await?
            .is_some())
    }
}

#[cfg(test)]
/// There are a few checks we (should) do for each unit test here
/// * Check the `del`'d element is actually gone with ~hb.get
/// * Run check_tree after del to make sure tree still has all invariants
/// * TODO Check expected final shape of the entire tree. Probably with hb.print() == expected
/// * TODO Compare structure with hyperbees-js (do this in integration tests
///
/// rough enumeration of possible test cases
///
/// key exists y/n
/// underflow y/n
/// deleted from: leaf, internal, root
/// how far up tree repairs are needed: once, some, all
/// kind of repair: rotate, merge
/// repair direction: left, right
///
/// Basic cases:
/// key does not exist.
/// key exists but no underflow with key in: root, internal, leaf
///
/// The rest of the cases multiply:
///
/// for x of (leaf, internal, root) do
///   for y of (once, some, all) do
///       for z of (rigt, left) do
///           for a of (merge, rotate) do
///               exists underflow x y z a
///
/// This gives 40 tests
/// This does account for different cascading combinations of repair up the tree.
///
mod test {
    use crate::{
        test::{check_tree, i32_key_vec, Rand},
        Tree,
    };

    #[tokio::test]
    async fn empty_tree_no_key() -> Result<(), Box<dyn std::error::Error>> {
        let hb = Tree::from_ram().await?;
        let key = vec![1];
        let res = hb.del(&key).await?;
        assert!(!res);
        Ok(())
    }

    #[tokio::test]
    async fn no_key() -> Result<(), Box<dyn std::error::Error>> {
        let (hb, ..) = crate::test::hb_put!(0..10).await?;
        let key = vec![1];
        let res = hb.del(&key).await?;
        assert!(!res);
        Ok(())
    }

    #[tokio::test]
    async fn delete_from_root_that_is_leaf() -> Result<(), Box<dyn std::error::Error>> {
        let (hb, keys) = crate::test::hb_put!(0..4).await?;
        let k = &keys[0].clone();
        let res = hb.del(k).await?;
        assert!(res);

        let res = hb.get(k).await?;
        assert_eq!(res, None);

        let res = hb.get(&keys[1].clone()).await?;
        assert!(res.is_some());
        check_tree(hb).await?;
        Ok(())
    }

    #[tokio::test]
    async fn delete_from_leaf_no_underflow() -> Result<(), Box<dyn std::error::Error>> {
        let (hb, keys) = crate::test::hb_put!(0..10).await?;
        let k = &keys.last().unwrap().clone();
        let res = hb.del(k).await?;
        assert!(res);
        let res = hb.get(k).await?;
        assert_eq!(res, None);
        check_tree(hb).await?;
        Ok(())
    }

    #[tokio::test]
    async fn delete_last_key() -> Result<(), Box<dyn std::error::Error>> {
        let (hb, keys) = crate::test::hb_put!(0..1).await?;
        let k = &keys.last().unwrap().clone();
        let res = hb.del(k).await?;
        assert!(res);
        let res = hb.get(k).await?;
        assert_eq!(res, None);
        check_tree(hb).await?;
        Ok(())
    }

    #[tokio::test]
    async fn delete_from_leaf_with_underflow_rotate_left() -> Result<(), Box<dyn std::error::Error>>
    {
        let (hb, keys) = crate::test::hb_put!(0..6).await?;
        let k = keys[0].clone();
        let res = hb.del(&k).await?;
        assert!(res);
        let res = hb.get(&k).await?;
        assert_eq!(res, None);
        check_tree(hb).await?;
        Ok(())
    }

    #[tokio::test]
    async fn delete_from_leaf_with_underflow_rotate_right() -> Result<(), Box<dyn std::error::Error>>
    {
        let (hb, keys) = crate::test::hb_put!(&[1, 2, 3, 4, 5, 0]).await?;
        let k = keys[keys.len() - 2].clone();
        let res = hb.del(&k).await?;
        assert!(res);
        let res = hb.get(&k).await?;
        assert_eq!(res, None);
        check_tree(hb).await?;
        Ok(())
    }

    #[tokio::test]
    async fn delete_from_leaf_with_underflow_merge_left() -> Result<(), Box<dyn std::error::Error>>
    {
        let (hb, keys) = crate::test::hb_put!(0..5).await?;
        let k = keys[0].clone();
        let res = hb.del(&k).await?;
        assert!(res);
        let res = hb.get(&k).await?;
        assert_eq!(res, None);
        check_tree(hb).await?;
        Ok(())
    }

    #[tokio::test]
    async fn delete_from_internal_no_underflow() -> Result<(), Box<dyn std::error::Error>> {
        let (hb, keys) = crate::test::hb_put!(0..19).await?;
        let k = keys[5].clone();
        let res = hb.del(&k).await?;
        assert!(res);
        let res = hb.get(&k).await?;
        assert_eq!(res, None);
        check_tree(hb).await?;
        Ok(())
    }

    #[tokio::test]
    async fn delete_from_internal_node_with_underflow_merge(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (hb, keys) = crate::test::hb_put!(0..19).await?;
        let k = keys[10].clone();
        let res = hb.del(&k).await?;
        assert!(res);
        let res = hb.get(&k).await?;
        assert_eq!(res, None);
        check_tree(hb).await?;
        Ok(())
    }

    #[tokio::test]
    async fn delete_from_internal_node_with_underflow_rotate(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (hb, keys) = crate::test::hb_put!(0..25).await?;
        let k = keys[10].clone();
        let res = hb.del(&k).await?;
        assert!(res);
        let res = hb.get(&k).await?;
        assert_eq!(res, None);
        check_tree(hb).await?;
        Ok(())
    }

    #[tokio::test]
    async fn bug_where_root_was_not_getting_replaced() -> Result<(), Box<dyn std::error::Error>> {
        let (hb, keys) = crate::test::hb_put!(0..5).await?;
        for k in keys.iter() {
            hb.del(k).await?;
            let res = hb.get(k).await?;
            assert_eq!(res, None);
            check_tree(hb.clone()).await?;
        }
        Ok(())
    }

    /// Instead of enumerating all possible cases we try a bunch of random cases, which checking
    /// for deletion and that the tree maintains all invariants.
    #[tokio::test]
    async fn rand_delete() -> Result<(), Box<dyn std::error::Error>> {
        let rand = Rand::default();
        let hb = Tree::from_ram().await?;

        let keys: Vec<Vec<u8>> = (0..100).map(i32_key_vec).collect();
        let keys = rand.shuffle(keys);

        for k in keys.iter() {
            let val: Option<&[u8]> = Some(k);
            hb.put(k, val).await?;
        }

        for k in rand.shuffle(keys).iter() {
            hb.del(k).await?;
            let res = hb.get(k).await?;
            assert_eq!(res, None);
            check_tree(hb.clone()).await?;
        }
        Ok(())
    }
    use super::cas_always_true;

    #[tokio::test]
    async fn test_del_compare_and_swap() -> Result<(), Box<dyn std::error::Error>> {
        let (hb, ..) = crate::test::hb_put!(0..5).await?;
        // key not presest
        let del_res = hb.del_compare_and_swap(b"foobar", cas_always_true).await?;
        assert_eq!(del_res, None);

        let k = i32_key_vec(0);
        // cas is false
        let del_res = hb
            .del_compare_and_swap(&k, |key: &[u8], _seq: u64, _val: &Option<Vec<u8>>| key != k)
            .await?;
        assert_eq!(del_res, Some(false));
        let get_res = hb.get(&k).await?;
        assert!(get_res.is_some());

        // cas is true
        let del_res = hb.del_compare_and_swap(&k, cas_always_true).await?;
        assert_eq!(del_res, Some(true));

        let get_res = hb.get(&k).await?;
        assert!(get_res.is_none());
        Ok(())
    }
}
