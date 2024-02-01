use crate::{
    nearest_node,
    put::{propagate_changes_up_tree, Changes},
    ChildWithCache, CoreMem, Hyperbee, HyperbeeError, InfiniteKeys, Key, Node, SharedNode,
    MAX_KEYS,
};

/// When deleting from a B-Tree, we might need to [`Side::merge`] or [`Side::rotate`] to
/// maintain the invariants of the tree. These have a "side" or handedness depending on which
/// way we rotate/merge. The methods on this impl help simplify that.
enum Side {
    Left,
    Right,
}

use Side::{Left, Right};
// TODO consider some trait on  node/children/keys to make the playing with entries easier

/// Glossary
/// father - parent node with a deficient child
/// deficient_index - index in the father of the deficient child
/// donor - the child of the father that donates to the deficint child
impl Side {
    const fn val(&self) -> isize {
        match *self {
            Right => 1,
            Left => -1,
        }
    }

    // TODO result not needed
    async fn get_donor_index<M: CoreMem>(
        &self,
        father: SharedNode<M>,
        deficient_index: usize,
    ) -> Result<Option<usize>, HyperbeeError> {
        let donor_index = deficient_index as isize + self.val();
        if donor_index < 0 || ((donor_index as usize) >= father.read().await.n_children().await) {
            return Ok(None);
        }
        Ok(Some(donor_index as usize))
    }

    /// In both rotation and merge we need a key from the father
    fn get_key_index(&self, deficient_index: usize) -> usize {
        match self {
            Right => deficient_index,
            Left => deficient_index - 1,
        }
    }

    async fn get_donor_key<M: CoreMem>(&self, donor: SharedNode<M>) -> Key {
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

    async fn get_donor_child<M: CoreMem>(&self, donor: SharedNode<M>) -> Option<ChildWithCache<M>> {
        if donor.read().await.n_children().await == 0 {
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
        key: Key,
    ) -> Key {
        let key_index = self.get_key_index(deficient_index);
        father
            .write()
            .await
            .keys
            .splice(key_index..(key_index + 1), vec![key])
            .collect::<Vec<Key>>()
            .pop()
            .expect("one val removed in splice")
    }

    async fn insert_donations_into_deficient_child<M: CoreMem>(
        &self,
        deficient_child: SharedNode<M>,
        key: Key,
        child: Option<ChildWithCache<M>>,
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
    ) -> Result<bool, HyperbeeError> {
        let donor_index = match self
            .get_donor_index(father.clone(), deficient_index)
            .await?
        {
            None => return Ok(false),
            Some(i) => i,
        };

        Ok((order >> 1)
            < father
                .read()
                .await
                .get_child(donor_index)
                .await?
                .read()
                .await
                .n_keys()
                .await)
    }

    async fn rotate<M: CoreMem>(
        &self,
        father: SharedNode<M>,
        deficient_index: usize,
        changes: &mut Changes<M>,
    ) -> Result<SharedNode<M>, HyperbeeError> {
        let donor_index = self
            .get_donor_index(father.clone(), deficient_index)
            .await?
            .expect("should already be checked");

        let donor = father.read().await.get_child(donor_index).await?;
        // pull donated parts out of donor
        let donated_key = self.get_donor_key(donor.clone()).await;
        let donated_child = self.get_donor_child(donor.clone()).await;
        // create new ref for changed donor
        let donor_ref = changes.add_node(donor.clone());
        // replace donor that changed in the father
        father.read().await.children.children.write().await[donor_index] = (donor_ref, Some(donor));

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
            (newly_non_deficient_child_ref, Some(deficient_child));

        Ok(father)
    }
    async fn can_merge<M: CoreMem>(&self, father: SharedNode<M>, deficient_index: usize) -> bool {
        let n_children = father.read().await.n_children().await;
        match self {
            Right => deficient_index + 1 < n_children,
            Left => deficient_index > 0,
        }
    }

    async fn merge<M: CoreMem>(
        &self,
        father: SharedNode<M>,
        deficient_index: usize,
        changes: &mut Changes<M>,
    ) -> Result<SharedNode<M>, HyperbeeError> {
        let donor_index = self
            .get_donor_index(father.clone(), deficient_index)
            .await?
            .expect("should already be checked");

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
            .splice(right_child_index..(right_child_index + 1), vec![])
            .await;

        // Move donated key from father and RHS keys into LHS
        let n_left_keys = left.read().await.n_keys().await;
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
        let left_ref = (changes.add_node(left.clone()), Some(left));
        father
            .read()
            .await
            .children
            .splice((right_child_index - 1)..(right_child_index), vec![left_ref])
            .await;
        Ok(father)
    }
}

async fn repair_one<M: CoreMem>(
    father: SharedNode<M>,
    deficient_index: usize,
    order: usize,
    changes: &mut Changes<M>,
) -> Result<SharedNode<M>, HyperbeeError> {
    if Right
        .can_rotate(father.clone(), deficient_index, order)
        .await?
    {
        return Right.rotate(father.clone(), deficient_index, changes).await;
    }
    if Left
        .can_rotate(father.clone(), deficient_index, order)
        .await?
    {
        return Left.rotate(father.clone(), deficient_index, changes).await;
    }
    if Right.can_merge(father.clone(), deficient_index).await {
        return Right.merge(father.clone(), deficient_index, changes).await;
    }
    if Left.can_merge(father.clone(), deficient_index).await {
        return Right.merge(father.clone(), deficient_index, changes).await;
    }
    panic!("this should never happen");
}
/// must be given a path who's last element has a deficient child...
async fn repair<M: CoreMem>(
    path: &mut Vec<(SharedNode<M>, usize)>,
    order: usize,
    changes: &mut Changes<M>,
) -> Result<ChildWithCache<M>, HyperbeeError> {
    let father_ref = loop {
        let (father, deficient_index) =
            path.pop().expect("path.len() > 0 should be checked before");

        let cur_father = repair_one(father, deficient_index, order, changes).await?;
        // if root empty use child
        if path.is_empty()
            && cur_father.read().await.n_keys().await == 0
            && cur_father.read().await.n_children().await == 1
        {
            let new_root = cur_father.read().await.get_child(0).await?;
            break (changes.add_root(new_root.clone()), Some(new_root));
        }

        // store updated father
        let father_ref = match path.is_empty() {
            true => (
                changes.add_root(cur_father.clone()),
                Some(cur_father.clone()),
            ),
            false => (
                changes.add_node(cur_father.clone()),
                Some(cur_father.clone()),
            ),
        };

        // if no more nodes, or father does not need repair, we are done
        if path.is_empty() || cur_father.read().await.n_keys().await >= MAX_KEYS >> 1 {
            break father_ref;
        }

        // add father_ref to next node in path and continue repair up tree
        let (grandpa, cur_deficient_index) = path.pop().unwrap();
        grandpa.read().await.children.children.write().await[cur_deficient_index + 1] =
            father_ref.clone();
        path.push((grandpa, cur_deficient_index));
    };

    return Ok(father_ref);
}

impl<M: CoreMem> Hyperbee<M> {
    pub async fn del(&mut self, key: &[u8]) -> Result<bool, HyperbeeError> {
        let root = match self.get_root(false).await? {
            Some(r) => r,
            None => return Ok(false),
        };

        //let (matched, mut node_path, mut index_path) = nearest_node(root, key).await?;
        let (matched, mut path) = nearest_node(root, key).await?;

        if !matched {
            return Ok(false);
        }

        // NB: jS hyperbee stores the "key" the deleted "key" in the created BlockEntry
        let mut changes: Changes<M> =
            Changes::new(self.version().await, key.clone().to_vec(), None);

        let (cur_node, cur_index) = path.pop().unwrap();

        // remove the key from the node
        cur_node.write().await.remove_key(cur_index).await;
        let child = if cur_node.read().await.is_leaf().await {
            let child_ref = if path.is_empty() {
                changes.add_root(cur_node.clone())
            } else {
                changes.add_node(cur_node.clone())
            };
            path.push((cur_node.clone(), cur_index));
            (child_ref, Some(cur_node.clone()))
        } else {
            todo!()
        };

        let (bottom_node, _) = path.pop().expect("if/else above ensures path is not empty");
        // if node is not root and is deficient
        let child = if !path.is_empty() && bottom_node.read().await.keys.len() < MAX_KEYS >> 1 {
            repair(&mut path, MAX_KEYS, &mut changes).await?
        } else {
            child
        };

        // if not root propagate changes
        let changes = if !path.is_empty() {
            propagate_changes_up_tree(changes, path, child).await
        } else {
            changes
        };

        self.blocks.read().await.add_changes(changes).await?;
        Ok(true)
    }
}

impl<M: CoreMem> Node<M> {
    async fn remove_key(&mut self, index: usize) -> Key {
        self.keys.remove(index)
    }
    async fn is_leaf(&self) -> bool {
        self.n_children().await == 0
    }
}

#[cfg(test)]
/// There are a few checks we (should) do for each unit test here
/// * Check the `del`'d element is actually gone with ~hb.get
/// * Run check_tree after del to make sure tree still has all invariants
/// * Check expected final shape of the entire tree. Probably with hb.print() == expected
///
/// The last one is kind of annoying. I could compare with data generated from js hb. Or maybe my
/// own appendtree code
///
/// Do all these in right and left:
/// DONE check rotate that effects root in leaf
/// TODO check rotate that does not effect root in leaf
///
/// TODO check rotate that effects root in non leaf
/// TODO check rotate that does not effect root in non leaf
///
/// Merge can also cause cascading merges. Test variations of that too.
/// TODO check merge that effects root in leaf
/// TODO check merge that does not effect root in leaf
///
/// TODO check merge that effects root in non leaf
/// TODO check merge that does not effect root in non leaf
///
mod test {
    use crate::test::{check_tree, in_memory_hyperbee};

    #[tokio::test]
    async fn empty_tree_no_key() -> Result<(), Box<dyn std::error::Error>> {
        let mut hb = in_memory_hyperbee().await?;
        let key = vec![1];
        let res = hb.del(&key).await?;
        assert!(!res);
        Ok(())
    }

    #[tokio::test]
    async fn no_key() -> Result<(), Box<dyn std::error::Error>> {
        let (mut hb, ..) = crate::test::hb_put!(0..10).await?;
        let key = vec![1];
        let res = hb.del(&key).await?;
        assert!(!res);
        Ok(())
    }

    #[tokio::test]
    async fn delete_from_root_that_is_leaf() -> Result<(), Box<dyn std::error::Error>> {
        let (mut hb, keys) = crate::test::hb_put!(0..4).await?;
        let k = &keys[0].clone();
        let res = hb.del(&k).await?;
        assert!(res);

        let res = hb.get(&k).await?;
        assert_eq!(res, None);

        let res = hb.get(&keys[1].clone()).await?;
        assert!(res.is_some());
        check_tree(hb).await?;
        Ok(())
    }

    #[tokio::test]
    async fn delete_from_leaf_no_underflow() -> Result<(), Box<dyn std::error::Error>> {
        let (mut hb, keys) = crate::test::hb_put!(0..10).await?;
        println!("{}", hb.print().await?);
        let k = &keys.last().unwrap().clone();
        let res = hb.del(&k).await?;
        assert!(res);
        let res = hb.get(&k).await?;
        assert_eq!(res, None);
        println!("{}", hb.print().await?);
        check_tree(hb).await?;
        Ok(())
    }

    #[tokio::test]
    async fn delete_last_key() -> Result<(), Box<dyn std::error::Error>> {
        let (mut hb, keys) = crate::test::hb_put!(0..1).await?;
        println!("{}", hb.print().await?);
        let k = &keys.last().unwrap().clone();
        let res = hb.del(&k).await?;
        assert!(res);
        let res = hb.get(&k).await?;
        assert_eq!(res, None);
        check_tree(hb).await?;
        Ok(())
    }

    #[tokio::test]
    async fn delete_from_leaf_with_underflow_rotate_left() -> Result<(), Box<dyn std::error::Error>>
    {
        let (mut hb, keys) = crate::test::hb_put!(0..6).await?;
        let k = keys[0].clone();
        println!("BEFORE {}", hb.print().await?);
        let res = hb.del(&k).await?;
        assert!(res);
        let res = hb.get(&k).await?;
        assert_eq!(res, None);
        println!("AFTER {}", hb.print().await?);
        check_tree(hb).await?;
        Ok(())
    }

    #[tokio::test]
    async fn delete_from_leaf_with_underflow_rotate_right() -> Result<(), Box<dyn std::error::Error>>
    {
        let (mut hb, keys) = crate::test::hb_put!(vec![1, 2, 3, 4, 5, 0]).await?;
        let k = keys[keys.len() - 2].clone();
        println!("BEFORE {}", hb.print().await?);
        let res = hb.del(&k).await?;
        assert!(res);
        let res = hb.get(&k).await?;
        assert_eq!(res, None);
        println!("AFTER {}", hb.print().await?);
        check_tree(hb).await?;
        Ok(())
    }

    #[tokio::test]
    async fn delete_from_leaf_with_underflow_merge_left() -> Result<(), Box<dyn std::error::Error>>
    {
        let (mut hb, keys) = crate::test::hb_put!(0..5).await?;
        let k = keys[0].clone();
        println!("BEFORE {}", hb.print().await?);
        let res = hb.del(&k).await?;
        assert!(res);
        let res = hb.get(&k).await?;
        assert_eq!(res, None);
        println!("AFTER {}", hb.print().await?);
        check_tree(hb).await?;
        Ok(())
    }

    #[tokio::test]
    async fn delete_from_internal_no_underflow() -> Result<(), Box<dyn std::error::Error>> {
        let (mut hb, keys) = crate::test::hb_put!(0..19).await?;
        let k = keys[5].clone();
        println!("BEFORE {}", hb.print().await?);
        let res = hb.del(&k).await?;
        assert!(res);
        let res = hb.get(&k).await?;
        assert_eq!(res, None);
        println!("AFTER {}", hb.print().await?);
        check_tree(hb).await?;
        Ok(())
    }

    #[tokio::test]
    async fn delete_from_internal_node_with_underflow_merge_foo(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (mut hb, keys) = crate::test::hb_put!(0..19).await?;
        let k = keys[10].clone();
        println!("BEFORE\n{}", hb.print().await?);
        let res = hb.del(&k).await?;
        panic!();
        assert!(res);
        let res = hb.get(&k).await?;
        assert_eq!(res, None);
        println!("AFTER {}", hb.print().await?);
        check_tree(hb).await?;
        Ok(())
    }
}
