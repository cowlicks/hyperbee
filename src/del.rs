use crate::{
    nearest_node,
    put::{propagate_changes_up_tree, Changes},
    ChildWithCache, CoreMem, Hyperbee, HyperbeeError, Key, Node, SharedNode, MAX_KEYS,
};

enum Side {
    Left,
    Right,
}

use Side::{Left, Right};

impl Side {
    const fn val(&self) -> isize {
        match *self {
            Right => 1,
            Left => -1,
        }
    }

    async fn get_donor_index<M: CoreMem>(
        &self,
        father: SharedNode<M>,
        deficient_index: usize,
    ) -> Result<Option<usize>, HyperbeeError> {
        let donor_index = deficient_index as isize + self.val();
        if donor_index < 0 || (!((donor_index as usize) < father.read().await.n_children().await)) {
            return Ok(None);
        }
        Ok(Some(donor_index as usize))
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
        let key_index = match self {
            Right => deficient_index,
            Left => deficient_index - 1,
        };
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

        return Ok((order >> 1)
            < father
                .read()
                .await
                .get_child(donor_index)
                .await?
                .read()
                .await
                .n_keys()
                .await);
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
}

async fn repair<M: CoreMem>(
    node_path: &mut Vec<SharedNode<M>>,
    index_path: &mut Vec<usize>,
    order: usize,
    changes: &mut Changes<M>,
) -> Result<SharedNode<M>, HyperbeeError> {
    let father = node_path
        .pop()
        .expect("node_path.len() > 0 should be checked before");
    let deficient_index = index_path
        .pop()
        .expect("node_path.len() > 0 should be checked before");
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
    todo!()
}

impl<M: CoreMem> Hyperbee<M> {
    pub async fn del(&mut self, key: &[u8]) -> Result<bool, HyperbeeError> {
        let root = match self.get_root(false).await? {
            Some(r) => r,
            None => return Ok(false),
        };

        let (matched, mut node_path, mut index_path) = nearest_node(root, key).await?;

        if !matched {
            return Ok(false);
        }

        // NB: jS hyperbee stores the "key" the deleted "key" in the created BlockEntry
        let mut changes: Changes<M> =
            Changes::new(self.version().await, key.clone().to_vec(), None);

        // remove the key from the node
        let cur_node = match node_path.pop() {
            Some(node) => node,
            None => todo!(),
        };
        let cur_index = index_path
            .pop()
            .expect("node_path and index_path *should* always have the same length");

        cur_node.write().await.remove_key(cur_index).await;
        let child = if cur_node.read().await.is_leaf().await {
            if node_path.len() == 0 {
                changes.add_root(cur_node.clone())
            } else {
                changes.add_node(cur_node.clone())
            }
        } else {
            todo!()
        };

        // if node is not root and is deficient
        let child = if !node_path.is_empty() && cur_node.read().await.keys.len() < MAX_KEYS >> 1 {
            let repaired = repair(&mut node_path, &mut index_path, MAX_KEYS, &mut changes).await?;
            match node_path.is_empty() {
                true => changes.add_root(repaired),
                false => changes.add_node(repaired),
            }
        } else {
            child
        };

        // if not root propagate changes
        let changes = if !node_path.is_empty() {
            propagate_changes_up_tree(changes, node_path, index_path, child).await
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
}
