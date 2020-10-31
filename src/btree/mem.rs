enum InsertResult {
    SuccessNoSplit,
    Duplicate,
    SuccessSplit {
        split_key: u32,
        split_node: Box<dyn Node>,
    },
}

trait Node {
    fn find_leaf(&self, key: u32) -> Option<&Leaf>;
    fn insert(&mut self, key: u32, value: u32) -> InsertResult;

    fn count_nodes(&self) -> (usize, usize) {
        return (1, 0);
    }

    fn as_internal(&mut self) -> Option<&mut InternalNode> {
        return None;
    }
}

struct Leaf {
    kv: Vec<(u32, u32)>,
}

impl Node for Leaf {
    fn find_leaf(&self, key: u32) -> Option<&Leaf> {
        if !self.kv.is_empty() && key >= self.kv[0].1 && key <= self.kv[self.kv.len() - 1].1 {
            Option::Some(&self)
        } else {
            Option::None
        }
    }

    fn insert(&mut self, key: u32, value: u32) -> InsertResult {
        let search_result = self.kv.binary_search_by_key(&key, |value| value.1);
        match search_result {
            Ok(_) => InsertResult::Duplicate,
            Err(index) => {
                if self.kv.len() < self.kv.capacity() {
                    self.kv.insert(index, (key, value));
                    InsertResult::SuccessNoSplit
                } else {
                    let midpoint_index = self.kv.len() / 2;
                    let midpoint_value = self.kv[midpoint_index].1;

                    // Allocate new kv for split node, copying from the midpoint of this node's kv.
                    let mut split_kv = Vec::with_capacity(self.kv.capacity());
                    for i in midpoint_index..self.kv.len() {
                        split_kv.push(self.kv[i])
                    }

                    // Truncate this kv from the midpoint and push the new key and value.
                    self.kv.truncate(midpoint_index);
                    let mut split_leaf = Leaf { kv: split_kv };

                    if key < midpoint_value {
                        self.insert(key, value);
                    } else {
                        split_leaf.insert(key, value);
                    }

                    InsertResult::SuccessSplit {
                        split_key: midpoint_value,
                        split_node: Box::new(split_leaf),
                    }
                }
            }
        }
    }
}

impl Leaf {
    fn lookup(&self, key: u32) -> Option<u32> {
        self.kv
            .binary_search_by_key(&key, |value| value.1)
            .map(|idx| self.kv[idx].1)
            .ok()
    }
}

struct InternalNode {
    keys: Vec<u32>,
    pointers: Vec<Box<dyn Node>>,
}

impl InternalNode {
    fn index_for(&self, key: u32) -> usize {
        match self.keys.binary_search(&key) {
            Ok(index) => index + 1,
            Err(index) => index,
        }
    }

    fn insert_key_and_pointer(&mut self, key: u32, pointer: Box<dyn Node>) {
        let idx = self.index_for(key);
        self.keys.insert(idx, key);
        self.pointers.insert(idx + 1, pointer);
    }
}

impl Node for InternalNode {
    fn find_leaf(&self, key: u32) -> Option<&Leaf> {
        self.pointers[self.index_for(key)].find_leaf(key)
    }

    fn insert(&mut self, key: u32, value: u32) -> InsertResult {
        let insert_index = self.index_for(key);
        let result = self.pointers[insert_index].insert(key, value);
        match result {
            InsertResult::SuccessSplit {
                split_key,
                split_node,
            } => {
                if self.keys.len() < self.keys.capacity() {
                    self.insert_key_and_pointer(split_key, split_node);
                    InsertResult::SuccessNoSplit
                } else {
                    let midpoint_index = self.keys.len() / 2;
                    let midpoint_key = self.keys[midpoint_index];

                    // Allocate new kv for split node, copying from the midpoint of this node's kv.
                    let mut right_keys = Vec::with_capacity(self.keys.capacity());
                    let mut right_pointers = Vec::with_capacity(self.pointers.capacity());
                    for i in (midpoint_index + 1)..self.keys.len() {
                        right_keys.push(self.keys[i])
                    }
                    right_pointers.extend(
                        self.pointers
                            .drain((midpoint_index + 1)..self.pointers.len()),
                    );

                    self.keys.truncate(midpoint_index);
                    self.pointers.truncate(midpoint_index + 1);

                    let mut right_node = InternalNode {
                        keys: right_keys,
                        pointers: right_pointers,
                    };

                    if split_key < midpoint_key {
                        self.insert_key_and_pointer(split_key, split_node)
                    } else {
                        right_node.insert_key_and_pointer(split_key, split_node)
                    }

                    InsertResult::SuccessSplit {
                        split_key: midpoint_key,
                        split_node: Box::new(right_node),
                    }
                }
            }
            x => x,
        }
    }

    fn count_nodes(&self) -> (usize, usize) {
        let mut leaf_count = 0;
        let mut internal_count = 1;
        for child in self.pointers.iter() {
            let (inner_leaf_count, inner_internal_count) = child.count_nodes();
            leaf_count += inner_leaf_count;
            internal_count += inner_internal_count;
        }
        (leaf_count, internal_count)
    }

    fn as_internal(&mut self) -> Option<&mut InternalNode> {
        Some(self)
    }
}

pub struct BTree {
    capacity: usize,
    root: Option<Box<dyn Node>>,
}

impl BTree {
    pub fn new(capacity: usize) -> BTree {
        BTree {
            capacity,
            root: Some(Box::new(Leaf {
                kv: Vec::with_capacity(capacity),
            })),
        }
    }

    pub fn count_nodes(&self) -> (usize, usize) {
        self.root.as_ref().map_or((0, 0), |root| root.count_nodes())
    }

    pub fn lookup(&self, key: u32) -> Option<u32> {
        self.root
            .as_ref()
            .and_then(|root| root.find_leaf(key).and_then(|leaf| leaf.lookup(key)))
    }

    // pub fn lookup_range<'a>(&self, from_key: u32, to_key: u32) -> &'a dyn Iterator<Item = u32> {
    //     panic!("Not implemented")
    // }

    pub fn insert(&mut self, key: u32, value: u32) -> bool {
        let result = self.root.as_mut().map(|root| root.insert(key, value));
        match result {
            Some(InsertResult::SuccessNoSplit) => true,
            Some(InsertResult::SuccessSplit {
                split_key: midpoint_key,
                split_node,
            }) => {
                if let Some(old_root) = self.root.take() {
                    let mut new_root = InternalNode {
                        keys: Vec::with_capacity(self.capacity),
                        pointers: Vec::with_capacity(self.capacity + 1),
                    };
                    new_root.keys.push(midpoint_key);
                    new_root.pointers.push(old_root);
                    new_root.pointers.push(split_node);
                    self.root.replace(Box::new(new_root));
                }
                true
            }
            _ => false,
        }
    }

    // pub fn update(&self, key: u32, value: u32) -> bool {
    //     return false;
    // }
    //
    // pub fn delete(&self, key: u32) -> bool {
    //     return false;
    // }
}

#[cfg(test)]
mod tests {
    use crate::btree::mem::BTree;

    #[test]
    fn leaf_node_insert_no_split() {
        let mut btree = BTree::new(3);
        btree.insert(10, 10);
        btree.insert(13, 13);
        btree.insert(15, 15);

        assert_eq!((1, 0), btree.count_nodes());
        assert_eq!(Some(10), btree.lookup(10));
        assert_eq!(Some(13), btree.lookup(13));
        assert_eq!(Some(15), btree.lookup(15));
        assert_eq!(None, btree.lookup(9));
    }

    #[test]
    fn leaf_node_insert_split() {
        let mut btree = BTree::new(3);
        btree.insert(10, 10);
        btree.insert(13, 13);
        btree.insert(15, 15);
        btree.insert(11, 11);

        assert_eq!((2, 1), btree.count_nodes());
        assert_eq!(Some(10), btree.lookup(10));
        assert_eq!(Some(11), btree.lookup(11));
        assert_eq!(Some(13), btree.lookup(13));
        assert_eq!(Some(15), btree.lookup(15));
        assert_eq!(None, btree.lookup(9));
    }

    #[test]
    fn internal_node_insert_split() {
        let mut btree = BTree::new(3);
        for i in (0..13).step_by(2) {
            btree.insert(i, i);
        }
        assert_eq!((5, 3), btree.count_nodes());
        for i in (0..13).step_by(2) {
            assert_eq!(Some(i), btree.lookup(i));
        }
    }
}
