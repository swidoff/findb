enum InsertResult {
    SuccessNoSplit,
    Duplicate,
    SuccessSplit {
        midpoint_key: u32,
        split_node: Box<dyn Node>,
    },
}

trait Node {
    fn find_leaf(&self, key: u32) -> Option<&Leaf>;
    fn insert(&mut self, key: u32, value: u32) -> InsertResult;

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
        match self.kv.binary_search_by_key(&key, |value| value.1) {
            Ok(_) => InsertResult::Duplicate,
            Err(index) => {
                if self.kv.len() < self.kv.capacity() {
                    self.kv.insert(index, (key, value));
                    InsertResult::SuccessNoSplit
                } else {
                    let midpoint_index = self.kv.len() / 2;

                    // Allocate new kv for split node, copying from the midpoint of this node's kv.
                    let mut split_kv = Vec::with_capacity(self.kv.capacity());
                    for i in midpoint_index..self.kv.len() {
                        split_kv.push(self.kv.remove(i))
                    }

                    // Truncate this kv from the midpoint and push the new key and value.
                    self.kv.push((key, value));

                    InsertResult::SuccessSplit {
                        midpoint_key: self.kv[midpoint_index].1,
                        split_node: Box::new(Leaf { kv: split_kv }),
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
}

impl Node for InternalNode {
    fn find_leaf(&self, key: u32) -> Option<&Leaf> {
        self.pointers[self.index_for(key)].find_leaf(key)
    }

    fn insert(&mut self, key: u32, value: u32) -> InsertResult {
        let insert_index = self.index_for(key);
        match self.pointers[insert_index].insert(key, value) {
            InsertResult::SuccessSplit {
                midpoint_key,
                split_node,
            } => {
                let midpoint_index = self.index_for(midpoint_key);

                if self.keys.len() < self.keys.capacity() {
                    self.keys.insert(midpoint_index, midpoint_key);
                    self.pointers.insert(midpoint_index + 1, split_node);
                    InsertResult::SuccessNoSplit
                } else {
                    // Allocate new kv for split node, copying from the midpoint of this node's kv.
                    let mut split_keys = Vec::with_capacity(self.keys.capacity());
                    let mut split_pointers = Vec::with_capacity(self.pointers.capacity());
                    for i in (midpoint_index + 1)..self.keys.len() {
                        split_keys.push(self.keys.remove(i))
                    }
                    for i in (midpoint_index + 1)..self.pointers.len() {
                        split_pointers.push(self.pointers.remove(i))
                    }

                    // Truncate this kv from the midpoint and push the new key and value.
                    self.keys.truncate(midpoint_index - 1);
                    self.pointers.truncate(midpoint_index);
                    self.keys.push(midpoint_key);
                    self.pointers.push(split_node);

                    InsertResult::SuccessSplit {
                        midpoint_key: self.keys[midpoint_index],
                        split_node: Box::new(InternalNode {
                            keys: split_keys,
                            pointers: split_pointers,
                        }),
                    }
                }
            }
            x => x,
        }
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

    pub fn lookup(&self, key: u32) -> Option<u32> {
        self.root
            .as_ref()
            .and_then(|root| root.find_leaf(key).and_then(|leaf| leaf.lookup(key)))
    }

    // pub fn lookup_range<'a>(&self, from_key: u32, to_key: u32) -> &'a dyn Iterator<Item = u32> {
    //     panic!("Not implemented")
    // }

    pub fn insert(&mut self, key: u32, value: u32) -> bool {
        match self.root.as_mut().map(|root| root.insert(key, value)) {
            Some(InsertResult::SuccessNoSplit) => true,
            Some(InsertResult::SuccessSplit {
                midpoint_key,
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
