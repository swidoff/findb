pub struct BTree {
    capacity: usize,
    root: Box<dyn Node>,
}

enum InsertResult {
    SuccessNoSplit,
    Duplicate,
    SuccessSplit {
        midpoint_key: u32,
        split_node: Box<dyn Node>,
    },
}

trait Node {
    fn find_node(&self, key: u32) -> Option<&Leaf>;
    fn insert(&mut self, key: u32, value: u32) -> InsertResult;
}

struct Leaf {
    kv: Vec<(u32, u32)>,
}

impl Node for Leaf {
    fn find_node(&self, key: u32) -> Option<&Leaf> {
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
                        split_kv.push(self.kv[i])
                    }

                    // Truncate this kv from the midpoint and push the new key and value.
                    self.kv.truncate(midpoint_index);
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
    fn child_for(&self, key: u32) -> &Box<dyn Node> {
        let index = match self.keys.binary_search(&key) {
            Ok(index) => index + 1,
            Err(index) => index,
        };
        &self.pointers[index]
    }
}

impl Node for InternalNode {
    fn find_node(&self, key: u32) -> Option<&Leaf> {
        self.child_for(key).find_node(key)
    }

    fn insert(&mut self, key: u32, value: u32) -> InsertResult {
        // self.child_for(key).insert(key, value)
        unimplemented!()
    }
}

impl BTree {
    pub fn new(capacity: usize) -> BTree {
        BTree {
            capacity,
            root: Box::new(Leaf {
                kv: Vec::with_capacity(capacity),
            }),
        }
    }

    pub fn lookup(&self, key: u32) -> Option<u32> {
        self.root.find_node(key).and_then(|leaf| leaf.lookup(key))
    }

    pub fn lookup_range<'a>(&self, from_key: u32, to_key: u32) -> &'a dyn Iterator<Item = u32> {
        panic!("Not implemented")
    }

    pub fn insert(&self, key: u32, value: u32) -> bool {
        return false;
    }

    pub fn update(&self, key: u32, value: u32) -> bool {
        return false;
    }

    pub fn delete(&self, key: u32) -> bool {
        return false;
    }
}
