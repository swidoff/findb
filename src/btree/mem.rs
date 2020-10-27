pub struct BTree {
    capacity: usize,
    root: Box<dyn Node>,
}

trait Node {
    fn find_node(&self, key: u32) -> Option<&Leaf>;
}

struct Leaf {
    keys_and_values: Vec<(u32, u32)>,
}

impl Node for Leaf {
    fn find_node(&self, key: u32) -> Option<&Leaf> {
        if !self.keys_and_values.is_empty()
            && key >= self.keys_and_values[0].1
            && key <= self.keys_and_values[self.keys_and_values.len() - 1].1
        {
            Option::Some(&self)
        } else {
            Option::None
        }
    }
}

impl Leaf {
    fn lookup(&self, key: u32) -> Option<u32> {
        self.keys_and_values
            .binary_search_by_key(&key, |value| value.1)
            .map(|idx| self.keys_and_values[idx].1)
            .ok()
    }
}

struct InternalNode {
    keys: Vec<u32>,
    pointers: Vec<Box<dyn Node>>,
}

impl Node for InternalNode {
    fn find_node(&self, key: u32) -> Option<&Leaf> {
        let child_node = match self.keys.binary_search(&key) {
            Ok(index) => &self.pointers[index + 1],
            Err(index) => &self.pointers[index],
        };
        child_node.find_node(key)
    }
}

impl BTree {
    pub fn new(capacity: usize) -> BTree {
        BTree {
            capacity,
            root: Box::new(Leaf {
                keys_and_values: Vec::with_capacity(capacity),
            }),
        }
    }

    pub fn lookup(&self, key: u32) -> Option<u32> {
        self.root.find_node(key).and_then(|leaf| leaf.lookup(key))
    }

    pub fn lookup_range<'a>(&self, from_key: u32, to_key: u32) -> &'a dyn Iterator<Item = u32> {
        panic!("Not implemented")
    }

    pub fn insert(&self, key: u32, value: u32) {}

    pub fn update(&self, key: u32, value: u32) -> bool {
        return false;
    }

    pub fn delete(&self, key: u32) -> bool {
        return false;
    }
}
