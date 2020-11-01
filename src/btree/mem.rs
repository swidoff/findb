enum InsertResult {
    SuccessNoSplit,
    Duplicate,
    SuccessSplit {
        split_key: u32,
        split_node: Box<dyn Node>,
    },
}

struct GraphViz {
    node_counter: usize,
    lines: Vec<String>,
}

impl GraphViz {
    fn new() -> GraphViz {
        GraphViz {
            node_counter: 0,
            lines: Vec::new(),
        }
    }

    fn add_leaf_node(&mut self, kv: &Vec<(u32, u32)>) -> usize {
        let node_id = self.node_counter;
        let mut line = format!("struct{} [label=\"", node_id);
        line.push_str("{{");
        for i in 0..kv.capacity() {
            if i > 0 {
                line.push('|')
            }
            if i < kv.len() {
                line.push_str(kv[i].0.to_string().as_str());
            }
        }
        line.push_str("}|{");
        for i in 0..kv.capacity() {
            if i > 0 {
                line.push('|')
            }
            if i < kv.len() {
                line.push_str(kv[i].1.to_string().as_str());
            }
        }
        line.push_str("}}\"];");
        self.lines.push(line);
        self.node_counter += 1;
        node_id
    }

    fn add_internal_node(&mut self, keys: &Vec<u32>) -> usize {
        let node_id = self.node_counter;
        let mut line = format!("struct{} [label=\"{{{{", node_id);
        for i in 0..keys.capacity() {
            if i > 0 {
                line.push('|')
            }
            if i < keys.len() {
                line.push_str(keys[i].to_string().as_str());
            }
        }
        line.push_str("}|{");
        for i in 0..(keys.capacity() + 1) {
            if i > 0 {
                line.push('|')
            }
            line.push_str("<p");
            line.push_str(i.to_string().as_str());
            line.push('>');
        }
        line.push_str("}}\"];");
        self.lines.push(line);
        self.node_counter += 1;
        node_id
    }

    fn add_edge(&mut self, node_id: usize, pointer_idx: usize, target_id: usize) {
        self.lines.push(format!(
            "struct{node_id}:p{pointer_idx} -> struct{target_id}",
            node_id = node_id,
            pointer_idx = pointer_idx,
            target_id = target_id
        ))
    }

    fn print(&self) {
        println!("digraph structs {{");
        println!("\tnode [shape=record]");
        for line in self.lines.iter() {
            println!("\t{}", line);
        }
        println!("}}");
    }
}

trait Node {
    fn find_leaf(&mut self, key: u32) -> Option<&mut Leaf>;
    fn insert(&mut self, key: u32, value: u32) -> InsertResult;
    fn add_to_graph_vis(&self, graphviz: &mut GraphViz) -> usize;

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
    fn find_leaf(&mut self, key: u32) -> Option<&mut Leaf> {
        if !self.kv.is_empty() && key >= self.kv[0].0 && key <= self.kv[self.kv.len() - 1].0 {
            Option::Some(self)
        } else {
            Option::None
        }
    }

    fn insert(&mut self, key: u32, value: u32) -> InsertResult {
        let search_result = self.kv.binary_search_by_key(&key, |value| value.0);
        match search_result {
            Ok(_) => InsertResult::Duplicate,
            Err(index) => {
                if self.kv.len() < self.kv.capacity() {
                    self.kv.insert(index, (key, value));
                    InsertResult::SuccessNoSplit
                } else {
                    let midpoint_index = self.kv.len() / 2;
                    let midpoint_key = self.kv[midpoint_index].0;

                    // Allocate new kv for split node, moving from the midpoint of this node's kv.
                    let mut new_leaf = Leaf {
                        kv: Vec::with_capacity(self.kv.capacity()),
                    };
                    new_leaf
                        .kv
                        .extend(self.kv.drain(midpoint_index..self.kv.len()));

                    // Insert the the new key and value into the correct node.
                    if key < midpoint_key {
                        self.insert(key, value);
                    } else {
                        new_leaf.insert(key, value);
                    }

                    InsertResult::SuccessSplit {
                        split_key: midpoint_key,
                        split_node: Box::new(new_leaf),
                    }
                }
            }
        }
    }

    fn add_to_graph_vis(&self, graphviz: &mut GraphViz) -> usize {
        graphviz.add_leaf_node(&self.kv)
    }
}

impl Leaf {
    fn lookup(&self, key: u32) -> Option<u32> {
        self.kv
            .binary_search_by_key(&key, |value| value.0)
            .map(|idx| self.kv[idx].1)
            .ok()
    }

    fn update(&mut self, key: u32, value: u32) -> Option<u32> {
        self.kv
            .binary_search_by_key(&key, |value| value.0)
            .map(|idx| {
                let orig_value = self.kv[idx].1;
                self.kv[idx].1 = value;
                orig_value
            })
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
    fn find_leaf(&mut self, key: u32) -> Option<&mut Leaf> {
        let index = self.index_for(key);
        self.pointers[index].find_leaf(key)
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

                    let mut new_node = InternalNode {
                        keys: Vec::with_capacity(self.keys.capacity()),
                        pointers: Vec::with_capacity(self.pointers.capacity()),
                    };

                    // Allocate new kv for split node, moving from the midpoint of this node's kv.
                    new_node
                        .keys
                        .extend(self.keys.drain((midpoint_index + 1)..self.keys.len()));
                    new_node.pointers.extend(
                        self.pointers
                            .drain((midpoint_index + 1)..self.pointers.len()),
                    );

                    // Remove the midpoint, since it's being promoted to the parent node.
                    self.keys.truncate(midpoint_index);
                    self.pointers.truncate(midpoint_index + 1);

                    if split_key < midpoint_key {
                        self.insert_key_and_pointer(split_key, split_node)
                    } else {
                        new_node.insert_key_and_pointer(split_key, split_node)
                    }

                    InsertResult::SuccessSplit {
                        split_key: midpoint_key,
                        split_node: Box::new(new_node),
                    }
                }
            }
            x => x,
        }
    }

    fn add_to_graph_vis(&self, graphviz: &mut GraphViz) -> usize {
        let node_id = graphviz.add_internal_node(&self.keys);
        for i in 0..self.pointers.len() {
            let target_id = self.pointers[i].add_to_graph_vis(graphviz);
            graphviz.add_edge(node_id, i, target_id);
        }
        return node_id;
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

    pub fn lookup(&mut self, key: u32) -> Option<u32> {
        self.root
            .as_mut()
            .and_then(|root| root.find_leaf(key))
            .and_then(|leaf| leaf.lookup(key))
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

    pub fn update(&mut self, key: u32, value: u32) -> Option<u32> {
        self.root
            .as_mut()
            .and_then(|root| root.find_leaf(key))
            .and_then(|leaf| leaf.update(key, value))
    }

    // pub fn delete(&self, key: u32) -> bool {
    //     return false;
    // }

    pub fn print(&self) {
        let mut gv = GraphViz::new();
        self.root
            .as_ref()
            .map(|root| root.add_to_graph_vis(&mut gv));
        gv.print();
    }
}

#[cfg(test)]
mod tests {
    use crate::btree::mem::BTree;
    use itertools::Itertools;

    #[test]
    fn leaf_node_insert_no_split() {
        let seq = [10, 15, 13];
        let mut btree = validate_insert_and_update(3, &seq);
        assert_eq!((1, 0), btree.count_nodes());
        assert_eq!(None, btree.lookup(11));
    }

    #[test]
    fn leaf_node_insert_split() {
        let seq = [10, 13, 15, 11];
        let mut btree = validate_insert_and_update(3, &seq);
        assert_eq!((2, 1), btree.count_nodes());
    }

    #[test]
    fn internal_node_insert_split() {
        let seq = (0..13).step_by(2).collect_vec();
        let btree = validate_insert_and_update(3, &seq[..]);
        assert_eq!((5, 3), btree.count_nodes());
    }

    #[test]
    fn insert_25_capacity3() {
        let seq = [
            51, 88, 41, 26, 94, 39, 60, 85, 96, 74, 90, 62, 1, 89, 23, 57, 5, 90, 0, 22, 88, 33,
            94, 41, 85,
        ];

        validate_insert_and_update(3, &seq);
    }

    #[test]
    fn insert_100_capacity5() {
        let seq = [
            281, 59, 672, 361, 997, 991, 640, 914, 623, 976, 585, 312, 811, 652, 143, 819, 682,
            743, 780, 234, 428, 365, 809, 214, 358, 84, 234, 313, 423, 161, 278, 68, 222, 208, 797,
            775, 569, 557, 200, 349, 323, 385, 981, 15, 251, 981, 257, 616, 939, 15, 818, 799, 581,
            658, 443, 73, 860, 704, 253, 287, 404, 105, 49, 131, 761, 105, 416, 63, 176, 610, 807,
            873, 18, 134, 715, 61, 515, 232, 820, 991, 276, 396, 182, 535, 484, 782, 659, 39, 752,
            176, 544, 275, 947, 449, 494, 823, 593, 291, 149, 998,
        ];

        validate_insert_and_update(5, &seq);
    }

    fn validate_insert_and_update(capacity: usize, values: &[u32]) -> BTree {
        let mut btree = BTree::new(capacity);
        for i in values.iter() {
            btree.insert(*i, *i);
        }
        for i in values.iter() {
            assert_eq!(Some(*i), btree.lookup(*i));
        }
        for i in values.iter() {
            let initial_value = btree.lookup(*i);
            let orig_value = btree.update(*i, *i * 10);
            assert_eq!(initial_value, orig_value);
        }
        for i in values.iter() {
            assert_eq!(Some(*i * 10), btree.lookup(*i));
        }
        btree.print();
        btree
    }
}
