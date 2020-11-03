use std::borrow::Borrow;
use std::cell::{Ref, RefCell};
use std::rc::{Rc, Weak};

enum InsertResult {
    SuccessNoSplit,
    Duplicate,
    SuccessSplit {
        split_key: u32,
        split_node: Rc<RefCell<dyn Node>>,
    },
}

trait Node {
    fn lookup(&self, key: u32) -> Option<u32>;
    fn lookup_range(&self, from_key: u32, to_key: u32) -> LookupRangeIterator;
    fn update(&mut self, key: u32, value: u32) -> Option<u32>;
    fn insert(&mut self, key: u32, value: u32) -> InsertResult;
    fn delete(&mut self, key: u32) -> Option<u32>;
    fn merge(&mut self, midpoint_key: u32, other: &Rc<RefCell<dyn Node>>) -> bool;
    fn add_to_graph_vis(&self, graphviz: &mut GraphViz) -> usize;

    fn count_nodes(&self) -> (usize, usize) {
        (1, 0)
    }
    fn merge_into_leaf(&mut self, _other: &mut Leaf) -> bool {
        false
    }
    fn merge_into_internal_node(&mut self, _midpoint_key: u32, _other: &mut InternalNode) -> bool {
        false
    }
}

struct Leaf {
    kv: Vec<(u32, u32)>,
    next: Weak<RefCell<Leaf>>,
    this: Weak<RefCell<Leaf>>,
}

impl Leaf {
    fn new(capacity: usize) -> Leaf {
        Leaf {
            kv: Vec::with_capacity(capacity),
            next: Weak::new(),
            this: Weak::new(),
        }
    }

    fn from_kv(capacity: usize, kv: &[(u32, u32)]) -> Leaf {
        let mut leaf = Leaf {
            kv: Vec::with_capacity(capacity),
            next: Weak::new(),
            this: Weak::new(),
        };
        leaf.kv.extend_from_slice(kv);
        leaf
    }
}

impl Node for Leaf {
    fn lookup(&self, key: u32) -> Option<u32> {
        self.kv
            .binary_search_by_key(&key, |value| value.0)
            .map(|idx| self.kv[idx].1)
            .ok()
    }

    fn lookup_range(&self, from_key: u32, to_key: u32) -> LookupRangeIterator {
        let index = match self.kv.binary_search_by_key(&from_key, |value| value.0) {
            Ok(index) => index,
            Err(index) => index,
        };

        LookupRangeIterator {
            leaf: Weak::clone(&self.this),
            index,
            to_key,
        }
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
                    let mut new_leaf = Leaf::new(self.kv.capacity());
                    new_leaf
                        .kv
                        .extend(self.kv.drain(midpoint_index..self.kv.len()));

                    // Insert the the new key and value into the correct node.
                    if key < midpoint_key {
                        self.insert(key, value);
                    } else {
                        new_leaf.insert(key, value);
                    }

                    let split_node = Rc::new(RefCell::new(new_leaf));
                    split_node.borrow_mut().this = Rc::downgrade(&split_node);
                    split_node.borrow_mut().next = Weak::clone(&self.next);
                    self.next = Rc::downgrade(&split_node);
                    InsertResult::SuccessSplit {
                        split_key: midpoint_key,
                        split_node,
                    }
                }
            }
        }
    }

    fn delete(&mut self, key: u32) -> Option<u32> {
        let search_result = self.kv.binary_search_by_key(&key, |value| value.0);
        match search_result {
            Ok(index) => {
                let value = self.kv.remove(index);
                Some(value.1)
            }
            Err(_) => None,
        }
    }

    fn merge(&mut self, _midpoint_key: u32, other: &Rc<RefCell<dyn Node>>) -> bool {
        other.borrow_mut().merge_into_leaf(self)
    }

    fn add_to_graph_vis(&self, graphviz: &mut GraphViz) -> usize {
        graphviz.add_leaf_node(&self.kv)
    }

    fn merge_into_leaf(&mut self, other: &mut Leaf) -> bool {
        if self.kv.len() + other.kv.len() > other.kv.capacity() {
            false
        } else {
            other.kv.extend(self.kv.drain(0..self.kv.len()));
            other.next = Weak::clone(&other.next);
            true
        }
    }
}

pub struct LookupRangeIterator {
    leaf: Weak<RefCell<Leaf>>,
    index: usize,
    to_key: u32,
}

impl LookupRangeIterator {
    fn empty() -> LookupRangeIterator {
        LookupRangeIterator {
            leaf: Weak::new(),
            index: 0,
            to_key: 0,
        }
    }
}

impl Iterator for LookupRangeIterator {
    type Item = u32;

    fn next(&mut self) -> Option<Self::Item> {
        match self.leaf.upgrade() {
            Some(leaf) => {
                let leaf: &RefCell<Leaf> = leaf.borrow();
                let leaf: Ref<Leaf> = leaf.borrow();
                if self.index >= leaf.kv.len() {
                    self.leaf = Weak::clone(&leaf.next);
                    self.index = 0;
                    self.next()
                } else if leaf.kv[self.index].0 <= self.to_key {
                    let res = Some(leaf.kv[self.index].1);
                    self.index += 1;
                    res
                } else {
                    None
                }
            }
            None => None,
        }
    }
}

struct InternalNode {
    keys: Vec<u32>,
    pointers: Vec<Rc<RefCell<dyn Node>>>,
}

impl InternalNode {
    fn index_for(&self, key: u32) -> usize {
        match self.keys.binary_search(&key) {
            Ok(index) => index + 1,
            Err(index) => index,
        }
    }

    fn insert_key_and_pointer(&mut self, key: u32, pointer: Rc<RefCell<dyn Node>>) {
        let idx = self.index_for(key);
        self.keys.insert(idx, key);
        self.pointers.insert(idx + 1, pointer);
    }
}

impl Node for InternalNode {
    fn lookup(&self, key: u32) -> Option<u32> {
        let index = self.index_for(key);
        self.pointers[index].borrow_mut().lookup(key)
    }

    fn lookup_range(&self, from_key: u32, to_key: u32) -> LookupRangeIterator {
        let index = self.index_for(from_key);
        self.pointers[index]
            .borrow_mut()
            .lookup_range(from_key, to_key)
    }

    fn update(&mut self, key: u32, value: u32) -> Option<u32> {
        let index = self.index_for(key);
        self.pointers[index].borrow_mut().update(key, value)
    }

    fn insert(&mut self, key: u32, value: u32) -> InsertResult {
        let insert_index = self.index_for(key);
        let result = self.pointers[insert_index].borrow_mut().insert(key, value);
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
                        split_node: Rc::new(RefCell::new(new_node)),
                    }
                }
            }
            x => x,
        }
    }

    fn delete(&mut self, key: u32) -> Option<u32> {
        let delete_index = self.index_for(key);
        let result = self.pointers[delete_index].borrow_mut().delete(key);
        if result.is_some() {
            let mut merged = false;
            if delete_index > 0 {
                let midpoint_key = self.keys[delete_index - 1];
                let (left, right) = self.pointers.split_at_mut(delete_index);
                if left[left.len() - 1]
                    .borrow_mut()
                    .merge(midpoint_key, &right[0])
                {
                    self.keys.remove(delete_index - 1);
                    self.pointers.remove(delete_index);
                    merged = true
                }
            }
            if !merged && delete_index < self.pointers.len() - 1 {
                let midpoint_key = self.keys[delete_index];
                let (left, right) = self.pointers.split_at_mut(delete_index + 1);
                if left[left.len() - 1]
                    .borrow_mut()
                    .merge(midpoint_key, &right[0])
                {
                    self.keys.remove(delete_index);
                    self.pointers.remove(delete_index + 1);
                }
            }
        }
        result
    }

    fn merge(&mut self, midpoint_key: u32, other: &Rc<RefCell<dyn Node>>) -> bool {
        other
            .borrow_mut()
            .merge_into_internal_node(midpoint_key, self)
    }

    fn add_to_graph_vis(&self, graphviz: &mut GraphViz) -> usize {
        let node_id = graphviz.add_internal_node(&self.keys);
        for i in 0..self.pointers.len() {
            let target: &RefCell<dyn Node> = self.pointers[i].borrow();
            let target: Ref<dyn Node> = target.borrow();
            let target_id = target.borrow().add_to_graph_vis(graphviz);
            graphviz.add_edge(node_id, i, target_id);
        }
        return node_id;
    }

    fn count_nodes(&self) -> (usize, usize) {
        let mut leaf_count = 0;
        let mut internal_count = 1;
        for child in self.pointers.iter() {
            let child: &RefCell<dyn Node> = child.borrow();
            let child: Ref<dyn Node> = child.borrow();
            let (inner_leaf_count, inner_internal_count) = child.borrow().count_nodes();
            leaf_count += inner_leaf_count;
            internal_count += inner_internal_count;
        }
        (leaf_count, internal_count)
    }

    fn merge_into_internal_node(&mut self, midpoint_key: u32, other: &mut InternalNode) -> bool {
        if self.pointers.len() + other.pointers.len() > other.pointers.capacity() {
            false
        } else {
            other.keys.push(midpoint_key);
            other.keys.extend(self.keys.drain(0..self.keys.len()));
            other
                .pointers
                .extend(self.pointers.drain(0..self.pointers.len()));
            true
        }
    }
}

pub struct BTree {
    capacity: usize,
    root: Option<Rc<RefCell<dyn Node>>>,
}

impl BTree {
    pub fn new(capacity: usize) -> BTree {
        let leaf = Rc::new(RefCell::new(Leaf::new(capacity)));
        leaf.borrow_mut().this = Rc::downgrade(&leaf);
        BTree {
            capacity,
            root: Some(leaf),
        }
    }

    pub fn count_nodes(&self) -> (usize, usize) {
        self.root.as_ref().map_or((0, 0), |root| {
            let root: &RefCell<dyn Node> = root.borrow();
            let root: Ref<dyn Node> = root.borrow();
            root.count_nodes()
        })
    }

    pub fn lookup(&mut self, key: u32) -> Option<u32> {
        self.root.as_ref().and_then(|root| {
            let root: &RefCell<dyn Node> = root.borrow();
            let root: Ref<dyn Node> = root.borrow();
            root.lookup(key)
        })
    }

    pub fn lookup_range(&self, from_key: u32, to_key: u32) -> LookupRangeIterator {
        self.root
            .as_ref()
            .map(|root| {
                let root: &RefCell<dyn Node> = root.borrow();
                let root: Ref<dyn Node> = root.borrow();
                root.lookup_range(from_key, to_key)
            })
            .unwrap()
    }

    pub fn insert(&mut self, key: u32, value: u32) -> bool {
        let result = self
            .root
            .as_mut()
            .map(|root| root.borrow_mut().insert(key, value));
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
                    self.root.replace(Rc::new(RefCell::new(new_root)));
                }
                true
            }
            _ => false,
        }
    }

    pub fn update(&mut self, key: u32, value: u32) -> Option<u32> {
        self.root
            .as_mut()
            .and_then(|root| root.borrow_mut().update(key, value))
    }

    pub fn delete(&mut self, key: u32) -> Option<u32> {
        self.root
            .as_mut()
            .and_then(|root| root.borrow_mut().delete(key))
    }

    pub fn print(&self) {
        let mut gv = GraphViz::new();
        self.root.as_ref().map(|root| {
            let root: &RefCell<dyn Node> = root.borrow();
            let root: Ref<dyn Node> = root.borrow();
            root.add_to_graph_vis(&mut gv)
        });
        gv.print();
    }
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

#[cfg(test)]
mod tests {
    use crate::btree::mem::{BTree, InternalNode, Leaf, Node};
    use itertools::Itertools;
    use std::cell::RefCell;
    use std::rc::Rc;

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
        let btree = validate_insert_and_update(3, &seq);
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

    #[test]
    fn delete_no_merge() {
        let mut btree = BTree {
            capacity: 3,
            root: Some(Rc::new(RefCell::new(Leaf::from_kv(
                3,
                &[(15, 150), (16, 160), (18, 180)],
            )))),
        };

        assert_eq!(Some(150), btree.delete(15));
        assert_eq!(None, btree.lookup(15));
        assert_eq!(None, btree.delete(17));
    }

    #[test]
    fn delete_merge_leaves() {
        let leaf1 = Rc::new(RefCell::new(Leaf::from_kv(
            3,
            &[(1, 10), (5, 50), (10, 100)],
        )));
        let leaf2 = Rc::new(RefCell::new(Leaf::from_kv(
            3,
            &[(15, 150), (16, 160), (17, 170)],
        )));
        let leaf3 = Rc::new(RefCell::new(Leaf::from_kv(
            3,
            &[(20, 200), (23, 230), (25, 250)],
        )));
        let mut btree = BTree {
            capacity: 3,
            root: Some(Rc::new(RefCell::new(InternalNode {
                keys: vec![11, 20],
                pointers: vec![leaf1, leaf2, leaf3],
            }))),
        };

        btree.print();
        assert_eq!((3, 1), btree.count_nodes());
        assert_eq!(Some(170), btree.delete(17));
        assert_eq!(Some(230), btree.delete(23));
        assert_eq!(Some(160), btree.delete(16));
        assert_eq!((2, 1), btree.count_nodes());
        btree.print();
    }

    #[test]
    fn delete_merge_internal_nodes() {
        fn leaf(keys: &[u32]) -> Rc<RefCell<Leaf>> {
            let mut kv = Vec::with_capacity(3);
            kv.extend(keys.iter().map(|k| (*k, *k * 10)));
            Rc::new(RefCell::new(Leaf::from_kv(3, &kv[..])))
        }

        fn internal(
            keys_arr: &[u32],
            pointers_arr: Vec<Rc<RefCell<dyn Node>>>,
        ) -> Rc<RefCell<InternalNode>> {
            let mut keys = Vec::with_capacity(3);
            let mut pointers = Vec::with_capacity(4);
            keys.extend(keys_arr);
            pointers.extend(pointers_arr);
            Rc::new(RefCell::new(InternalNode { keys, pointers }))
        }

        let leaf1 = leaf(&[1, 2, 3]);
        let leaf2 = leaf(&[4]);
        let leaf3 = leaf(&[6, 7, 8]);
        let leaf4 = leaf(&[9]);
        let leaf5 = leaf(&[10]);
        let leaf6 = leaf(&[11, 12]);
        let leaf7 = leaf(&[13, 14, 15]);
        let internal1 = internal(&[4], vec![leaf1, leaf2]);
        let internal2 = internal(&[9, 10], vec![leaf3, leaf4, leaf5]);
        let internal3 = internal(&[13], vec![leaf6, leaf7]);
        let root = internal(&[5, 11], vec![internal1, internal2, internal3]);
        let mut btree = BTree {
            capacity: 3,
            root: Some(root),
        };

        btree.print();
        assert_eq!((7, 4), btree.count_nodes());
        assert_eq!(Some(100), btree.delete(10));
        assert_eq!((6, 3), btree.count_nodes());
        btree.print();
    }

    #[test]
    fn delete_100_capacity5() {
        let seq = [
            90, 95, 85, 41, 11, 29, 100, 19, 1, 30, 3, 2, 39, 18, 82, 26, 49, 28, 46, 88, 77, 58,
            35, 54, 61, 16, 91, 9, 40, 48, 94, 45, 99, 69, 38, 57, 65, 13, 7, 55, 22, 86, 71, 34,
            50, 15, 98, 10, 36, 96, 79, 92, 62, 21, 89, 43, 78, 93, 44, 20, 72, 56, 68, 17, 6, 42,
            73, 64, 70, 75, 5, 76, 80, 74, 8, 63, 60, 59, 31, 25, 27, 33, 32, 14, 52, 24, 4, 47,
            81, 97, 53, 51, 84, 67, 83, 12, 23, 37, 87, 66,
        ];

        let mut btree = BTree::new(5);
        for i in seq.iter() {
            btree.insert(*i, *i * 100);
        }
        btree.print();

        for i in 0..seq.len() {
            let initial_value = btree.lookup(seq[i]);
            let orig_value = btree.delete(seq[i]);
            assert_eq!(initial_value, orig_value);

            for j in (i + 1)..seq.len() {
                assert_eq!(Some(seq[j] * 100), btree.lookup(seq[j]));
            }
        }
        for i in seq.iter() {
            assert_eq!(None, btree.lookup(*i));
        }
        // assert_eq!((1, 0), btree.count_nodes(1, 0));
        // btree.print();

        for i in 0..25 {
            btree.insert(seq[i], seq[i] * 100);
            assert_eq!(Some(seq[i] * 100), btree.lookup(seq[i]));
        }
        btree.print();
    }

    #[test]
    fn leaf_node_lookup_range() {
        let seq = [10, 15, 13];
        let btree = validate_insert_and_update(3, &seq);
        assert_eq!(
            vec![100, 130, 150],
            btree.lookup_range(10, 15).collect_vec()
        );
        let empty: Vec<u32> = Vec::new();

        assert_eq!(vec![130, 150], btree.lookup_range(13, 15).collect_vec());
        assert_eq!(vec![100, 130], btree.lookup_range(10, 13).collect_vec());
        assert_eq!(vec![100], btree.lookup_range(10, 10).collect_vec());
        assert_eq!(vec![100], btree.lookup_range(0, 10).collect_vec());
        assert_eq!(vec![150], btree.lookup_range(15, 1000).collect_vec());
        assert_eq!(vec![130, 150], btree.lookup_range(13, 1000).collect_vec());
        assert_eq!(empty, btree.lookup_range(16, 100).collect_vec());
        assert_eq!(empty, btree.lookup_range(1, 9).collect_vec());
    }

    #[test]
    fn insert_100_lookup_range() {
        let seq = [
            90, 95, 85, 41, 11, 29, 100, 19, 1, 30, 3, 2, 39, 18, 82, 26, 49, 28, 46, 88, 77, 58,
            35, 54, 61, 16, 91, 9, 40, 48, 94, 45, 99, 69, 38, 57, 65, 13, 7, 55, 22, 86, 71, 34,
            50, 15, 98, 10, 36, 96, 79, 92, 62, 21, 89, 43, 78, 93, 44, 20, 72, 56, 68, 17, 6, 42,
            73, 64, 70, 75, 5, 76, 80, 74, 8, 63, 60, 59, 31, 25, 27, 33, 32, 14, 52, 24, 4, 47,
            81, 97, 53, 51, 84, 67, 83, 12, 23, 37, 87, 66,
        ];

        let btree = validate_insert_and_update(5, &seq);
        assert_eq!(
            vec![130, 140, 150],
            btree.lookup_range(13, 15).collect_vec()
        );
        assert_eq!(
            vec![800, 810, 820, 830, 840, 850, 860],
            btree.lookup_range(80, 86).collect_vec()
        );
    }
}
