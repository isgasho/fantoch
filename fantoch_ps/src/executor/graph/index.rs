use super::tarjan::Vertex;
use fantoch::id::Dot;
use std::cell::UnsafeCell;
use std::collections::{HashMap, HashSet};

#[derive(Default, Debug)]
pub struct VertexIndex {
    index: HashMap<Dot, UnsafeCell<Vertex>>,
}

impl VertexIndex {
    pub fn new() -> Self {
        Default::default()
    }

    /// Indexes a new vertex, returning whether a vertex with this dot was
    /// already indexed or not.
    pub fn index(&mut self, vertex: Vertex) -> bool {
        let res = self.index.insert(vertex.dot(), UnsafeCell::new(vertex));
        res.is_none()
    }

    /// Returns a mutable reference to an indexed vertex (if previously
    /// indexed).
    pub fn get_mut(&self, dot: &Dot) -> Option<&mut Vertex> {
        self.index.get(dot).map(|cell| unsafe { &mut *cell.get() })
    }

    /// Removes a vertex from the index.
    pub fn remove(&mut self, dot: &Dot) -> Option<Vertex> {
        self.index.remove(dot).map(|cell| cell.into_inner())
    }
}

#[derive(Default, Debug)]
pub struct PendingIndex {
    index: HashMap<Dot, HashSet<Dot>>,
}

impl PendingIndex {
    pub fn new() -> Self {
        Default::default()
    }

    /// Indexes a new `dot` as a child of `dep_dot`:
    /// - when `dep_dot` is executed, we'll try to execute `dot` as `dep_dot`
    ///   was a dependency and maybe now `dot` can be executed
    pub fn index(&mut self, dep_dot: Dot, dot: Dot) {
        // get current list of pending dots
        let pending = match self.index.get_mut(&dep_dot) {
            Some(pending) => pending,
            None => self.index.entry(dep_dot).or_insert_with(HashSet::new),
        };
        // add new `dot` to pending
        pending.insert(dot);
    }

    /// Finds all pending dots for a given dependency dot.
    pub fn remove(&mut self, dep_dot: &Dot) -> Option<HashSet<Dot>> {
        self.index.remove(dep_dot)
    }
}