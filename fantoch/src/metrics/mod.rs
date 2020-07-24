// This module contains the definition of `F64`.
pub mod float;

// This module contains the definition of `Histogram`.
mod histogram;

// Re-exports.
pub use float::F64;
pub use histogram::{Histogram, Stats};

use crate::HashMap;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::hash::Hash;

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Metrics<K: Eq + Hash, V> {
    collected: HashMap<K, Histogram>,
    aggregated: HashMap<K, V>,
}

impl<K, V> Metrics<K, V>
where
    K: Eq + Hash,
    V: Default,
{
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self {
            collected: HashMap::new(),
            aggregated: HashMap::new(),
        }
    }

    pub fn collect(&mut self, kind: K, value: u64) {
        let stats = match self.collected.get_mut(&kind) {
            Some(current) => current,
            None => self.collected.entry(kind).or_insert_with(Histogram::new),
        };
        stats.increment(value);
    }

    pub fn aggregate<F>(&mut self, kind: K, update: F)
    where
        F: FnOnce(&mut V),
    {
        let current = match self.aggregated.get_mut(&kind) {
            Some(current) => current,
            None => self.aggregated.entry(kind).or_insert_with(V::default),
        };
        update(current);
    }

    pub fn get_collected(&self, kind: K) -> Option<&Histogram> {
        self.collected.get(&kind)
    }

    pub fn get_aggregated(&self, kind: K) -> Option<&V> {
        self.aggregated.get(&kind)
    }
}

impl<K, V> fmt::Debug for Metrics<K, V>
where
    K: Eq + Hash + fmt::Debug,
    V: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (kind, histogram) in self.collected.iter() {
            writeln!(f, "{:?}: {:?}", kind, histogram)?;
        }
        for (kind, value) in self.aggregated.iter() {
            writeln!(f, "{:?}: {:?}", kind, value)?;
        }
        Ok(())
    }
}
