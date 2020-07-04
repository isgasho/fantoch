use dashmap::{DashMap, ElementGuard, Iter};
use fantoch::kvs::Key;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct Shared<V: 'static> {
    clocks: Arc<DashMap<Key, V>>,
}

impl<V> Shared<V>
where
    V: Default,
{
    // Create a `SharedClocks` instance.
    pub fn new() -> Self {
        // create clocks
        let clocks = Arc::new(DashMap::new());
        Self { clocks }
    }

    // Tries to retrieve the current value associated with `key`. If there's no
    // associated value, an entry will be created.
    pub fn get(&self, key: &Key) -> ElementGuard<Key, V> {
        match self.clocks.get(key) {
            Some(value) => value,
            None => {
                self.maybe_insert(key);
                self.get(key)
            }
        }
    }

    pub fn iter(&self) -> Iter<Key, V> {
        self.clocks.iter()
    }

    pub fn len(&self) -> usize {
        self.clocks.len()
    }

    fn maybe_insert(&self, key: &Key) {
        // insert entry only if it doesn't yet exist:
        // - maybe another thread tried to `maybe_insert` and was able to insert
        //   before us
        // - replacing this function with what follows should make the tests
        //   fail (blindly inserting means that we could lose updates)
        // `self.clocks.insert(key.clone(), V::default());`
        // - `Entry::or_*` methods from `dashmap` ensure that we don't lose any
        //   updates. See: https://github.com/xacrimon/dashmap/issues/47
        // TODO this functionality seems to have been removed
        // self.clocks.entry(key.clone()).or_default();
    }
}
