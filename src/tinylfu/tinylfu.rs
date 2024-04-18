use crate::tinylfu::estimator::Estimator;
use std::hash::BuildHasher;
use std::sync::atomic;
use std::sync::atomic::AtomicUsize;
use t1ha::T1haHashMap;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const VEC_GROWTH_CAP: usize = 65536;

/// TinyLFU cache
/// paper: https://arxiv.org/pdf/1512.00727.pdf
/// Tuning knobs based on dataset and hardware: evict_window,
struct TinyLFU<K, V> {
    capacity: usize,
    estimator: Estimator,
    window_counter: AtomicUsize,
    window_limit: usize,
    cache: T1haHashMap<K, V>, // hashmap data

    // aging
    min_frequency: u8,
}

impl<K, V> TinyLFU<K, V>
where
    K: std::hash::Hash + Eq,
    V: PartialEq,
{
    /// Create a new TinyLFU cache with a given capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        let estimator = Estimator::new_optimal(capacity);
        Self {
            cache: T1haHashMap::with_capacity_and_hasher(capacity, Default::default()),
            capacity,
            window_counter: Default::default(),
            window_limit: capacity * 8, // heuristic
            estimator,
            min_frequency: 0,
        }
    }

    /// Get a value from the cache.
    pub fn get(&mut self, key: &K) -> Option<&V> {
        self.check_window();
        self.cache.get(key)
    }

    /// Set a key-value pair in the cache.
    ///
    /// Cache is fixed with capacity and it doesn't grow
    pub fn put(&mut self, key: K, value: V) {
        let hash = self.cache.hasher().hash_one(&key);
        self.cache.insert(key, value);
    }

    fn check_window(&mut self) {
        let window_counter = self.window_counter.fetch_add(1, atomic::Ordering::Relaxed);

        if window_counter >= self.window_limit {
            self.window_counter.store(0, atomic::Ordering::Relaxed);
            self.estimator.age(1);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sanity() {
        let mut cache = TinyLFU::with_capacity(64);
        cache.put(1, 1);
        cache.put(2, 2);
        cache.put(3, 3);

        assert_eq!(cache.get(&1), Some(&1));
        assert_eq!(cache.get(&2), Some(&2));
        assert_eq!(cache.get(&3), Some(&3));
    }
}
