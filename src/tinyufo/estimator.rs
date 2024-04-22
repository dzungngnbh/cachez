use std::cmp;
use std::cmp::max;
use std::hash::{Hash, Hasher};
use std::sync::atomic::Ordering::{Acquire, Relaxed};
use std::sync::atomic::{AtomicU8, AtomicUsize};
use t1ha::T1haHasher;

use crate::tinyufo::types::Key;

/// Stores estimated frequency of items in the cache.
///
/// Inner algorithm: Count-Min Sketch
/// we limit frequency with 8 bit
#[derive(Debug)]
pub struct Estimator {
    inner: Vec<(Vec<AtomicU8>, u64)>,
}

impl Estimator {
    /// Create a new Count-Min Sketch with optimal parameters
    pub fn new_optimal(items: usize) -> Self {
        let (w, d) = Self::optimal_params(items);
        Self::new(w, d)
    }

    /// Find optimal parameters for Count-Min Sketch
    fn optimal_params(items: usize) -> (usize, usize) {
        // From https://en.wikipedia.org/wiki/Count%E2%80%93min_sketch
        // w = ⌈e/ε⌉ and d = ⌈ln 1/δ⌉
        let error_rate = 1.0 / (items as f64);
        let failure_rate = 1.0 / (items as f64);
        let w = max(16, (std::f64::consts::E / error_rate).ceil() as usize);
        let d = max(2, (failure_rate.ln() / 0.5f64.ln()).ceil() as usize);
        (w, d)
    }

    /// Create a new Count-Min Sketch with `hashes` hash functions and `slots` slots
    pub fn new(hashes: usize, slots: usize) -> Self {
        let mut inner = Vec::with_capacity(hashes);
        for _ in 0..hashes {
            let mut slot = Vec::with_capacity(slots);
            for _ in 0..slots {
                slot.push(AtomicU8::new(0));
            }
            let seed = fastrand::u64(..);
            inner.push((slot, seed))
        }

        Self { inner }
    }

    /// Get the estimated frequency of the `key`
    pub fn get<H: Hash>(&self, key: H) -> u8 {
        let mut min = u8::MAX;
        for (slot, seed) in &self.inner {
            let mut hasher = T1haHasher::with_seed(*seed);
            key.hash(&mut hasher);
            let hash = hasher.finish() as usize % slot.len();
            let current = &slot[hash];
            let value = current.load(Relaxed);
            min = cmp::min(min, value);
        }
        min
    }

    /// Increment the frequency of the `key`
    ///
    /// Returns the min of all the frequencies of different hash seeds
    pub fn incr<H: Hash>(&mut self, key: H) -> u8 {
        let mut min = u8::MAX;
        for (slot, seed) in &self.inner {
            let mut hasher = T1haHasher::with_seed(*seed);
            key.hash(&mut hasher);
            let hash = hasher.finish() as usize % slot.len();
            let current = &slot[hash];
            let new = Self::incr_no_overflow(current);
            min = cmp::min(u8::MAX, new);
        }
        min
    }

    /// Age, shift right all counters by `shift` bits
    pub fn age(&mut self, shift: u8) {
        for (slot, _) in &self.inner {
            for counter in slot {
                let value = counter.load(Relaxed);
                counter.store(value >> shift, Relaxed);
            }
        }
    }

    /// Increment the frequency of the key without overflowing
    fn incr_no_overflow(counter: &AtomicU8) -> u8 {
        let mut value = counter.load(Relaxed);
        loop {
            if value == u8::MAX {
                return value;
            }
            match counter.compare_exchange_weak(value, value + 1, Acquire, Relaxed) {
                Ok(_) => return value,
                Err(val) => value = val,
            }
        }
    }
}

/// No doorkeeper LFU
pub struct TinyLFU {
    estimator: Estimator,
    window_counter: AtomicUsize,
    window_limit: usize,
}

impl TinyLFU {
    pub fn new(cache_size: usize) -> Self {
        let estimator = Estimator::new_optimal(cache_size);
        Self {
            window_counter: Default::default(),
            window_limit: cache_size * 8, // heuristic
            estimator,
        }
    }

    pub fn get(&mut self, key: Key) -> u8 {
        self.estimator.get(key)
    }

    pub fn incr(&mut self, key: Key) -> u8 {
        let current_window_counter = self.window_counter.fetch_add(1, Relaxed);
        if current_window_counter >= self.window_limit {
            // reset the counter and age the estimator
            self.window_counter.store(0, Relaxed);
            self.estimator.age(1);
        }
        self.estimator.incr(key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_optimal_params() {
        let (slots, hashes) = Estimator::optimal_params(1_000_000);
        // just smoke check some standard input
        assert_eq!(slots, 2718282);
        assert_eq!(hashes, 20);
    }

    #[test]
    fn test_sanity_estimator() {
        let mut estimator = Estimator::new_optimal(64);
        assert_eq!(estimator.get(1), 0);
        estimator.incr(1);
        assert_eq!(estimator.get(1), 1);
    }

    #[test]
    fn test_sanity_tinylfu() {
        let mut lfu = TinyLFU::new(64);
        assert_eq!(lfu.get(1), 0);
        lfu.incr(1);
        assert_eq!(lfu.get(1), 1);
    }
}
