use crate::tinylfu::estimator::Estimator;
use crate::tinylfu::types::Key;
use std::collections::VecDeque;
use std::marker::PhantomData;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize};
use t1ha::T1haHashMap;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const VEC_GROWTH_CAP: usize = 65536;
type Weight = u16;

const USES_CAP: u8 = 3;
/// Cache entry holds its data and metadata
struct Entry<T> {
    /// We limit uses to 3, TODO: find better implementation bit vector to fit 3
    uses: AtomicU8,
    queue: AtomicBool, // 0: small, 1: main
    weight: Weight,
    data: T,
}

impl<T> Entry<T> {
    pub(crate) fn new(data: T) -> Self {
        Self {
            uses: AtomicU8::new(0),
            queue: AtomicBool::new(false),
            weight: Default::default(),
            data,
        }
    }

    // Uses ----------------------------------------
    /// Increment the uses counter, return the new value
    pub(crate) fn incr_uses(&self) -> u8 {
        loop {
            let uses = self.uses();
            if uses >= USES_CAP {
                return uses;
            }

            if let Err(new_uses) = self.uses.compare_exchange(uses, uses + 1, Relaxed, Relaxed) {
                // someone else updated the uses
                if new_uses >= USES_CAP {
                    return new_uses;
                } // else retry
            } else {
                return uses + 1;
            }
        }
    }

    /// Decrement the uses counter, return the previous value
    pub(crate) fn decr_uses(&self) -> u8 {
        loop {
            let uses = self.uses();
            if uses == 0 {
                return uses;
            }

            if let Err(new_uses) = self.uses.compare_exchange(uses, uses - 1, Relaxed, Relaxed) {
                // someone else updated the uses
                if new_uses == 0 {
                    return new_uses;
                } // else retry
            } else {
                return uses;
            }
        }
    }

    /// Get the uses counter
    pub(crate) fn uses(&self) -> u8 {
        self.uses.load(Relaxed)
    }
}

// Experiment: We use S3FiFo https://s3fifo.com/ for admission policy
// TODO: Double check with your own queue performance with VecDeque
struct FifoQueues<T> {
    small: VecDeque<Key>, // Knob: 10% of the cache size
    small_weight: u16,

    main: VecDeque<Key>,
    main_weight: u16,

    _t: PhantomData<T>,
}

impl<T> FifoQueues<T> {
    pub(crate) fn new() -> Self {
        Self {
            small: VecDeque::new(),
            small_weight: 0,
            main: VecDeque::new(),
            main_weight: 0,
            _t: PhantomData,
        }
    }

    /// Admit a key to the fifos
    ///
    pub(crate) fn admit(&mut self, key: Key, data: T, cache: &mut T1haHashMap<Key, Entry<T>>) {}
}

/// TinyLFU cache
/// paper: https://arxiv.org/pdf/1512.00727.pdf
/// Tuning knobs based on dataset and hardware: evict_window,
struct TinyUFO<T> {
    capacity: usize,
    estimator: Estimator,
    window_counter: AtomicUsize,
    window_limit: usize,
    cache: T1haHashMap<Key, Entry<T>>, // hashmap data
    queues: FifoQueues<T>,

    // aging
    min_frequency: u8,
}

impl<T> TinyUFO<T> {
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
            queues: FifoQueues::new(),
        }
    }

    /// Get a value from the cache.
    pub fn get(&mut self, key: &Key) -> Option<&Entry<T>> {
        self.check_window();
        self.cache.get(key)
    }

    /// Set a key-value pair in the cache.
    ///
    /// Cache is fixed with capacity and it doesn't grow
    pub fn put(&mut self, key: Key, data: T) {
        self.queues.admit(key, data, &mut self.cache);
    }

    fn check_window(&mut self) {
        let window_counter = self.window_counter.fetch_add(1, Relaxed);

        if window_counter >= self.window_limit {
            self.window_counter.store(0, Relaxed);
            self.estimator.age(1);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sanity() {
        let mut cache = TinyUFO::with_capacity(64);
        cache.put(1, 1);
        cache.put(2, 2);
    }
}
