use crate::tinyufo::estimator::TinyLFU;
use crate::tinyufo::types::Key;
use std::collections::VecDeque;
use std::hash::{BuildHasher, Hash};
use std::marker::PhantomData;
use std::sync::atomic::Ordering::{Relaxed, SeqCst};
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize};
use t1ha::T1haHashMap;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const VEC_GROWTH_CAP: usize = 65536;
const USES_CAP: u8 = 3;

type Weight = u16;

const SMALL: bool = false;
const MAIN: bool = true;

/// Cache entry holds its data and metadata
struct Entry<T> {
    /// We limit uses to 3, TODO: find better implementation bit vector to fit 3
    pub uses: AtomicU8,
    pub queue: AtomicBool,
    // 0: small, 1: main
    pub weight: Weight,
    pub data: T,
}

impl<T> Entry<T> {
    pub(crate) fn new(data: T) -> Self {
        Self {
            uses: AtomicU8::new(1),
            queue: AtomicBool::new(SMALL),
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

    /// Move the entry to the main queue
    pub(crate) fn move_to_main(&self) {
        self.queue.store(MAIN, Relaxed);
    }
}

struct EvictedEntry<T> {
    pub key: Key,
    // hashed key
    pub data: T,
    pub weight: Weight,
}

const SMALL_QUEUE_PERCENTAGE: f32 = 0.1;

// Experiment: We use S3FiFo https://s3fifo.com/ for admission policy
// TODO: Double check with your own queue performance with VecDeque
struct FifoQueues<T: Clone> {
    small: VecDeque<Key>,
    // 10% of the cache
    small_weight: AtomicUsize,
    main: VecDeque<Key>,
    main_weight: AtomicUsize,
    estimator: TinyLFU, // as ghost queue

    small_weight_limit: usize,
    total_weight_limit: usize,

    _t: PhantomData<T>,
}

impl<T> FifoQueues<T>
where
    T: Clone,
{
    pub(crate) fn new(total_weight_limit: usize, capacity: usize) -> Self {
        let small_weight_limit =
            (total_weight_limit as f32 * SMALL_QUEUE_PERCENTAGE).floor() as usize + 1;
        Self {
            small: VecDeque::with_capacity(capacity / 10), // 10% of the cache (heuristic
            small_weight: Default::default(),
            main: VecDeque::with_capacity(capacity),
            main_weight: Default::default(),
            estimator: TinyLFU::new(capacity),
            total_weight_limit,
            small_weight_limit,
            _t: PhantomData,
        }
    }

    /// Admit a key to the fifos
    pub(crate) fn admit(
        &mut self,
        key: Key,
        weight: Weight,
        data: T,
        cache: &mut T1haHashMap<Key, Entry<T>>,
    ) {
        if let Some(current_entry) = cache.get(&key) {
            // if the key is already in the cache, we just increment the uses
            current_entry.incr_uses();
        } else {
            let mut new_entry = Entry::new(data);

            let evicts = self.try_evict(weight, cache);
            if evicts.is_empty() {
                // nothing is evicted, we can insert the new entry
                new_entry.weight = weight;
            } else {
                // tinylfu: we check evicted entry and new one, if the new one has higher freq,
                // we insert it, otherwise we put back the evicted entry
                let new_freq = self.estimator.incr(key);
                let evicted_first = &evicts[0];
                let evicted_freq = self.estimator.get(evicted_first.key);
                if evicted_freq < new_freq {
                    new_entry.weight = weight;
                } else {
                    // new_entry.queue.store(SMALL, Relaxed); // default: insert it back to small, TODO
                    new_entry.weight = evicted_first.weight;
                }
            }
            // TODO: multithread checking
            // for all the cases, we insert new_entry to small
            let _ = cache.insert(key, new_entry);
            self.small.push_back(key);
            self.small_weight.fetch_add(weight as usize, SeqCst);
        }
    }

    /// Try to evict as many entries as possible to make room for the new entry.
    fn try_evict(
        &mut self,
        weight: Weight,
        cache: &mut T1haHashMap<Key, Entry<T>>,
    ) -> Vec<EvictedEntry<T>> {
        let mut evicted = if self.total_weight_limit
            < self.small_weight.load(SeqCst) + self.main_weight.load(SeqCst)
        {
            Vec::with_capacity(1)
        } else {
            vec![]
        };

        while self.total_weight_limit
            < self.small_weight.load(SeqCst) + self.main_weight.load(SeqCst)
        {
            if let Some(evicted_item) = self.evict_one(cache) {
                evicted.push(evicted_item);
            } else {
                break;
            }
        }
        evicted
    }

    /// Evict one entry from the cache
    ///
    /// Algorithm: we will try to evict from small first then main.
    fn evict_one(&mut self, cache: &mut T1haHashMap<Key, Entry<T>>) -> Option<EvictedEntry<T>> {
        if self.small_weight.load(SeqCst) > self.small_weight_limit {
            if let Some(evicted) = self.evict_small(cache) {
                return Some(evicted);
            }
        }

        self.evict_main(cache)
    }

    /// Evict one entry from the small queue
    fn evict_small(&mut self, cache: &mut T1haHashMap<Key, Entry<T>>) -> Option<EvictedEntry<T>> {
        loop {
            let to_evict = self.small.pop_front()?;

            if let Some(entry) = cache.get_mut(&to_evict) {
                if entry.uses() > 1 {
                    entry.move_to_main();
                    self.main.push_back(to_evict);
                    self.main_weight.fetch_add(entry.weight as usize, SeqCst);
                    continue;
                }
                let weight = entry.weight;
                let data = entry.data.clone();
                cache.remove(&to_evict);
                self.small_weight.fetch_sub(weight as usize, SeqCst);
                return Some(EvictedEntry {
                    key: to_evict,
                    data,
                    weight,
                });
            }
            return None;
        }
    }

    /// Evict one entry from the main queue
    fn evict_main(&mut self, cache: &mut T1haHashMap<Key, Entry<T>>) -> Option<EvictedEntry<T>> {
        loop {
            let to_evict = self.main.pop_front()?;

            if let Some(entry) = cache.get_mut(&to_evict) {
                // we decr the use, if it's still in use, we move it back to the main queue
                if entry.decr_uses() > 0 {
                    self.main.push_back(to_evict);
                    continue;
                }
                let weight = entry.weight;
                let data = entry.data.clone();
                cache.remove(&to_evict);
                self.main_weight.fetch_sub(weight as usize, SeqCst);
                return Some(EvictedEntry {
                    key: to_evict,
                    data,
                    weight,
                });
            }

            return None;
        }
    }
}

fn update_weight_atomic(weight: &AtomicUsize, old: u16, new: u16) {
    let diff = new.abs_diff(old);
    if diff == 0 {
        return;
    }

    if new > old {
        weight.fetch_add(diff as usize, SeqCst);
    } else {
        weight.fetch_sub(diff as usize, SeqCst);
    }
}

/// TinyLFU cache
/// paper: https://arxiv.org/pdf/1512.00727.pdf
/// Tuning knobs based on dataset and hardware: evict_window,
struct TinyUFO<K, T>
where
    T: Clone,
{
    capacity: usize,
    cache: T1haHashMap<Key, Entry<T>>,
    // storage backend
    queues: FifoQueues<T>,

    _k: PhantomData<K>,
}

impl<K: Hash, T: Clone> TinyUFO<K, T> {
    /// Create a new TinyLFU cache with a given capacity.
    pub fn new(total_weight_limit: usize, capacity: usize) -> Self {
        Self {
            cache: T1haHashMap::with_capacity_and_hasher(capacity, Default::default()),
            capacity,
            queues: FifoQueues::new(total_weight_limit, capacity),

            _k: PhantomData,
        }
    }

    /// Get a value from the cache.
    pub fn get(&mut self, key: &K) -> Option<&T> {
        let hashed_key = self.cache.hasher().hash_one(key);
        return if let Some(entry) = self.cache.get(&hashed_key) {
            entry.incr_uses();
            Some(&entry.data)
        } else {
            None
        };
    }

    /// Set a key-value pair in the cache.
    ///
    /// Cache is fixed with capacity and it doesn't grow
    pub fn put(&mut self, key: K, weight: Weight, data: T) {
        let hashed_key = self.cache.hasher().hash_one(&key);
        self.queues.admit(hashed_key, weight, data, &mut self.cache);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sanity() {
        let items = 100;
        let mut cache = TinyUFO::new(100, 10);
        cache.put(1, 1, 1);
        cache.put(2, 2, 1);
    }
}
