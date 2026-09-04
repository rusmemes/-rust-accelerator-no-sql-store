use crate::common::{PARTITIONS_AMOUNT, PartitionId, now_millis};
use crossbeam_skiplist::SkipMap;
use dashmap::DashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

#[derive(Debug, Clone)]
pub struct Record {
    pub expiration_time_ms: u64,
    pub creation_time_ms: u64,
    pub value: Vec<u8>,
}

#[derive(Debug, Clone)]
pub enum Mutation {
    Put(Arc<Record>),
    Delete { deletion_time_ms: u64 },
}

impl Mutation {
    fn version(&self) -> u64 {
        match self {
            Self::Put(record) => record.creation_time_ms,
            Self::Delete { deletion_time_ms } => *deletion_time_ms,
        }
    }
}

#[derive(Default, Clone)]
pub struct RuntimeStore {
    cache: Arc<DashMap<PartitionId, PartitionStore>>,
    next_revision: Arc<AtomicU64>,
}

#[derive(Default)]
struct PartitionStore {
    records: SkipMap<Key, Mutation>,
    changes: SkipMap<u64, Key>,
    active_writers: AtomicUsize,
    revision: AtomicU64,
}

struct WriterGuard<'a> {
    partition: &'a PartitionStore,
    next_revision: &'a AtomicU64,
    changed_key: Option<Key>,
}

impl Drop for WriterGuard<'_> {
    fn drop(&mut self) {
        let revision = self.next_revision.fetch_add(1, Ordering::SeqCst) + 1;
        if let Some(key) = self.changed_key {
            self.partition.changes.insert(revision, key);
        }
        self.partition.revision.store(revision, Ordering::SeqCst);
        self.partition.active_writers.fetch_sub(1, Ordering::SeqCst);
    }
}

impl PartitionStore {
    fn writer<'a>(
        &'a self,
        next_revision: &'a AtomicU64,
        changed_key: Option<Key>,
    ) -> WriterGuard<'a> {
        self.active_writers.fetch_add(1, Ordering::SeqCst);
        WriterGuard {
            partition: self,
            next_revision,
            changed_key,
        }
    }

    fn stable_revision(&self) -> Option<u64> {
        let revision_before = self.revision.load(Ordering::SeqCst);
        if self.active_writers.load(Ordering::SeqCst) != 0 {
            return None;
        }
        let revision_after = self.revision.load(Ordering::SeqCst);
        let writers_after = self.active_writers.load(Ordering::SeqCst);
        (revision_before == revision_after && writers_after == 0).then_some(revision_after)
    }
}

impl RuntimeStore {
    pub fn new() -> Self {
        Self {
            cache: Default::default(),
            next_revision: Default::default(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Key(pub u64);

impl Key {
    pub fn partition(&self) -> PartitionId {
        PartitionId((self.0 as usize % PARTITIONS_AMOUNT) as u16)
    }
}

impl RuntimeStore {
    pub fn get_partition_mutations(
        &self,
        partition: &PartitionId,
        amount: usize,
        after_key: Option<&Key>,
    ) -> Vec<(Key, Mutation)> {
        let Some(partition) = self.cache.get(partition) else {
            return vec![];
        };
        partition
            .records
            .iter()
            .skip_while(|entry| after_key.is_some_and(|key| entry.key() <= key))
            .take(amount)
            .map(|entry| (*entry.key(), entry.value().clone()))
            .collect()
    }

    /// Returns a revision only when no mutation of this partition is in flight.
    pub fn stable_partition_revision(&self, partition: &PartitionId) -> Option<u64> {
        match self.cache.get(partition) {
            Some(entry) => entry.stable_revision(),
            None => Some(0),
        }
    }

    pub fn get_partition_changes(
        &self,
        partition: &PartitionId,
        after_revision: u64,
        amount: usize,
    ) -> Option<(u64, Vec<(Key, Mutation)>)> {
        let partition = self.cache.get(partition)?;
        let changes: Vec<_> = partition
            .changes
            .iter()
            .skip_while(|entry| *entry.key() <= after_revision)
            .take(amount)
            .map(|entry| (*entry.key(), *entry.value()))
            .collect();
        let last_revision = changes.last()?.0;
        let records = changes
            .into_iter()
            .filter_map(|(_, key)| {
                partition
                    .records
                    .get(&key)
                    .map(|entry| (key, entry.value().clone()))
            })
            .collect();
        Some((last_revision, records))
    }

    pub fn prune_partition_changes(&self, partition: &PartitionId, through_revision: u64) {
        if let Some(partition) = self.cache.get(partition) {
            while let Some(entry) = partition.changes.front() {
                if *entry.key() > through_revision {
                    break;
                }
                entry.remove();
            }
        }
    }

    pub fn remove_partition_if_empty(&self, partition: PartitionId) {
        if let dashmap::mapref::entry::Entry::Occupied(occupied) = self.cache.entry(partition)
            && occupied.get().records.is_empty()
        {
            occupied.remove();
        }
    }

    pub fn remove_partition(&self, partition: &PartitionId) {
        self.cache.remove(&partition);
    }

    pub fn delete_at(&self, key: Key, deletion_time_ms: u64) {
        let partition = key.partition();
        let map = self.cache.entry(partition).or_default();
        let _writer = map.writer(&self.next_revision, Some(key));
        let mutation = Mutation::Delete { deletion_time_ms };
        map.records.compare_insert(key, mutation.clone(), |old| {
            old.version() <= mutation.version()
        });
    }

    pub fn get_versioned(&self, key: Key) -> Option<Mutation> {
        let partition = key.partition();

        let mut needs_partition_cleanup = false;
        let res = if let Some(sorted_map) = self.cache.get(&partition) {
            if let Some(entry) = sorted_map.records.get(&key) {
                let Mutation::Put(record) = entry.value() else {
                    return Some(entry.value().clone());
                };
                let exp_time = record.expiration_time_ms;
                if exp_time == 0 || exp_time > now_millis() {
                    Some(Mutation::Put(record.clone()))
                } else {
                    let _writer = sorted_map.writer(&self.next_revision, None);
                    entry.remove();
                    needs_partition_cleanup = true;
                    None
                }
            } else {
                None
            }
        } else {
            None
        };

        if needs_partition_cleanup {
            self.remove_partition_if_empty(partition);
        }
        res
    }

    #[cfg(test)]
    pub fn get(&self, key: Key) -> Option<Arc<Record>> {
        match self.get_versioned(key) {
            Some(Mutation::Put(record)) => Some(record),
            Some(Mutation::Delete { .. }) | None => None,
        }
    }

    pub fn put(&self, key: Key, record: Record) {
        let partition = key.partition();
        let sorted_map = self.cache.entry(partition).or_default();
        let _writer = sorted_map.writer(&self.next_revision, Some(key));
        let record = Arc::new(record);
        let mutation = Mutation::Put(record.clone());
        sorted_map
            .records
            .compare_insert(key, mutation.clone(), |old| {
                old.version() <= mutation.version()
            });
    }

    /// Removes every record expired at or before `now_ms`.
    ///
    /// `DashMap` only locks one shard while a partition is looked up, and
    /// `SkipMap` supports concurrent traversal and removal. Consequently this
    /// scan never takes a store-wide lock and can run alongside reads/writes.
    pub fn remove_expired(&self, now_ms: u64) -> usize {
        let partition_ids: Vec<_> = self.cache.iter().map(|entry| *entry.key()).collect();
        let mut removed = 0;

        for partition_id in partition_ids {
            if let Some(partition) = self.cache.get(&partition_id) {
                for entry in partition.records.iter() {
                    let Mutation::Put(record) = entry.value() else {
                        continue;
                    };
                    let expiration_time_ms = record.expiration_time_ms;
                    if expiration_time_ms != 0 && expiration_time_ms <= now_ms {
                        let _writer = partition.writer(&self.next_revision, None);
                        entry.remove();
                        removed += 1;
                    }
                }
            }

            self.remove_partition_if_empty(partition_id);
        }

        removed
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_runtime_store_expiration() {
        let store = RuntimeStore::new();
        let key = Key(1);
        let value = vec![1, 2, 3];

        let now = now_millis();
        store.put(
            key,
            Record {
                expiration_time_ms: now + 100,
                creation_time_ms: now,
                value: value.clone(),
            },
        );

        let record = store.get(key).expect("Record should be present");
        assert_eq!(record.value, value);

        std::thread::sleep(Duration::from_millis(150));

        assert!(store.get(key).is_none());

        let partition = key.partition();
        assert!(!store.cache.contains_key(&partition));
    }

    #[test]
    fn test_remove_partition_if_empty() {
        let store = RuntimeStore::new();
        let key = Key(1);
        let partition = key.partition();

        let now = now_millis();
        store.put(
            key,
            Record {
                expiration_time_ms: now + 1000,
                creation_time_ms: now,
                value: vec![1],
            },
        );
        assert!(store.cache.contains_key(&partition));

        store.remove_partition_if_empty(partition);
        assert!(store.cache.contains_key(&partition));

        std::thread::sleep(Duration::from_millis(1100));
        assert!(store.get(key).is_none());

        assert!(!store.cache.contains_key(&partition));
    }

    #[test]
    fn test_runtime_store_delete() {
        let store = RuntimeStore::new();
        let key = Key(1);
        let partition = key.partition();

        store.put(
            key,
            Record {
                value: vec![1, 2, 3],
                expiration_time_ms: 0,
                creation_time_ms: 0,
            },
        );
        assert!(store.cache.contains_key(&partition));
        assert!(store.get(key).is_some());

        store.delete_at(key, now_millis());
        assert!(store.get(key).is_none());
        assert!(store.cache.contains_key(&partition));
    }

    #[test]
    fn tombstone_prevents_stale_put_but_allows_newer_put() {
        let store = RuntimeStore::new();
        let key = Key(1);

        store.delete_at(key, 200);
        store.put(
            key,
            Record {
                value: vec![1],
                expiration_time_ms: 0,
                creation_time_ms: 100,
            },
        );
        assert!(store.get(key).is_none());

        store.put(
            key,
            Record {
                value: vec![2],
                expiration_time_ms: 0,
                creation_time_ms: 300,
            },
        );
        assert_eq!(store.get(key).unwrap().value, vec![2]);
    }

    #[test]
    fn test_runtime_store_put_ordering() {
        let store = RuntimeStore::new();
        let key = Key(1);

        store.put(
            key,
            Record {
                value: vec![1],
                expiration_time_ms: 0,
                creation_time_ms: 100,
            },
        );
        assert_eq!(store.get(key).unwrap().value, vec![1]);

        store.put(
            key,
            Record {
                value: vec![2],
                expiration_time_ms: 0,
                creation_time_ms: 50,
            },
        );
        assert_eq!(store.get(key).unwrap().value, vec![1]);

        store.put(
            key,
            Record {
                value: vec![3],
                expiration_time_ms: 0,
                creation_time_ms: 150,
            },
        );

        assert_eq!(store.get(key).unwrap().value, vec![3]);

        store.put(
            key,
            Record {
                value: vec![4],
                expiration_time_ms: 0,
                creation_time_ms: 150,
            },
        );
        assert_eq!(store.get(key).unwrap().value, vec![4]);
    }

    #[test]
    fn test_runtime_store_no_expiration() {
        let store = RuntimeStore::new();
        let key = Key(1);
        let now = now_millis();

        store.put(
            key,
            Record {
                value: vec![1],
                expiration_time_ms: 0,
                creation_time_ms: now,
            },
        );
        std::thread::sleep(Duration::from_millis(100));
        assert!(store.get(key).is_some());
    }

    #[test]
    fn test_runtime_store_concurrent_ops() {
        let store = Arc::new(RuntimeStore::new());
        let mut handles = vec![];

        for i in 0..100 {
            let store_clone = store.clone();
            handles.push(std::thread::spawn(move || {
                for j in 0..1000 {
                    let key = Key((i * 1000 + j) as u64);
                    store_clone.put(
                        key,
                        Record {
                            value: vec![1],
                            expiration_time_ms: 0,
                            creation_time_ms: 0,
                        },
                    );
                    if j % 2 == 0 {
                        store_clone.delete_at(key, 1);
                    }
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        let mut total_count = 0;
        for i in 0..PARTITIONS_AMOUNT {
            total_count += store
                .cache
                .get(&PartitionId(i as u16))
                .map(|partition| {
                    partition
                        .records
                        .iter()
                        .filter(|entry| matches!(entry.value(), Mutation::Put(_)))
                        .count()
                })
                .unwrap_or(0);
        }
        assert_eq!(total_count, 50000);
    }

    #[test]
    fn remove_expired_scans_all_partitions_and_keeps_live_records() {
        let store = RuntimeStore::new();
        let expired_one = Key(1);
        let expired_two = Key(PARTITIONS_AMOUNT as u64 + 2);
        let live = Key(3);
        let immortal = Key(4);

        for (key, expiration_time_ms) in [
            (expired_one, 99),
            (expired_two, 100),
            (live, 101),
            (immortal, 0),
        ] {
            store.put(
                key,
                Record {
                    expiration_time_ms,
                    creation_time_ms: 1,
                    value: vec![key.0 as u8],
                },
            );
        }

        assert_eq!(store.remove_expired(100), 2);
        assert!(
            store
                .cache
                .get(&expired_one.partition())
                .is_none_or(|partition| partition.records.get(&expired_one).is_none())
        );
        assert!(
            store
                .cache
                .get(&expired_two.partition())
                .is_none_or(|partition| partition.records.get(&expired_two).is_none())
        );
        assert!(
            store
                .cache
                .get(&live.partition())
                .unwrap()
                .records
                .get(&live)
                .is_some()
        );
        assert!(
            store
                .cache
                .get(&immortal.partition())
                .unwrap()
                .records
                .get(&immortal)
                .is_some()
        );
    }
}
