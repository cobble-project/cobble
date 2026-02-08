use crate::lsm::LSMTreeVersion;
use crate::memtable::{ActiveMemtable, ImmutableMemtable};
use arc_swap::ArcSwap;
use std::collections::VecDeque;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Condvar, Mutex};

pub(crate) struct DbState {
    pub(crate) seq_id: u64,
    pub(crate) lsm_version: LSMTreeVersion,
    pub(crate) active: Option<Arc<Mutex<ActiveMemtable>>>,
    pub(crate) immutables: VecDeque<ImmutableMemtable>,
}

impl DbState {
    /// Create the next DbState with incremented seq_id
    pub(crate) fn new(
        handle: &DbStateHandle,
        lsm_version: LSMTreeVersion,
        active_memtable: Option<Arc<Mutex<ActiveMemtable>>>,
        immutable_memtable: VecDeque<ImmutableMemtable>,
    ) -> Self {
        Self {
            seq_id: handle.allocate_seq_id(),
            lsm_version,
            active: active_memtable,
            immutables: immutable_memtable,
        }
    }
}

pub(crate) struct DbStateHandle {
    current: ArcSwap<DbState>,
    lock: Mutex<()>,
    next_seq_id: AtomicU64,
    changed: Condvar,
}

impl DbStateHandle {
    pub(crate) fn new() -> Self {
        Self {
            current: ArcSwap::from_pointee(DbState {
                seq_id: 0,
                lsm_version: LSMTreeVersion { levels: vec![] },
                active: None,
                immutables: VecDeque::new(),
            }),
            lock: Mutex::new(()),
            next_seq_id: AtomicU64::new(1),
            changed: Condvar::new(),
        }
    }

    pub(crate) fn allocate_seq_id(&self) -> u64 {
        self.next_seq_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    pub(crate) fn load(&self) -> Arc<DbState> {
        self.current.load_full()
    }

    pub(crate) fn lock(&self) -> std::sync::MutexGuard<'_, ()> {
        self.lock.lock().unwrap()
    }

    pub(crate) fn wait_for_change<'a>(
        &self,
        guard: std::sync::MutexGuard<'a, ()>,
    ) -> std::sync::MutexGuard<'a, ()> {
        self.changed.wait(guard).unwrap()
    }

    pub(crate) fn cas_mutate<F>(&self, expected: u64, f: F) -> bool
    where
        F: FnOnce(&DbStateHandle, &DbState) -> Option<DbState>,
    {
        let current = self.load();
        if current.seq_id != expected {
            return false;
        }
        let Some(new_version) = f(self, &current) else {
            return true;
        };
        self.store(new_version);
        true
    }

    pub(crate) fn store(&self, new_version: DbState) {
        self.current.store(Arc::new(new_version));
        self.changed.notify_all();
    }
}
