use crate::coordinator::{CoordinatorConfig, DbCoordinator, GlobalSnapshotManifest};
use crate::db_state::full_bucket_range;
use crate::error::{Error, Result};
use crate::{Config, Db, ReadOptions, WriteBatch, WriteOptions};
use bytes::Bytes;
use log::error;
use std::ops::Deref;
use std::sync::{Arc, Condvar, Mutex};

#[derive(Default)]
struct SingleNodeSnapshotState {
    in_flight: usize,
    closing: bool,
}

/// Single node database that proxies reads/writes and emits global snapshots.
pub struct SingleDb {
    db: Arc<Db>,
    coordinator: Arc<DbCoordinator>,
    total_buckets: u32,
    snapshot_state: Arc<Mutex<SingleNodeSnapshotState>>,
    snapshot_done: Arc<Condvar>,
}

impl SingleDb {
    pub fn open(config: Config) -> Result<Self> {
        let total_buckets = config.total_buckets;
        if total_buckets == 0 || total_buckets > (u16::MAX as u32) + 1 {
            return Err(Error::ConfigError(
                "total_buckets must be in range 1..=65536".to_string(),
            ));
        }
        let db = Arc::new(Db::open(
            config.clone(),
            std::iter::once(full_bucket_range(total_buckets)).collect(),
        )?);
        let coordinator = Arc::new(DbCoordinator::open(CoordinatorConfig::from_config(
            &config,
        ))?);
        Ok(Self {
            db,
            coordinator,
            total_buckets,
            snapshot_state: Arc::new(Mutex::new(SingleNodeSnapshotState::default())),
            snapshot_done: Arc::new(Condvar::new()),
        })
    }

    fn begin_snapshot_in_flight(&self) -> Result<()> {
        let mut state = self.snapshot_state.lock().unwrap();
        if state.closing {
            return Err(Error::InvalidState(
                "SingleDb is closing; cannot create new snapshot".to_string(),
            ));
        }
        state.in_flight += 1;
        Ok(())
    }

    fn finish_snapshot_in_flight(
        snapshot_state: &Arc<Mutex<SingleNodeSnapshotState>>,
        snapshot_done: &Arc<Condvar>,
    ) {
        let mut state = snapshot_state.lock().unwrap();
        state.in_flight = state.in_flight.saturating_sub(1);
        if state.in_flight == 0 {
            snapshot_done.notify_all();
        }
    }

    pub fn snapshot(&self) -> Result<u64> {
        self.snapshot_with_callback(|result| {
            if let Err(err) = result {
                error!("single node snapshot failed: {}", err);
            }
        })
    }

    pub fn snapshot_with_callback<F>(&self, callback: F) -> Result<u64>
    where
        F: Fn(Result<GlobalSnapshotManifest>) + Send + Sync + 'static,
    {
        self.begin_snapshot_in_flight()?;

        let global_snapshot_id = self.coordinator.allocate_snapshot_id();
        let coordinator = Arc::clone(&self.coordinator);
        let total_buckets = self.total_buckets;
        let snapshot_state = Arc::clone(&self.snapshot_state);
        let snapshot_done = Arc::clone(&self.snapshot_done);
        if let Err(err) = self.db.snapshot_with_callback(move |result| {
            let global_result = match result {
                Ok(shard_input) => materialize_global_snapshot(
                    &coordinator,
                    shard_input,
                    total_buckets,
                    global_snapshot_id,
                ),
                Err(err) => Err(err),
            };
            Self::finish_snapshot_in_flight(&snapshot_state, &snapshot_done);
            callback(global_result);
        }) {
            Self::finish_snapshot_in_flight(&self.snapshot_state, &self.snapshot_done);
            return Err(err);
        }
        Ok(global_snapshot_id)
    }

    /// Retain a global snapshot and its underlying local shard snapshot(s).
    pub fn retain_snapshot(&self, global_snapshot_id: u64) -> Result<bool> {
        let shard_snapshot_ids = self.local_shard_snapshot_ids(global_snapshot_id)?;
        for snapshot_id in &shard_snapshot_ids {
            if !self.db.retain_snapshot(*snapshot_id) {
                return Ok(false);
            }
        }
        Ok(self.coordinator.retain_snapshot(global_snapshot_id))
    }

    /// Expire a global snapshot and its underlying local shard snapshot(s).
    pub fn expire_snapshot(&self, global_snapshot_id: u64) -> Result<bool> {
        let shard_snapshot_ids = self.local_shard_snapshot_ids(global_snapshot_id)?;
        if !self.coordinator.expire_snapshot(global_snapshot_id)? {
            return Ok(false);
        }
        for snapshot_id in shard_snapshot_ids {
            let _ = self.db.expire_snapshot(snapshot_id)?;
        }
        Ok(true)
    }

    /// List global snapshots materialized by this single node coordinator.
    pub fn list_snapshots(&self) -> Result<Vec<GlobalSnapshotManifest>> {
        self.coordinator.list_global_snapshots()
    }

    /// Load one global snapshot by id.
    pub fn get_snapshot(&self, snapshot_id: u64) -> Result<GlobalSnapshotManifest> {
        self.coordinator.load_global_snapshot(snapshot_id)
    }

    pub fn db(&self) -> &Db {
        self.db.as_ref()
    }

    pub fn get(&self, bucket: u16, key: &[u8]) -> Result<Option<Vec<Option<Bytes>>>> {
        self.get_with_options(bucket, key, &ReadOptions::default())
    }

    pub fn get_with_options(
        &self,
        bucket: u16,
        key: &[u8],
        options: &ReadOptions,
    ) -> Result<Option<Vec<Option<Bytes>>>> {
        self.db.get_with_options(bucket, key, options)
    }

    pub fn scan<'a>(
        &'a self,
        bucket: u16,
        range: std::ops::Range<&[u8]>,
    ) -> Result<crate::DbIterator<'a>> {
        self.scan_with_options(bucket, range, &crate::ScanOptions::default())
    }

    pub fn scan_with_options<'a>(
        &'a self,
        bucket: u16,
        range: std::ops::Range<&[u8]>,
        options: &crate::ScanOptions,
    ) -> Result<crate::DbIterator<'a>> {
        self.db.scan_with_options(bucket, range, options)
    }

    pub fn put<K, V>(&self, bucket: u16, key: K, column: u16, value: V) -> Result<()>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        self.db.put(bucket, key, column, value)
    }

    pub fn put_with_options<K, V>(
        &self,
        bucket: u16,
        key: K,
        column: u16,
        value: V,
        options: &WriteOptions,
    ) -> Result<()>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        self.db
            .put_with_options(bucket, key, column, value, options)
    }

    pub fn delete<K>(&self, bucket: u16, key: K, column: u16) -> Result<()>
    where
        K: AsRef<[u8]>,
    {
        self.db.delete(bucket, key, column)
    }

    pub fn merge<K, V>(&self, bucket: u16, key: K, column: u16, value: V) -> Result<()>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        self.db.merge(bucket, key, column, value)
    }

    pub fn merge_with_options<K, V>(
        &self,
        bucket: u16,
        key: K,
        column: u16,
        value: V,
        options: &WriteOptions,
    ) -> Result<()>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        self.db
            .merge_with_options(bucket, key, column, value, options)
    }

    pub fn write_batch(&self, batch: WriteBatch) -> Result<()> {
        self.db.write_batch(batch)
    }

    pub fn set_time(&self, next: u32) {
        self.db.set_time(next)
    }

    pub fn close(&self) -> Result<()> {
        let mut state = self.snapshot_state.lock().unwrap();
        state.closing = true;
        while state.in_flight > 0 {
            state = self.snapshot_done.wait(state).unwrap();
        }
        drop(state);
        self.db.close()
    }
}

impl Deref for SingleDb {
    type Target = Db;

    fn deref(&self) -> &Self::Target {
        self.db.as_ref()
    }
}

fn materialize_global_snapshot(
    coordinator: &Arc<DbCoordinator>,
    shard_input: crate::coordinator::ShardSnapshotInput,
    total_buckets: u32,
    global_snapshot_id: u64,
) -> Result<GlobalSnapshotManifest> {
    let global_snapshot = coordinator.take_global_snapshot_with_id(
        total_buckets,
        vec![shard_input],
        global_snapshot_id,
    )?;
    coordinator.materialize_global_snapshot(&global_snapshot)?;
    Ok(global_snapshot)
}

impl SingleDb {
    fn local_shard_snapshot_ids(&self, global_snapshot_id: u64) -> Result<Vec<u64>> {
        let global = self.coordinator.load_global_snapshot(global_snapshot_id)?;
        let db_id = self.db.id();
        let shard_ids: Vec<u64> = global
            .shard_snapshots
            .into_iter()
            .filter(|shard| shard.db_id == db_id)
            .map(|shard| shard.snapshot_id)
            .collect();
        if shard_ids.is_empty() {
            return Err(Error::InvalidState(format!(
                "Global snapshot {} has no shard snapshot for db {}",
                global_snapshot_id, db_id
            )));
        }
        Ok(shard_ids)
    }
}
