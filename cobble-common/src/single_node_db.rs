use crate::coordinator::{CoordinatorConfig, DbCoordinator};
use crate::error::{Error, Result};
use crate::{Config, Db, ReadOptions, WriteBatch};
use bytes::Bytes;
use log::error;
use std::ops::Deref;
use std::sync::Arc;

/// Single node database that proxies reads/writes and emits global snapshots.
pub struct SingleNodeDb {
    db: Arc<Db>,
    coordinator: Arc<DbCoordinator>,
    total_buckets: u16,
}

impl SingleNodeDb {
    pub fn open(config: Config) -> Result<Self> {
        let total_buckets = config.total_buckets;
        if total_buckets == 0 {
            return Err(Error::ConfigError(
                "total_buckets must be greater than 0".to_string(),
            ));
        }
        let db = Arc::new(Db::open(
            config.clone(),
            std::iter::once(0u16..total_buckets).collect(),
        )?);
        let coordinator = Arc::new(DbCoordinator::open(CoordinatorConfig::from_config(
            &config,
        ))?);
        Ok(Self {
            db,
            coordinator,
            total_buckets,
        })
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
        F: Fn(Result<u64>) + Send + Sync + 'static,
    {
        let global_snapshot_id = self.coordinator.allocate_snapshot_id();
        let db = Arc::clone(&self.db);
        let coordinator = Arc::clone(&self.coordinator);
        let total_buckets = self.total_buckets;
        self.db.snapshot_with_callback(move |result| {
            let global_result = match result {
                Ok(snapshot_id) => materialize_global_snapshot(
                    &coordinator,
                    &db,
                    snapshot_id,
                    total_buckets,
                    global_snapshot_id,
                ),
                Err(err) => Err(err),
            };
            callback(global_result);
        })?;
        Ok(global_snapshot_id)
    }

    pub fn db(&self) -> &Db {
        self.db.as_ref()
    }

    pub fn get(
        &self,
        bucket: u16,
        key: &[u8],
        options: &ReadOptions,
    ) -> Result<Option<Vec<Option<Bytes>>>> {
        self.db.get(bucket, key, options)
    }

    pub fn put<K, V>(&self, bucket: u16, key: K, column: u16, value: V) -> Result<()>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        self.db.put(bucket, key, column, value)
    }

    pub fn put_with_ttl<K, V>(
        &self,
        bucket: u16,
        key: K,
        column: u16,
        value: V,
        ttl_seconds: Option<u32>,
    ) -> Result<()>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        self.db
            .put_with_ttl(bucket, key, column, value, ttl_seconds)
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

    pub fn merge_with_ttl<K, V>(
        &self,
        bucket: u16,
        key: K,
        column: u16,
        value: V,
        ttl_seconds: Option<u32>,
    ) -> Result<()>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        self.db
            .merge_with_ttl(bucket, key, column, value, ttl_seconds)
    }

    pub fn write_batch(&self, batch: WriteBatch) -> Result<()> {
        self.db.write_batch(batch)
    }

    pub fn set_time(&self, next: u32) {
        self.db.set_time(next)
    }

    pub fn close(&self) -> Result<()> {
        self.db.close()
    }
}

impl Deref for SingleNodeDb {
    type Target = Db;

    fn deref(&self) -> &Self::Target {
        self.db.as_ref()
    }
}

fn materialize_global_snapshot(
    coordinator: &Arc<DbCoordinator>,
    db: &Arc<Db>,
    snapshot_id: u64,
    total_buckets: u16,
    global_snapshot_id: u64,
) -> Result<u64> {
    // materialize global snapshot from bucket snapshot
    let bucket_snapshot = db.bucket_snapshot_input(snapshot_id)?;
    let global_snapshot = coordinator.take_global_snapshot_with_id(
        total_buckets,
        vec![bucket_snapshot],
        global_snapshot_id,
    )?;
    coordinator.materialize_global_snapshot(&global_snapshot)?;
    Ok(global_snapshot.id)
}
