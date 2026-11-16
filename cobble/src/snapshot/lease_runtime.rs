//! Runtime roots and I/O adapter for the shared snapshot lease collector.
use super::SnapshotManager;
use super::lease::{ImportLeases, LeaseCleanupMode, LeaseMetadata};
use crate::db_state::DbStateHandle;
use crate::file::{FileManager, ReadAllFile};
use crate::lsm::LSMTree;
use crate::rescale_protocol::{
    IMPORT_RECORD_DIR, ImportStart, export_lease_name, has_import_starts, load_import_end,
    snapshot_source_file_manager,
};
use crate::runtime_manifest::publisher::RuntimeManifestPublisherHandle;
use crate::{Config, Result};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};
use tokio::sync::Notify;

pub(super) struct RuntimeLeaseCleanup {
    db_id: String,
    config: Config,
    state: Arc<DbStateHandle>,
    lsm: Weak<LSMTree>,
    publisher: Option<Weak<RuntimeManifestPublisherHandle>>,
    enabled: AtomicBool,
    pending: AtomicBool,
    wake: Option<Arc<Notify>>,
}

impl SnapshotManager {
    pub(crate) fn install_import_cleanup(
        &self,
        db_id: String,
        config: Config,
        state: Arc<DbStateHandle>,
        lsm: &Arc<LSMTree>,
        publisher: Option<&Arc<RuntimeManifestPublisherHandle>>,
        wake: Option<Arc<Notify>>,
    ) {
        assert!(
            self.import_cleanup
                .set(RuntimeLeaseCleanup {
                    db_id,
                    config,
                    state,
                    lsm: Arc::downgrade(lsm),
                    publisher: publisher.map(Arc::downgrade),
                    enabled: AtomicBool::new(false),
                    pending: AtomicBool::new(false),
                    wake,
                })
                .is_ok(),
            "snapshot import cleanup already installed"
        );
    }

    pub(crate) fn enable_import_cleanup(&self) {
        if let Some(context) = self.import_cleanup.get() {
            // Restore must load every retained snapshot before the startup scan.
            // Check even when no new import was started, including for orphan ENDs.
            context.enabled.store(true, Ordering::Release);
            self.activate_import_cleanup();
        }
    }

    pub(crate) fn activate_import_cleanup(&self) {
        if let Some(context) = self.import_cleanup.get() {
            context.pending.store(true, Ordering::Release);
            self.request_import_cleanup();
        }
    }

    pub(super) fn request_import_cleanup(&self) {
        if let Some(context) = self.import_cleanup.get()
            && context.enabled.load(Ordering::Acquire)
            && context.pending.load(Ordering::Acquire)
            && let Some(wake) = &context.wake
        {
            wake.notify_one();
        }
    }

    /// Run adoption and reclamation on the existing tiering worker only while needed.
    pub(crate) fn poll_imports(&self, advance: impl FnOnce() -> Result<()>) -> Result<()> {
        let Some(context) = self.import_cleanup.get() else {
            return Ok(());
        };
        if !context.enabled.load(Ordering::Acquire)
            || !context.pending.swap(false, Ordering::AcqRel)
        {
            return Ok(());
        }
        let remaining = advance()
            .and_then(|()| self.reclaim_import_leases())
            .and_then(|()| has_import_starts(&self.file_manager));
        // Only re-arm here; clearing after the scan could lose a new import's activation.
        // Leave live references and failures to retry on the normal worker interval.
        if !matches!(remaining, Ok(false)) {
            context.pending.store(true, Ordering::Release);
        }
        remaining.map(|_| ())
    }

    /// Executed serially by the existing tiering worker; failures retain START for retry.
    pub(crate) fn reclaim_import_leases(&self) -> Result<()> {
        let Some(context) = self.import_cleanup.get() else {
            return Ok(());
        };
        if !context.enabled.load(Ordering::Acquire) {
            return Ok(());
        }
        let _access = self.db_lifecycle.begin_owned_access()?;
        let metadata = RuntimeMetadata {
            manager: &self.file_manager,
            config: &context.config,
        };
        let imports = ImportLeases::load(&metadata, &context.db_id)?;
        if imports.starts.is_empty() {
            return imports.reclaim(&metadata, &Default::default(), LeaseCleanupMode::Online);
        }
        let Some(lsm) = context.lsm.upgrade() else {
            return Ok(());
        };
        if lsm.has_pending_compactions()
            || crate::compaction::dedicated::has_active_dedicated_compaction(&self.file_manager)?
        {
            return Ok(());
        }
        if let Some(publisher) = &context.publisher {
            let Some(publisher) = publisher.upgrade() else {
                return Ok(());
            };
            publisher.publish_at_least(context.state.load().seq_id)?;
        }
        for record in &imports.starts {
            if load_import_end(&self.file_manager, &record.export_id)?.is_some()
                && let Some(id) = record.import_snapshot_id
                && !self.is_retained(id)
            {
                let _ = self.expire_snapshot(id)?;
            }
        }
        let mut referenced = self.snapshot_lease_references();
        referenced.extend(self.file_manager.referenced_import_leases(&context.state));
        imports.reclaim(&metadata, &referenced, LeaseCleanupMode::Online)
    }
}

struct RuntimeMetadata<'a> {
    manager: &'a FileManager,
    config: &'a Config,
}
impl LeaseMetadata for RuntimeMetadata<'_> {
    fn import_names(&self) -> Result<Vec<String>> {
        self.manager.list_metadata_names(IMPORT_RECORD_DIR)
    }
    fn read(&self, name: &str) -> Result<Option<Vec<u8>>> {
        if !self.manager.metadata_file_exists_untracked(name)? {
            return Ok(None);
        }
        Ok(Some(
            self.manager
                .open_metadata_file_reader_untracked(name)?
                .read_all()?
                .to_vec(),
        ))
    }
    fn remove(&self, name: &str) -> Result<()> {
        self.manager.remove_metadata_file(name)
    }
    fn release_source(&self, record: &ImportStart) -> Result<()> {
        let (source, _) = snapshot_source_file_manager(
            self.config,
            &record.source_db_id,
            &record.source_manifest_path,
        )?;
        source.remove_metadata_file(&export_lease_name(&record.export_id))
    }
}
