use super::Db;
use crate::db_status::CloseTransition;
use crate::{DeletionStatus, DeletionWait, Error, Result};
use log::{error, warn};

impl Db {
    pub(crate) fn force_close(&self) {
        if let Some(wal_writer) = &self.wal_writer {
            wal_writer.force_close();
        }
        if let Some(worker) = &self.primary_tiering_worker {
            worker.stop();
            worker.join();
        }
        if let Some(publisher) = &self.runtime_manifest_publisher {
            publisher.stop();
            publisher.join();
        }
        if let Some(poller) = &self.dedicated_poller {
            poller.stop();
        }
        self.memtable_manager.force_close();
        self.lsm_tree.shutdown_compaction();
        self.snapshot_manager.force_close();
        if let Err(err) = self.unregister_governance() {
            warn!(
                "failed to unregister db {} during force close: {}",
                self.id, err
            );
        }
        self.db_lifecycle.mark_closed();
    }

    /// Close the database, release runtime files and wait for this DB's physical deletions.
    /// A successful return is the handoff boundary for reopening the same local directories.
    pub fn close(&self) -> Result<()> {
        self.close_with_deletion_wait(DeletionWait::Forever)
            .map(|_| ())
    }

    /// Stop DB work, then apply the deletion wait policy. Timeout/NoWait permanently cancel
    /// queued deletions; they cannot interrupt synchronous backend I/O already in progress.
    /// Repeated calls can wait for that I/O, but never restart cancelled deletions.
    /// Drop preserves cancellation rather than unexpectedly waiting forever again.
    pub fn close_with_deletion_wait(&self, wait: DeletionWait) -> Result<DeletionStatus> {
        let shutdown = {
            let mut completed = self.shutdown_result.lock().unwrap();
            completed
                .get_or_insert_with(|| self.shutdown().inspect_err(|_| self.force_close()))
                .clone()
        };
        if let Err(error) = shutdown {
            if wait != DeletionWait::Forever {
                let _ = self.file_manager.wait_for_deletions(DeletionWait::NoWait);
            }
            return Err(error);
        }
        self.file_manager.wait_for_deletions(wait)
    }

    fn shutdown(&self) -> Result<()> {
        match self.db_lifecycle.begin_close()? {
            // Successful shutdowns are cached under shutdown_result. Reaching this state here
            // means force_close bypassed the graceful cleanup barrier, not a safe handoff.
            CloseTransition::AlreadyClosingOrClosed => {
                return Err(Error::IoError(
                    "Database was force-closed without completing cleanup".into(),
                ));
            }
            CloseTransition::Transitioned => {}
        }
        self.db_lifecycle.wait_for_accesses_to_drain();
        if let Some(err) = self.lifecycle_error() {
            return Err(err);
        }
        if let Some(wal_writer) = &self.wal_writer {
            wal_writer.close()?;
        }
        if let Some(worker) = &self.primary_tiering_worker {
            worker.stop();
            worker.join();
        }
        self.memtable_manager.close()?;
        if let Some(err) = self.lifecycle_error() {
            return Err(err);
        }
        self.lsm_tree.shutdown_compaction();
        // Stop the dedicated compaction poller before closing the snapshot manager so the
        // poller does not race with snapshot shutdown (e.g. trying to materialize a snapshot
        // while the materializer worker is being torn down).
        if let Some(poller) = &self.dedicated_poller {
            poller.stop();
            poller.join();
        }
        self.file_manager.stop_offload_worker();
        if let Some(publisher) = &self.runtime_manifest_publisher {
            publisher
                .publish_current()
                .inspect_err(|error| self.db_lifecycle.mark_error(error.clone()))?;
            publisher.stop();
            publisher.join();
        }
        self.snapshot_manager.close()?;
        if let Some(err) = self.lifecycle_error() {
            return Err(err);
        }
        // Workers and accesses are stopped. Release their file-owning state BEFORE draining
        // cleanup; leaving these references for Drop would submit deletions after the barrier.
        self.memtable_manager.release_closed_state();
        self.db_state.release_closed_state();
        self.file_manager.release_closed_files();
        self.unregister_governance()?;
        self.db_lifecycle.mark_closed();
        Ok(())
    }
}

impl Drop for Db {
    fn drop(&mut self) {
        if let Err(err) = self.close_with_deletion_wait(self.file_manager.deletion_wait_on_drop()) {
            error!("db close failed during drop: {}", err);
        }
    }
}

#[cfg(test)]
#[path = "../tests/unit/db_close.rs"]
mod tests;
