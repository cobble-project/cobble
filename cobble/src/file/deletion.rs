//! DB-scoped completion tracking for physical file deletion.
//!
//! FileSystem::delete_async is fire-and-forget. Managed files instead execute synchronous
//! backend deletes on the shared I/O blocking pool, so completion and errors belong to the
//! FileManager that scheduled them, rather than to a process-wide filesystem queue.

use super::{FileSystem, read_ahead_runtime};
use crate::{Error, Result};
use std::collections::{HashMap, VecDeque};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;
use tokio::runtime::Handle;
use tokio::sync::Notify;
use tokio::task::{Id, JoinSet};

// Bound both blocking-pool submissions and executing deletes for each DB.
const MAX_CONCURRENT_DELETIONS: usize = 4;

/// How long close waits for physical file deletion after stopping DB work.
/// This does not bound the time spent stopping reads, snapshots or compaction.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum DeletionWait {
    /// Cancel queued deletions immediately; already-started I/O may still be running.
    NoWait,
    /// Drain for at most this duration, then cancel remaining queued deletions.
    Timeout(Duration),
    /// Wait until all previously started/queued deletions have finished.
    #[default]
    Forever,
}

/// Result of the deletion phase of close. Cancelled files are left on storage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[must_use = "check for in-flight deletion before reopening the same directories"]
pub enum DeletionStatus {
    /// All submitted deletions completed successfully.
    Complete,
    Cancelled {
        /// When true, do not reopen/adopt these directories yet: a backend delete
        /// is still executing. Call close again to wait for its completion.
        in_flight: bool,
    },
}

#[derive(Clone)]
struct DeleteJob {
    fs: Arc<dyn FileSystem>,
    path: String,
}

impl DeleteJob {
    fn same_file(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.fs, &other.fs) && self.path == other.path
    }

    fn execute(&self) -> Result<()> {
        catch_unwind(AssertUnwindSafe(|| {
            let result = self.fs.delete(&self.path);
            if let Err(error) = &result {
                log::warn!("Failed to delete {}: {}", self.path, error);
            }
            result
        }))
        .unwrap_or_else(|_| {
            Err(Error::IoError(format!(
                "File deletion panicked: {}",
                self.path
            )))
        })
    }
}

#[derive(Default)]
struct State {
    jobs: VecDeque<DeleteJob>,
    draining: bool,
    // Only deletes admitted by the blocking worker, not submissions waiting in the pool.
    in_flight: usize,
    cancelled: bool,
    error: Option<Error>,
}

impl State {
    fn has_pending_work(&self) -> bool {
        if self.cancelled {
            self.in_flight != 0
        } else {
            self.draining
        }
    }

    fn cancel_pending(&mut self) -> VecDeque<DeleteJob> {
        self.cancelled = true;
        std::mem::take(&mut self.jobs)
    }

    fn result(&self) -> Result<DeletionStatus> {
        let status = if self.cancelled {
            DeletionStatus::Cancelled {
                in_flight: self.in_flight != 0,
            }
        } else {
            DeletionStatus::Complete
        };
        self.error.clone().map_or(Ok(status), Err)
    }
}

#[derive(Default)]
pub(crate) struct FileDeletionQueue {
    state: Mutex<State>,
    idle: Condvar,
    ready: Notify,
}

/// Keep the cancellation barrier accurate even if a custom backend or logger panics.
struct InFlightDelete<'a>(&'a FileDeletionQueue);

impl Drop for InFlightDelete<'_> {
    fn drop(&mut self) {
        let mut state = self.0.state.lock().unwrap();
        state.in_flight -= 1;
        self.0.idle.notify_all();
    }
}

impl FileDeletionQueue {
    pub(crate) fn submit(self: &Arc<Self>, fs: Arc<dyn FileSystem>, path: String) {
        let mut state = self.state.lock().unwrap();
        if state.cancelled {
            return;
        }
        state.jobs.push_back(DeleteJob { fs, path });
        self.ready.notify_one();
        if state.draining {
            return;
        }
        state.draining = true;
        let queue = Arc::clone(self);
        let runtime = read_ahead_runtime();
        // One lightweight scheduler per DB; only actual filesystem I/O occupies pool threads.
        runtime.spawn(queue.run(runtime.clone()));
    }

    async fn run(self: Arc<Self>, io: Handle) {
        let mut tasks = JoinSet::new();
        let mut active: HashMap<Id, DeleteJob> = HashMap::new();
        loop {
            while tasks.len() < MAX_CONCURRENT_DELETIONS {
                let job = {
                    let mut state = self.state.lock().unwrap();
                    // Skip an active path rather than blocking unrelated files behind it.
                    let next = state
                        .jobs
                        .iter()
                        .position(|job| !active.values().any(|running| job.same_file(running)));
                    next.and_then(|index| state.jobs.remove(index))
                };
                let Some(job) = job else { break };
                let tracked = job.clone();
                let queue = Arc::clone(&self);
                let task = tasks.spawn_blocking_on(move || queue.execute(job), &io);
                active.insert(task.id(), tracked);
            }
            if tasks.is_empty() {
                let mut state = self.state.lock().unwrap();
                // Pair with submit under the same lock: a concurrent append either keeps
                // this scheduler alive or starts a new one after draining becomes false.
                if state.jobs.is_empty() {
                    state.draining = false;
                    self.idle.notify_all();
                    return;
                }
                continue;
            }
            let can_dispatch = tasks.len() < MAX_CONCURRENT_DELETIONS;
            let completed = tokio::select! {
                completed = tasks.join_next_with_id() => completed.expect("nonempty deletion tasks"),
                _ = self.ready.notified(), if can_dispatch => continue,
            };
            let id = match completed {
                Ok((id, ())) => id,
                Err(error) => {
                    self.state.lock().unwrap().error.get_or_insert_with(|| {
                        Error::IoError(format!("Deletion task failed: {error}"))
                    });
                    error.id()
                }
            };
            active.remove(&id);
        }
    }

    fn execute(&self, job: DeleteJob) {
        let _in_flight = {
            let mut state = self.state.lock().unwrap();
            // Admission and cancellation share this lock. Once cancellation reports zero
            // in-flight I/O, no dispatched-but-not-started task can begin a backend delete.
            if state.cancelled {
                return;
            }
            state.in_flight += 1;
            InFlightDelete(self)
        };
        if let Err(error) = job.execute() {
            self.state.lock().unwrap().error.get_or_insert(error);
        }
    }

    /// Call after stopping producers and releasing their file references. This is a barrier
    /// for this owner only; it does not wait for other DBs sharing a filesystem or I/O pool.
    pub(crate) fn wait(&self, wait: DeletionWait) -> Result<DeletionStatus> {
        let pending = |state: &mut State| state.has_pending_work();
        let mut state = self.state.lock().unwrap();
        state = match wait {
            DeletionWait::Forever => self.idle.wait_while(state, pending).unwrap(),
            DeletionWait::Timeout(timeout) => {
                self.idle
                    .wait_timeout_while(state, timeout, pending)
                    .unwrap()
                    .0
            }
            DeletionWait::NoWait => state,
        };
        let cancelled_jobs = if wait == DeletionWait::NoWait
            || (state.has_pending_work() && wait != DeletionWait::Forever)
        {
            let jobs = state.cancel_pending();
            self.idle.notify_all();
            jobs
        } else {
            VecDeque::new()
        };
        let result = state.result();
        drop(state);
        // A custom filesystem's destructor must not run under the queue control lock.
        drop(cancelled_jobs);
        result
    }

    pub(crate) fn wait_on_drop(&self) -> DeletionWait {
        if self.state.lock().unwrap().cancelled {
            DeletionWait::NoWait
        } else {
            DeletionWait::Forever
        }
    }
}

#[cfg(test)]
#[path = "../../tests/unit/file/deletion.rs"]
mod tests;
