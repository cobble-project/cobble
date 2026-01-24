//! Compaction module for merging multiple SortedRuns into new SST files.
//!
//! This module provides the infrastructure for running compaction tasks asynchronously.
//! Compaction merges data from multiple SortedRuns using MergingIterator and
//! DeduplicatingIterator, producing a set of new SST files with sorted key ranges.

mod executor;

#[allow(unused_imports)]
pub(crate) use executor::{
    CompactionExecutor, CompactionOptions, CompactionResult, CompactionTask,
};

#[allow(unused_imports)]
pub(crate) use crate::format::{FileBuilder, FileBuilderFactory};

use crate::error::Result;
use crate::lsm::VersionEdit;
use std::sync::{Arc, Mutex};

pub(crate) struct CompactionWorker {
    executor: CompactionExecutor,
    lsm_tree: Arc<Mutex<crate::lsm::LSMTree>>,
}

impl CompactionWorker {
    pub(crate) fn new(
        executor: CompactionExecutor,
        lsm_tree: Arc<Mutex<crate::lsm::LSMTree>>,
    ) -> Self {
        Self { executor, lsm_tree }
    }

    pub(crate) fn submit(
        &self,
        task: CompactionTask,
    ) -> tokio::task::JoinHandle<Result<CompactionResult>> {
        let lsm_tree = Arc::clone(&self.lsm_tree);
        let on_complete = Arc::new(move |edit: VersionEdit| {
            if let Ok(mut tree) = lsm_tree.lock() {
                tree.apply_edit(edit);
            }
        });
        self.executor.execute(task, Some(on_complete))
    }
}
