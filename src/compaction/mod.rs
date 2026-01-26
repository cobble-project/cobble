//! Compaction module for merging multiple SortedRuns into new SST files.
//!
//! This module provides the infrastructure for running compaction tasks asynchronously.
//! Compaction merges data from multiple SortedRuns using MergingIterator and
//! DeduplicatingIterator, producing a set of new SST files with sorted key ranges.

mod executor;
mod policy;

#[allow(unused_imports)]
pub(crate) use executor::{CompactionExecutor, CompactionResult, CompactionTask};
pub(crate) use policy::{
    CompactionConfig, CompactionPlan, CompactionPolicy, MinOverlapPolicy, RoundRobinPolicy,
    build_runs_for_plan,
};

#[allow(unused_imports)]
pub(crate) use crate::format::{FileBuilder, FileBuilderFactory};

use crate::error::Result;
use crate::iterator::SortedRun;
use crate::lsm::VersionEdit;
use crate::sst::SSTWriterOptions;
use log::info;
use std::sync::{Arc, Mutex, Weak};

pub(crate) struct CompactionWorker {
    executor: CompactionExecutor,
    file_builder_factory: Arc<FileBuilderFactory>,
    file_manager: Arc<crate::file::FileManager>,
    lsm_tree: Weak<Mutex<crate::lsm::LSMTree>>,
}

impl CompactionWorker {
    pub(crate) fn new(
        executor: CompactionExecutor,
        file_builder_factory: Arc<FileBuilderFactory>,
        file_manager: Arc<crate::file::FileManager>,
        lsm_tree: Weak<Mutex<crate::lsm::LSMTree>>,
    ) -> Self {
        Self {
            executor,
            file_builder_factory,
            file_manager,
            lsm_tree,
        }
    }

    pub(crate) fn submit(
        &self,
        task: CompactionTask,
    ) -> tokio::task::JoinHandle<Result<CompactionResult>> {
        let lsm_tree = self.lsm_tree.clone();
        let on_complete = Arc::new(move |edit: VersionEdit| {
            if let Some(lsm_tree) = lsm_tree.upgrade()
                && let Ok(mut tree) = lsm_tree.lock()
            {
                tree.on_compaction_complete();
                tree.apply_edit(edit);
            }
        });
        self.executor.execute(task, Some(on_complete))
    }

    pub(crate) fn submit_runs(
        &self,
        sorted_runs: Vec<SortedRun>,
        output_level: u8,
        data_file_type: crate::data_file::DataFileType,
    ) -> Option<tokio::task::JoinHandle<Result<CompactionResult>>> {
        if sorted_runs.is_empty() {
            return None;
        }
        let task = CompactionTask::new(
            sorted_runs,
            output_level,
            Arc::clone(&self.file_manager),
            Arc::clone(&self.file_builder_factory),
            data_file_type,
        );
        Some(self.submit(task))
    }

    pub(crate) fn shutdown(self) {
        info!("compaction worker shutdown");
        let mut executor = self.executor;
        executor.shutdown();
    }
}

pub(crate) fn make_sst_builder_factory(options: SSTWriterOptions) -> Arc<FileBuilderFactory> {
    Arc::new(Box::new(move |writer| {
        Box::new(crate::sst::SSTWriter::new(writer, options.clone())) as Box<dyn FileBuilder>
    }))
}
