//! Compaction module for merging multiple SortedRuns into new SST files.
//!
//! This module provides the infrastructure for running compaction tasks asynchronously.
//! Compaction merges data from multiple SortedRuns using MergingIterator and
//! DeduplicatingIterator, producing a set of new SST files with sorted key ranges.

mod executor;
mod policy;
mod remote;

#[allow(unused_imports)]
pub(crate) use executor::{CompactionExecutor, CompactionResult, CompactionTask};
pub(crate) use policy::{
    CompactionConfig, CompactionPlan, CompactionPolicy, MinOverlapPolicy, RoundRobinPolicy,
    build_runs_for_plan,
};
pub use remote::RemoteCompactionServer;
pub(crate) use remote::RemoteCompactionWorker;

#[allow(unused_imports)]
pub(crate) use crate::format::{FileBuilder, FileBuilderFactory};

use crate::error::Result;
use crate::iterator::SortedRun;
use crate::lsm::VersionEdit;
use crate::sst::SSTWriterOptions;
use log::info;
use std::sync::{Arc, Mutex, Weak};

pub(crate) trait CompactionWorker: Send + Sync {
    fn submit_runs(
        &self,
        sorted_runs: Vec<SortedRun>,
        output_level: u8,
        data_file_type: crate::data_file::DataFileType,
        ttl_provider: Arc<crate::ttl::TTLProvider>,
    ) -> Option<tokio::task::JoinHandle<Result<CompactionResult>>>;
    fn shutdown(&self);
}

pub(crate) struct LocalCompactionWorker {
    executor: Mutex<CompactionExecutor>,
    file_builder_factory: Arc<FileBuilderFactory>,
    file_manager: Arc<crate::file::FileManager>,
    lsm_tree: Weak<crate::lsm::LSMTree>,
}

impl LocalCompactionWorker {
    pub(crate) fn new(
        executor: CompactionExecutor,
        file_builder_factory: Arc<FileBuilderFactory>,
        file_manager: Arc<crate::file::FileManager>,
        lsm_tree: Weak<crate::lsm::LSMTree>,
    ) -> Self {
        Self {
            executor: Mutex::new(executor),
            file_builder_factory,
            file_manager,
            lsm_tree,
        }
    }

    fn submit(&self, task: CompactionTask) -> tokio::task::JoinHandle<Result<CompactionResult>> {
        let lsm_tree = self.lsm_tree.clone();
        let on_complete = Arc::new(move |edit: VersionEdit| {
            if let Some(lsm_tree) = lsm_tree.upgrade() {
                lsm_tree.on_compaction_complete();
                lsm_tree.apply_edit(edit);
            }
        });
        let executor = self.executor.lock().unwrap();
        executor.execute(task, Some(on_complete))
    }

    fn submit_runs_inner(
        &self,
        sorted_runs: Vec<SortedRun>,
        output_level: u8,
        data_file_type: crate::data_file::DataFileType,
        ttl_provider: Arc<crate::ttl::TTLProvider>,
    ) -> Option<tokio::task::JoinHandle<Result<CompactionResult>>> {
        if sorted_runs.is_empty() {
            return None;
        }
        let db_id = self
            .file_manager
            .db_id()
            .unwrap_or_else(|| "unknown".to_string());
        let task = CompactionTask::new(
            db_id,
            sorted_runs,
            output_level,
            Arc::clone(&self.file_manager),
            Arc::clone(&self.file_builder_factory),
            data_file_type,
            ttl_provider,
        );
        Some(self.submit(task))
    }

    fn shutdown_inner(&self) {
        info!("compaction worker shutdown");
        let mut executor = self.executor.lock().unwrap();
        executor.shutdown();
    }
}

impl CompactionWorker for LocalCompactionWorker {
    fn submit_runs(
        &self,
        sorted_runs: Vec<SortedRun>,
        output_level: u8,
        data_file_type: crate::data_file::DataFileType,
        ttl_provider: Arc<crate::ttl::TTLProvider>,
    ) -> Option<tokio::task::JoinHandle<Result<CompactionResult>>> {
        self.submit_runs_inner(sorted_runs, output_level, data_file_type, ttl_provider)
    }

    fn shutdown(&self) {
        self.shutdown_inner();
    }
}

pub(crate) fn make_sst_builder_factory(options: SSTWriterOptions) -> Arc<FileBuilderFactory> {
    Arc::new(Box::new(move |writer| {
        Box::new(crate::sst::SSTWriter::new(writer, options.clone())) as Box<dyn FileBuilder>
    }))
}

pub(crate) fn build_sst_writer_options(config: &crate::Config) -> SSTWriterOptions {
    SSTWriterOptions {
        num_columns: config.num_columns,
        bloom_filter_enabled: config.sst_bloom_filter_enabled,
        bloom_bits_per_key: config.sst_bloom_bits_per_key,
        partitioned_index: config.sst_partitioned_index,
        ..SSTWriterOptions::default()
    }
}

pub(crate) fn build_compaction_config(config: &crate::Config) -> CompactionConfig {
    CompactionConfig {
        policy: config.compaction_policy,
        l0_file_limit: config.l0_file_limit,
        l1_base_bytes: config.l1_base_bytes,
        level_size_multiplier: config.level_size_multiplier,
        max_level: config.max_level,
        num_columns: config.num_columns,
        target_file_size: config.base_file_size,
        bloom_filter_enabled: config.sst_bloom_filter_enabled,
        bloom_bits_per_key: config.sst_bloom_bits_per_key,
        partitioned_index: config.sst_partitioned_index,
        read_ahead_enabled: config.compaction_read_ahead_enabled,
        max_threads: config.compaction_threads,
        ..CompactionConfig::default()
    }
}
