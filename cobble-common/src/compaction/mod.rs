//! Compaction module for merging multiple SortedRuns into new SST files.
//!
//! This module provides the infrastructure for running compaction tasks asynchronously.
//! Compaction merges data from multiple SortedRuns using MergingIterator and
//! DeduplicatingIterator, producing a set of new SST files with sorted key ranges.

mod executor;
mod policy;
mod remote;

#[allow(unused_imports)]
pub(crate) use executor::{
    CompactionExecutor, CompactionResult, CompactionTask, CompactionTaskMetrics,
};
pub(crate) use policy::{
    CompactionConfig, CompactionPlan, CompactionPolicy, MinOverlapPolicy, RoundRobinPolicy,
    build_runs_for_plan, level_threshold,
};
pub use remote::RemoteCompactionServer;
#[allow(unused_imports)]
pub(crate) use remote::RemoteCompactionWorker;

#[allow(unused_imports)]
pub(crate) use crate::format::{FileBuilder, FileBuilderFactory};

use crate::db_status::DbLifecycle;
use crate::error::Result;
use crate::iterator::SortedRun;
use crate::lsm::VersionEdit;
use crate::metrics_manager::MetricsManager;
use crate::schema::SchemaManager;
use crate::sst::SSTWriterOptions;
use log::info;
use std::sync::{Arc, Mutex, Weak};

pub(crate) trait CompactionWorker: Send + Sync {
    fn submit_runs(
        &self,
        lsm_tree_idx: usize,
        sorted_runs: Vec<SortedRun>,
        output_level: u8,
        data_file_type: crate::data_file::DataFileType,
        ttl_provider: Arc<crate::ttl::TTLProvider>,
    ) -> Option<tokio::task::JoinHandle<Result<CompactionResult>>>;
    fn shutdown(&self);
}

pub(crate) struct LocalCompactionWorker {
    executor: Mutex<CompactionExecutor>,
    file_manager: Arc<crate::file::FileManager>,
    lsm_tree: Weak<crate::lsm::LSMTree>,
    config: crate::Config,
    db_lifecycle: Arc<DbLifecycle>,
    compaction_metrics: Arc<CompactionTaskMetrics>,
    metrics_manager: Arc<MetricsManager>,
    schema_manager: Arc<SchemaManager>,
}

impl LocalCompactionWorker {
    pub(crate) fn new(
        executor: CompactionExecutor,
        file_manager: Arc<crate::file::FileManager>,
        lsm_tree: Weak<crate::lsm::LSMTree>,
        config: crate::Config,
        db_lifecycle: Arc<DbLifecycle>,
        metrics_manager: Arc<MetricsManager>,
        schema_manager: Arc<SchemaManager>,
    ) -> Self {
        let compaction_metrics = metrics_manager.compaction_metrics();
        Self {
            executor: Mutex::new(executor),
            file_manager,
            lsm_tree,
            config,
            db_lifecycle,
            compaction_metrics,
            metrics_manager,
            schema_manager,
        }
    }

    fn submit(&self, task: CompactionTask) -> tokio::task::JoinHandle<Result<CompactionResult>> {
        let lsm_tree = self.lsm_tree.clone();
        let on_complete = Arc::new(move |lsm_tree_idx: usize, edit: VersionEdit, vlog_edit| {
            if let Some(lsm_tree) = lsm_tree.upgrade()
                && let Some(apply_tree_idx) = lsm_tree.on_compaction_complete(lsm_tree_idx)
            {
                lsm_tree.apply_edit(apply_tree_idx, edit, vlog_edit);
            }
        });
        let executor = self.executor.lock().unwrap();
        executor.execute(task, Some(on_complete))
    }

    fn submit_runs_inner(
        &self,
        lsm_tree_idx: usize,
        sorted_runs: Vec<SortedRun>,
        output_level: u8,
        data_file_type: crate::data_file::DataFileType,
        ttl_provider: Arc<crate::ttl::TTLProvider>,
    ) -> Option<tokio::task::JoinHandle<Result<CompactionResult>>> {
        if sorted_runs.is_empty() {
            return None;
        }
        let tree = self.lsm_tree.upgrade()?;
        if self.db_lifecycle.ensure_open().is_err() {
            return None;
        }
        let sst_metrics = tree.sst_metrics();
        let mut sst_options = build_sst_writer_options(&self.config, output_level);
        sst_options.num_columns = self.schema_manager.current_num_columns();
        sst_options.metrics = Some(
            self.metrics_manager
                .sst_writer_metrics(sst_options.compression),
        );
        let file_builder_factory = make_sst_builder_factory(sst_options);
        let task = CompactionTask::new(
            Arc::clone(&self.compaction_metrics),
            sst_metrics,
            lsm_tree_idx,
            sorted_runs,
            output_level,
            Arc::clone(&self.file_manager),
            file_builder_factory,
            data_file_type,
            ttl_provider,
            Arc::clone(&self.schema_manager),
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
        lsm_tree_idx: usize,
        sorted_runs: Vec<SortedRun>,
        output_level: u8,
        data_file_type: crate::data_file::DataFileType,
        ttl_provider: Arc<crate::ttl::TTLProvider>,
    ) -> Option<tokio::task::JoinHandle<Result<CompactionResult>>> {
        self.submit_runs_inner(
            lsm_tree_idx,
            sorted_runs,
            output_level,
            data_file_type,
            ttl_provider,
        )
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

pub(crate) fn build_sst_writer_options(config: &crate::Config, level: u8) -> SSTWriterOptions {
    SSTWriterOptions {
        num_columns: config.num_columns,
        bloom_filter_enabled: config.sst_bloom_filter_enabled,
        bloom_bits_per_key: config.sst_bloom_bits_per_key,
        partitioned_index: config.sst_partitioned_index,
        compression: config.sst_compression_for_level(level),
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
        split_trigger_level: config.lsm_split_trigger_level,
        ..CompactionConfig::default()
    }
}
