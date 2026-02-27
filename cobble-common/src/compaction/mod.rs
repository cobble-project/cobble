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
    build_runs_for_plan,
};
pub use remote::RemoteCompactionServer;
#[allow(unused_imports)]
pub(crate) use remote::RemoteCompactionWorker;

#[allow(unused_imports)]
pub(crate) use crate::format::{FileBuilder, FileBuilderFactory};

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
        metrics_manager: Arc<MetricsManager>,
        schema_manager: Arc<SchemaManager>,
    ) -> Self {
        let compaction_metrics = metrics_manager.compaction_metrics();
        Self {
            executor: Mutex::new(executor),
            file_manager,
            lsm_tree,
            config,
            compaction_metrics,
            metrics_manager,
            schema_manager,
        }
    }

    fn submit(&self, task: CompactionTask) -> tokio::task::JoinHandle<Result<CompactionResult>> {
        let lsm_tree = self.lsm_tree.clone();
        let on_complete = Arc::new(move |edit: VersionEdit, vlog_edit| {
            if let Some(lsm_tree) = lsm_tree.upgrade() {
                lsm_tree.on_compaction_complete();
                lsm_tree.apply_edit_with_vlog(edit, vlog_edit);
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
        let sst_metrics = self
            .lsm_tree
            .upgrade()
            .map(|tree| tree.sst_metrics())
            .unwrap_or_else(|| self.metrics_manager.sst_iterator_metrics());
        let mut sst_options = build_sst_writer_options(&self.config, output_level);
        sst_options.metrics = Some(
            self.metrics_manager
                .sst_writer_metrics(sst_options.compression),
        );
        let file_builder_factory = make_sst_builder_factory(sst_options);
        let task = CompactionTask::new(
            Arc::clone(&self.compaction_metrics),
            sst_metrics,
            sorted_runs,
            output_level,
            Arc::clone(&self.file_manager),
            file_builder_factory,
            data_file_type,
            ttl_provider,
            self.schema_manager.latest_schema(),
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
        ..CompactionConfig::default()
    }
}
