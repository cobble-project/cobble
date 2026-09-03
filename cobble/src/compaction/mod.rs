//! Compaction module for merging multiple SortedRuns into new SST files.
//!
//! This module provides the infrastructure for running compaction tasks asynchronously.
//! Compaction merges data from multiple SortedRuns using MergingIterator and
//! DeduplicatingIterator, producing a set of new SST files with sorted key ranges.

pub(crate) mod dedicated;
pub(crate) mod dedicated_apply;
pub mod dedicated_compactor;
pub(crate) mod dedicated_poller;
pub mod dedicated_service;
mod executor;
mod policy;
mod remote;
mod resilient;

#[allow(unused_imports)]
pub(crate) use executor::{
    BlockingTaskTracker, CompactionExecutor, CompactionResult, CompactionTask,
    CompactionTaskMetrics, build_compaction_runtime,
};
pub(crate) use policy::{
    CompactionConfig, CompactionPlan, CompactionPolicy, CompactionPolicyContext, MinOverlapPolicy,
    RoundRobinPolicy, ScorePriorityPolicy, file_fully_covered_by_truncation_cursor,
    level_threshold, resolve_compaction_plan,
};
pub use remote::RemoteCompactionServer;
#[allow(unused_imports)]
pub(crate) use remote::RemoteCompactionWorker;
#[allow(unused_imports)]
pub(crate) use resilient::ResilientRemoteCompactionWorker;

#[allow(unused_imports)]
pub(crate) use crate::format::{FileBuilder, FileBuilderFactory};

use crate::data_file::DataFileType;
use crate::db_status::DbLifecycle;
use crate::error::Result;
use crate::iterator::SortedRun;
use crate::lsm::VersionEdit;
use crate::metrics_manager::MetricsManager;
use crate::parquet::{ParquetWriter, ParquetWriterOptions};
use crate::schema::SchemaManager;
use crate::sst::SSTWriterOptions;
use crate::vlog::VlogVersion;
use crate::writer_options::{WriterOptions, WriterOptionsFactory};
use log::{error, info};
use std::sync::{Arc, Mutex, Weak};

pub(crate) trait CompactionWorker: Send + Sync {
    #[allow(clippy::too_many_arguments)]
    fn submit_runs(
        &self,
        lsm_tree_idx: usize,
        sorted_runs: Vec<SortedRun>,
        output_level: u8,
        target_schema_id: u64,
        vlog_version: VlogVersion,
        data_file_type: DataFileType,
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
        let file_manager = Arc::clone(&self.file_manager);
        let on_complete = Arc::new(
            move |lsm_tree_idx: usize, edit: VersionEdit, vlog_edit, preload_block_keys| {
                if let Some(lsm_tree) = lsm_tree.upgrade()
                    && lsm_tree
                        .apply_compaction_result(lsm_tree_idx, edit, vlog_edit)
                        .is_some()
                {
                    // Final local handoff for the hot-block flow documented on
                    // `ScanHotBlockRegistry`: after new files are visible in the
                    // local FileManager, load the writer-produced output block keys
                    // on the dedicated preload runtime. Remote compaction uses the
                    // same worker after remapping remote output file ids.
                    lsm_tree
                        .submit_block_cache_preload(Arc::clone(&file_manager), preload_block_keys);
                }
            },
        );
        let executor = self.executor.lock().unwrap();
        executor.execute(task, Some(on_complete))
    }

    #[allow(clippy::too_many_arguments)]
    fn submit_runs_inner(
        &self,
        lsm_tree_idx: usize,
        sorted_runs: Vec<SortedRun>,
        output_level: u8,
        target_schema_id: u64,
        vlog_version: VlogVersion,
        data_file_type: DataFileType,
        ttl_provider: Arc<crate::ttl::TTLProvider>,
    ) -> Option<tokio::task::JoinHandle<Result<CompactionResult>>> {
        if sorted_runs.is_empty() {
            return None;
        }
        let tree = self.lsm_tree.upgrade()?;
        if self.db_lifecycle.ensure_open().is_err() {
            return None;
        }
        let truncation_cursors = tree.db_state().load().truncation_cursors_snapshot();
        let sst_metrics = tree.sst_metrics();
        let Some(tree_scope) = tree.tree_scope_of_tree(lsm_tree_idx) else {
            error!(
                "skip compaction submit because tree scope {} is missing",
                lsm_tree_idx
            );
            return None;
        };
        let schema = self.schema_manager.schema(target_schema_id).ok()?;
        let runtime_num_columns = schema
            .num_columns_in_family(tree_scope.column_family_id)
            .unwrap_or_else(|| schema.num_columns());
        let mut writer_options = match build_writer_options(
            &self.config,
            output_level,
            data_file_type,
            runtime_num_columns,
        ) {
            Ok(options) => options,
            Err(err) => {
                error!(
                    "skip compaction submit due to invalid writer size config: {}",
                    err
                );
                return None;
            }
        };
        match &mut writer_options {
            WriterOptions::Sst(sst_options) => {
                sst_options.metrics = Some(
                    self.metrics_manager
                        .sst_writer_metrics(sst_options.compression),
                );
            }
            WriterOptions::Parquet(_) => {}
        }
        let file_builder_factory = make_data_file_builder_factory(writer_options.clone());
        let writer_options_factory = WriterOptionsFactory::from(&writer_options);
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
        )
        .with_writer_options_factory(writer_options_factory)
        .with_target_schema_id(target_schema_id)
        .with_vlog_version(vlog_version)
        .with_column_family(tree_scope.column_family_id, runtime_num_columns)
        .with_truncation_cursors(truncation_cursors)
        .with_scan_hot_block_cache(
            tree.block_cache(),
            tree.cache_namespace(),
            tree.scan_hot_blocks(),
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
        target_schema_id: u64,
        vlog_version: VlogVersion,
        data_file_type: DataFileType,
        ttl_provider: Arc<crate::ttl::TTLProvider>,
    ) -> Option<tokio::task::JoinHandle<Result<CompactionResult>>> {
        self.submit_runs_inner(
            lsm_tree_idx,
            sorted_runs,
            output_level,
            target_schema_id,
            vlog_version,
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

pub(crate) fn make_parquet_builder_factory(
    options: ParquetWriterOptions,
) -> Arc<FileBuilderFactory> {
    Arc::new(Box::new(move |writer| {
        Box::new(
            ParquetWriter::with_options(writer, options.clone())
                .expect("failed to create parquet writer"),
        ) as Box<dyn FileBuilder>
    }))
}

pub(crate) fn make_data_file_builder_factory(
    writer_options: WriterOptions,
) -> Arc<FileBuilderFactory> {
    match writer_options {
        WriterOptions::Sst(options) => make_sst_builder_factory(options),
        WriterOptions::Parquet(options) => make_parquet_builder_factory(options),
    }
}

pub(crate) fn build_parquet_writer_options(
    config: &crate::Config,
    num_columns: usize,
) -> Result<ParquetWriterOptions> {
    Ok(ParquetWriterOptions {
        row_group_size_bytes: config.parquet_row_group_size_bytes()?.max(1),
        num_columns,
        ..ParquetWriterOptions::default()
    })
}

pub(crate) fn build_sst_writer_options(
    config: &crate::Config,
    level: u8,
    num_columns: usize,
) -> SSTWriterOptions {
    SSTWriterOptions {
        num_columns,
        bloom_filter_enabled: config.sst_bloom_filter_enabled,
        bloom_bits_per_key: config.sst_bloom_bits_per_key,
        partitioned_index: config.sst_partitioned_index,
        read_metadata_cache_mode: config.sst_read_metadata_cache_mode,
        data_block_restart_interval: config.sst_data_block_restart_interval,
        compression: config.sst_compression_for_level(level),
        block_checksum_enabled: config.block_checksum_enabled,
        ..SSTWriterOptions::default()
    }
}

pub(crate) fn build_writer_options(
    config: &crate::Config,
    level: u8,
    data_file_type: DataFileType,
    num_columns: usize,
) -> Result<WriterOptions> {
    Ok(match data_file_type {
        DataFileType::SSTable => {
            WriterOptions::Sst(build_sst_writer_options(config, level, num_columns))
        }
        DataFileType::Parquet => {
            WriterOptions::Parquet(build_parquet_writer_options(config, num_columns)?)
        }
    })
}

pub(crate) fn build_compaction_config(
    config: &crate::Config,
    num_columns: usize,
) -> Result<CompactionConfig> {
    Ok(CompactionConfig {
        policy: config.compaction_policy,
        l0_file_limit: config.l0_file_limit,
        l1_base_bytes: config.l1_base_bytes_bytes()?,
        level_size_multiplier: config.level_size_multiplier,
        max_level: config.max_level,
        num_columns,
        target_file_size: config.base_file_size_bytes()?,
        bloom_filter_enabled: config.sst_bloom_filter_enabled,
        bloom_bits_per_key: config.sst_bloom_bits_per_key,
        partitioned_index: config.sst_partitioned_index,
        read_metadata_cache_mode: config.sst_read_metadata_cache_mode,
        pinned_metadata_max_level: config.sst_pinned_metadata_max_level,
        pinned_metadata_partitions_enabled: config.sst_pinned_metadata_partitions_enabled,
        read_ahead_enabled: config.compaction_read_ahead_enabled,
        max_threads: config.compaction_threads,
        split_trigger_level: config.lsm_split_trigger_level,
        output_file_type: config.data_file_type,
        ..CompactionConfig::default()
    })
}

#[cfg(test)]
#[path = "../../tests/unit/compaction/mod.rs"]
mod tests;
