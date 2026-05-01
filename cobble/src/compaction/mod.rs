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

use crate::data_file::DataFileType;
use crate::db_status::DbLifecycle;
use crate::error::Result;
use crate::iterator::SortedRun;
use crate::lsm::VersionEdit;
use crate::metrics_manager::MetricsManager;
use crate::parquet::{ParquetWriter, ParquetWriterOptions};
use crate::schema::SchemaManager;
use crate::sst::SSTWriterOptions;
use crate::writer_options::{WriterOptions, WriterOptionsFactory};
use log::{error, info};
use std::sync::{Arc, Mutex, Weak};

pub(crate) trait CompactionWorker: Send + Sync {
    fn submit_runs(
        &self,
        lsm_tree_idx: usize,
        sorted_runs: Vec<SortedRun>,
        output_level: u8,
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
        let sst_metrics = tree.sst_metrics();
        let Some(tree_scope) = tree.tree_scope_of_tree(lsm_tree_idx) else {
            error!(
                "skip compaction submit because tree scope {} is missing",
                lsm_tree_idx
            );
            return None;
        };
        let schema = self.schema_manager.latest_schema();
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
        .with_column_family(tree_scope.column_family_id, runtime_num_columns);
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
        data_file_type: DataFileType,
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
        data_block_restart_interval: config.sst_data_block_restart_interval,
        compression: config.sst_compression_for_level(level),
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
        read_ahead_enabled: config.compaction_read_ahead_enabled,
        max_threads: config.compaction_threads,
        split_trigger_level: config.lsm_split_trigger_level,
        output_file_type: config.data_file_type,
        ..CompactionConfig::default()
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::VolumeDescriptor;
    use crate::data_file::DataFile;
    use crate::db_state::{DbState, DbStateHandle, LSMTreeScope, MultiLSMTreeVersion};
    use crate::iterator::SortedRun;
    use crate::lsm::{LSMTree, LSMTreeVersion, Level};
    use crate::metrics_manager::MetricsManager;
    use crate::sst::row_codec::{decode_value, encode_key, encode_value};
    use crate::sst::{SSTIterator, SSTIteratorOptions, SSTWriter, SSTWriterOptions};
    use crate::r#type::{Column, Key, Value, ValueType};
    use serial_test::serial;
    use size::Size;
    use std::collections::VecDeque;
    use std::sync::Arc;

    fn cleanup_test_root(path: &str) {
        let _ = std::fs::remove_dir_all(path);
    }

    fn make_value_bytes(data: &[u8], num_columns: usize) -> Vec<u8> {
        let value = Value::new(vec![Some(Column::new(ValueType::Put, data.to_vec()))]);
        encode_value(&value, num_columns).to_vec()
    }

    fn make_test_key_in_family(column_family_id: u8, raw_key: &[u8]) -> Vec<u8> {
        encode_key(&Key::new_with_column_family(
            0,
            column_family_id,
            raw_key.to_vec(),
        ))
        .to_vec()
    }

    fn create_test_sst(
        file_manager: &Arc<crate::file::FileManager>,
        entries: Vec<(Vec<u8>, Vec<u8>)>,
        options: SSTWriterOptions,
        schema_id: u64,
    ) -> Result<Arc<DataFile>> {
        let (file_id, writer_file) = file_manager.create_data_file_with_offload()?;
        let mut writer = SSTWriter::new(writer_file, options);
        for (key, value) in entries {
            writer.add(&key, &value)?;
        }
        let (first_key, last_key, file_size, footer_bytes) = writer.finish_with_range()?;
        let bucket_range = DataFile::bucket_range_from_keys(&first_key, &last_key);
        let data_file = DataFile::new(
            crate::data_file::DataFileType::SSTable,
            first_key,
            last_key,
            file_id,
            crate::file::TrackedFileId::new(file_manager, file_id),
            schema_id,
            file_size,
            bucket_range.clone(),
            bucket_range,
        );
        data_file.set_meta_bytes(footer_bytes);
        Ok(Arc::new(data_file))
    }

    #[test]
    #[serial(file)]
    fn test_local_compaction_worker_uses_tree_scope_column_family_width() {
        let root = "/tmp/local_compaction_worker_cf_width";
        cleanup_test_root(root);

        let column_family_id = 1;
        let num_columns = 1;
        let config = crate::Config {
            volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
            base_file_size: Size::from_const(128),
            sst_bloom_filter_enabled: true,
            compaction_threads: 2,
            num_columns: 2,
            ..crate::Config::default()
        };
        let db_id = "local-compaction-worker-cf-width".to_string();
        let metrics_manager = Arc::new(MetricsManager::new(&db_id));
        let file_manager = Arc::new(
            crate::file::FileManager::from_config(&config, &db_id, Arc::clone(&metrics_manager))
                .unwrap(),
        );

        let mut sst_options = build_sst_writer_options(&config, 0, num_columns);
        sst_options.metrics = Some(metrics_manager.sst_writer_metrics(sst_options.compression));

        let schema_manager = Arc::new(SchemaManager::new(config.num_columns));
        let mut schema_builder = schema_manager.builder();
        schema_builder
            .add_column(0, None, None, Some("metrics".to_string()))
            .unwrap();
        let target_schema = schema_builder.commit();

        let expected_key = make_test_key_in_family(column_family_id, b"k1");
        let source_file = create_test_sst(
            &file_manager,
            vec![(expected_key.clone(), make_value_bytes(b"v1", num_columns))],
            sst_options,
            target_schema.version(),
        )
        .unwrap();

        let lsm_version = LSMTreeVersion {
            levels: vec![
                Level {
                    ordinal: 0,
                    tiered: true,
                    files: vec![Arc::clone(&source_file)],
                },
                Level {
                    ordinal: 1,
                    tiered: false,
                    files: Vec::new(),
                },
            ],
        };
        let db_state = Arc::new(DbStateHandle::new());
        db_state.store(DbState {
            seq_id: 0,
            bucket_ranges: Vec::new(),
            multi_lsm_version: MultiLSMTreeVersion::from_scopes_with_tree_versions(
                1,
                &[LSMTreeScope::new(0u16..=0u16, column_family_id)],
                vec![Arc::new(lsm_version)],
            )
            .unwrap(),
            vlog_version: crate::vlog::VlogVersion::new(),
            active: None,
            immutables: VecDeque::new(),
            suggested_base_snapshot_id: None,
        });

        let db_lifecycle = Arc::new(DbLifecycle::new_open());
        let lsm_tree = Arc::new(LSMTree::with_state_and_ttl(
            Arc::clone(&db_state),
            Arc::new(crate::ttl::TTLProvider::disabled()),
            Arc::clone(&db_lifecycle),
            Arc::clone(&metrics_manager),
        ));

        let executor = CompactionExecutor::new(
            build_compaction_config(&config, num_columns).unwrap(),
            Arc::clone(&db_lifecycle),
        )
        .unwrap();
        let worker = LocalCompactionWorker::new(
            executor,
            Arc::clone(&file_manager),
            Arc::downgrade(&lsm_tree),
            config.clone(),
            Arc::clone(&db_lifecycle),
            Arc::clone(&metrics_manager),
            Arc::clone(&schema_manager),
        );

        let handle = worker
            .submit_runs(
                0,
                vec![SortedRun::new(0, vec![source_file])],
                1,
                crate::data_file::DataFileType::SSTable,
                Arc::new(crate::ttl::TTLProvider::disabled()),
            )
            .expect("compaction handle");
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let result = runtime.block_on(handle).unwrap().unwrap();
        assert_eq!(result.new_files().len(), 1);

        let reader = file_manager
            .open_data_file_reader(result.new_files()[0].file_id)
            .unwrap();
        let mut iter = SSTIterator::with_cache_and_file(
            Box::new(reader),
            result.new_files()[0].as_ref(),
            SSTIteratorOptions {
                num_columns,
                bloom_filter_enabled: true,
                ..SSTIteratorOptions::default()
            },
            None,
        )
        .unwrap();
        iter.seek_to_first().unwrap();
        let (key, mut value) = iter.current().unwrap().unwrap();
        assert_eq!(key.as_ref(), expected_key.as_slice());
        let decoded = decode_value(&mut value, num_columns).unwrap();
        assert_eq!(
            decoded.columns()[0].as_ref().unwrap().data().as_ref(),
            b"v1"
        );

        cleanup_test_root(root);
    }
}
