use super::{
    Config, Error, GovernanceMode, MemtableType, PrimaryVolumeOffloadPolicyKind, ReadOptions,
    ReaderConfigEntry, RemoteCompactionFailureMode, ScanOptions, SstReadMetadataCacheMode,
    VolumeDescriptor, VolumeUsageKind, WriteOptions,
};
use crate::SstCompressionAlgorithm;
use crate::data_file::DataFileType;
use crate::schema::Schema;
use size::Size;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;
use tempfile::Builder;

#[test]
fn test_default_memtable_type_is_adaptive() {
    assert_eq!(MemtableType::default(), MemtableType::Adaptive);
    assert_eq!(Config::default().memtable_type, MemtableType::Adaptive);
    assert_eq!(Config::default().sst_pinned_metadata_max_level, Some(2));
}

#[test]
fn wal_is_opt_in_and_requires_one_explicit_volume() {
    assert!(!Config::from_json_str("{}").unwrap().wal_enabled);

    let mut missing = Config::default();
    missing.wal_enabled = true;
    assert!(missing.normalize_volume_paths().is_err());

    let mut duplicate = Config::default();
    duplicate.wal_enabled = true;
    duplicate.volumes = vec![
        VolumeDescriptor::new("file:///tmp/wal-a", vec![VolumeUsageKind::Wal]),
        VolumeDescriptor::new("file:///tmp/wal-b", vec![VolumeUsageKind::Wal]),
    ];
    assert!(duplicate.normalize_volume_paths().is_err());

    let mut enabled = Config::default();
    enabled.wal_enabled = true;
    enabled.volumes = vec![VolumeDescriptor::new(
        "file:///tmp/wal",
        vec![VolumeUsageKind::Wal],
    )];
    assert!(enabled.normalize_volume_paths().is_ok());
}

#[test]
fn test_resolved_write_stall_limit() {
    let mut config = Config::default();
    assert_eq!(config.resolved_write_stall_limit(), 32);

    config.l0_file_limit = 64;
    assert_eq!(config.resolved_write_stall_limit(), 66);

    config.write_stall_limit = Some(65);
    assert_eq!(config.resolved_write_stall_limit(), 66);

    config.write_stall_limit = Some(80);
    assert_eq!(config.resolved_write_stall_limit(), 80);
}

#[test]
fn test_runtime_manifest_mode_resolution() {
    let mut config = Config::default();
    assert!(!config.runtime_manifests_enabled());
    assert!(config.runtime_manifests_enabled_for_dedicated_compactor());

    config.compaction_mode = super::CompactionMode::Dedicated;
    assert!(config.runtime_manifests_enabled());

    config.runtime_manifest_mode = super::RuntimeManifestMode::Disabled;
    assert!(!config.runtime_manifests_enabled());
    assert!(!config.runtime_manifests_enabled_for_dedicated_compactor());

    config.compaction_mode = super::CompactionMode::Embedded;
    config.runtime_manifest_mode = super::RuntimeManifestMode::Enabled;
    assert!(config.runtime_manifests_enabled());
    assert!(config.runtime_manifests_enabled_for_dedicated_compactor());
}

#[test]
fn dedicated_compaction_accepts_ttl() {
    Config::from_json_str(r#"{"compaction_mode":"dedicated","ttl_enabled":true}"#)
        .expect("dedicated TTL should be supported");
}

#[test]
fn test_pinned_metadata_level_minus_one_disables_pinning() {
    let config = Config::from_json_str(r#"{"sst_pinned_metadata_max_level":-1}"#).unwrap();

    assert_eq!(config.sst_pinned_metadata_max_level, None);
}

#[test]
fn test_config_from_file_round_trip() {
    let mut volume = VolumeDescriptor::new(
        "file:///tmp/cobble".to_string(),
        vec![
            VolumeUsageKind::PrimaryDataPriorityHigh,
            VolumeUsageKind::Meta,
        ],
    );
    volume.custom_options = Some(
        [
            ("endpoint".to_string(), "http://127.0.0.1:9000".to_string()),
            ("region".to_string(), "us-east-1".to_string()),
        ]
        .into_iter()
        .collect(),
    );
    let config = Config {
        volumes: vec![volume],
        memtable_capacity: Size::from_kib(1),
        memtable_buffer_count: 3,
        memtable_type: MemtableType::Vec,
        num_columns: 2,
        total_buckets: 1024,
        l0_file_limit: 5,
        write_stall_limit: Some(12),
        l1_base_bytes: Size::from_kib(8),
        level_size_multiplier: 7,
        max_level: 4,
        compaction_policy: super::CompactionPolicyKind::ScorePriority,
        block_cache_size: Size::from_const(256),
        block_cache_hybrid_enabled: true,
        block_cache_hybrid_disk_size: Some(Size::from_kib(1)),
        reader: ReaderConfigEntry {
            pin_partition_in_memory_count: 2,
            block_cache_size: Size::from_kib(2),
            reload_tolerance_seconds: 5,
        },
        base_file_size: Size::from_const(512),
        sst_bloom_filter_enabled: true,
        sst_bloom_bits_per_key: 11,
        sst_partitioned_index: true,
        sst_read_metadata_cache_mode: SstReadMetadataCacheMode::Off,
        sst_pinned_metadata_max_level: Some(2),
        sst_pinned_metadata_partitions_enabled: true,
        sst_data_block_restart_interval: 32,
        data_file_type: DataFileType::Parquet,
        block_checksum_enabled: false,
        parquet_row_group_size_bytes: Size::from_kib(4),
        sst_compression_by_level: vec![
            SstCompressionAlgorithm::None,
            SstCompressionAlgorithm::None,
            SstCompressionAlgorithm::Lz4,
        ],
        ttl_enabled: true,
        default_ttl_seconds: Some(120),
        value_separation_threshold: Some(Size::from_kib(4)),
        vlog_low_priority_primary_enabled: true,
        time_provider: crate::time::TimeProviderKind::Manual,
        log_path: Some("/tmp/cobble.log".to_string()),
        log_max_file_size: Size::from_mib(16),
        log_keep_files: 5,
        jni_direct_buffer_size: Size::from_kib(8),
        jni_direct_buffer_pool_size: 32,
        log_console: true,
        log_level: log::LevelFilter::Debug,
        snapshot_on_flush: true,
        wal_enabled: false,
        wal_flush_interval_ms: 5,
        active_memtable_incremental_snapshot_ratio: 0.5,
        lsm_split_trigger_level: Some(2),
        primary_volume_write_stop_watermark: 0.93,
        primary_volume_offload_trigger_watermark: 0.82,
        primary_volume_backfill_trigger_watermark: 0.41,
        file_transfer_concurrency: 3,
        primary_volume_offload_policy: PrimaryVolumeOffloadPolicyKind::LargestFile,
        snapshot_retention: Some(3),
        snapshot_only_track: false,
        snapshot_disable_incremental_base_link: false,
        governance_mode: GovernanceMode::Noop,
        compaction_read_ahead_enabled: false,
        compaction_remote_addr: Some("127.0.0.1:9999".to_string()),
        compaction_threads: 6,
        compaction_remote_timeout_ms: 120_000,
        compaction_remote_failure_mode: RemoteCompactionFailureMode::Skip,
        compaction_server_max_concurrent: 8,
        compaction_server_max_queued: 32,
        compaction_mode: super::CompactionMode::Embedded,
        runtime_manifest_mode: super::RuntimeManifestMode::Enabled,
        compaction_dedicated_poll_interval_ms: 1_000,
        compaction_orphan_min_age_ms: 300_000,
    };

    let serialized = serde_json::to_string(&config).expect("Cannot serialize config");
    let mut json_file = Builder::new()
        .suffix(".json")
        .tempfile()
        .expect("Should create temp json");
    json_file
        .write_all(serialized.as_bytes())
        .expect("Should able to write json");
    json_file.flush().expect("Should able to flush json");
    let decoded: Config = Config::from_path(json_file.path()).expect("Cannot deserialize json");

    assert_eq!(decoded.volumes.len(), 1);
    assert!(decoded.volumes[0].supports(VolumeUsageKind::PrimaryDataPriorityHigh));
    assert!(decoded.volumes[0].supports(VolumeUsageKind::Meta));
    assert_eq!(
        decoded.volumes[0]
            .custom_options
            .as_ref()
            .and_then(|v| v.get("endpoint")),
        Some(&"http://127.0.0.1:9000".to_string())
    );
    assert_eq!(decoded.memtable_capacity, Size::from_kib(1));
    assert_eq!(decoded.memtable_type, MemtableType::Vec);
    assert_eq!(decoded.total_buckets, 1024);
    assert_eq!(decoded.write_stall_limit, Some(12));
    assert_eq!(
        decoded.compaction_policy,
        super::CompactionPolicyKind::ScorePriority
    );
    assert!(decoded.sst_partitioned_index);
    assert_eq!(
        decoded.sst_read_metadata_cache_mode,
        SstReadMetadataCacheMode::Off
    );
    assert_eq!(decoded.sst_pinned_metadata_max_level, Some(2));
    assert!(decoded.sst_pinned_metadata_partitions_enabled);
    assert_eq!(decoded.time_provider, crate::time::TimeProviderKind::Manual);
    assert_eq!(decoded.log_max_file_size, Size::from_mib(16));
    assert_eq!(decoded.log_keep_files, 5);
    assert_eq!(decoded.jni_direct_buffer_size, Size::from_kib(8));
    assert_eq!(decoded.jni_direct_buffer_pool_size, 32);
    assert_eq!(decoded.log_level, log::LevelFilter::Debug);
    assert_eq!(decoded.snapshot_retention, Some(3));
    assert_eq!(decoded.active_memtable_incremental_snapshot_ratio, 0.5);
    assert_eq!(decoded.lsm_split_trigger_level, Some(2));
    assert_eq!(decoded.primary_volume_write_stop_watermark, 0.93);
    assert_eq!(decoded.primary_volume_offload_trigger_watermark, 0.82);
    assert_eq!(decoded.primary_volume_backfill_trigger_watermark, 0.41);
    assert_eq!(decoded.file_transfer_concurrency, 3);
    assert_eq!(
        decoded.primary_volume_offload_policy,
        PrimaryVolumeOffloadPolicyKind::LargestFile
    );
    assert_eq!(decoded.value_separation_threshold, Some(Size::from_kib(4)));
    assert!(decoded.vlog_low_priority_primary_enabled);
    assert_eq!(decoded.compaction_server_max_concurrent, 8);
    assert_eq!(decoded.compaction_server_max_queued, 32);
    assert_eq!(
        decoded.compaction_remote_failure_mode,
        RemoteCompactionFailureMode::Skip
    );
    assert_eq!(decoded.data_file_type, DataFileType::Parquet);
    assert!(!decoded.block_checksum_enabled);
    assert_eq!(decoded.parquet_row_group_size_bytes, Size::from_kib(4));
    assert_eq!(decoded.reader.block_cache_size, Size::from_kib(2));
    assert_eq!(decoded.reader.reload_tolerance_seconds, 5);
    assert!(decoded.block_cache_hybrid_enabled);
    assert_eq!(
        decoded.block_cache_hybrid_disk_size,
        Some(Size::from_kib(1))
    );

    let yaml = serde_yaml::to_string(&config).expect("Cannot serialize yaml");
    let mut yaml_file = Builder::new()
        .suffix(".yaml")
        .tempfile()
        .expect("Should create temp yaml");
    yaml_file
        .write_all(yaml.as_bytes())
        .expect("Should able to write yaml");
    yaml_file.flush().expect("Should able to flush yaml");
    let decoded_yaml: Config =
        Config::from_path(yaml_file.path()).expect("Cannot deserialize yaml");
    assert_eq!(decoded_yaml.reader.block_cache_size, Size::from_kib(2));

    let mut path_buf = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path_buf.push("tests/testdata/config.ini");

    let decoded_ini = Config::from_path(path_buf.as_path()).expect("Cannot deserialize ini");
    assert_eq!(decoded_ini.memtable_capacity, Size::from_kib(1));
    assert_eq!(decoded_ini.reader.block_cache_size, Size::from_kib(2));
    assert_eq!(decoded_ini.data_file_type, DataFileType::SSTable);
}

#[test]
fn test_volume_descriptor_kinds_list() {
    let json = r#"{
            "base_dir": "file:///tmp/cobble",
            "kinds": ["meta", "primary_data_priority_high", "snapshot", "cache"]
        }"#;

    let volume: VolumeDescriptor =
        serde_json::from_str(json).expect("Cannot deserialize volume descriptor");
    assert!(volume.supports(VolumeUsageKind::Meta));
    assert!(volume.supports(VolumeUsageKind::PrimaryDataPriorityHigh));
    assert!(volume.supports(VolumeUsageKind::Snapshot));
    assert!(volume.supports(VolumeUsageKind::Cache));
}

#[test]
fn test_volume_descriptor_kinds_readonly() {
    let json = r#"{
            "base_dir": "file:///tmp/cobble-readonly",
            "kinds": ["readonly"]
        }"#;
    let volume: VolumeDescriptor =
        serde_json::from_str(json).expect("Cannot deserialize readonly volume descriptor");
    assert!(volume.supports(VolumeUsageKind::Readonly));
    assert!(!volume.supports(VolumeUsageKind::Snapshot));
    assert!(!volume.supports(VolumeUsageKind::PrimaryDataPriorityHigh));
}

#[test]
fn test_read_options_column_family_constructors() {
    let options = ReadOptions::for_columns_in_family("metrics", vec![2, 0]);
    assert_eq!(options.column_family(), Some("metrics"));
    assert_eq!(options.columns(), Some(&[2, 0][..]));

    let options = ReadOptions::for_column(1).with_column_family("default");
    assert_eq!(options.column_family(), Some("default"));
    assert_eq!(options.columns(), Some(&[1][..]));
}

#[test]
fn test_scan_options_column_family_builder() {
    let options = ScanOptions::for_columns(vec![2, 0]).with_column_family("metrics");
    assert_eq!(options.column_family(), Some("metrics"));
    assert_eq!(options.columns(), Some(&[2, 0][..]));
    assert!(!options.preload_scan_cursor_block());

    let options = ScanOptions::for_column(1)
        .with_column_family("default")
        .with_preload_scan_cursor_block(true);
    assert_eq!(options.column_family(), Some("default"));
    assert_eq!(options.columns(), Some(&[1][..]));
    assert!(options.preload_scan_cursor_block());
}

#[test]
fn test_write_options_column_family_constructor() {
    let options = WriteOptions::with_column_family("metrics");
    assert_eq!(options.column_family(), Some("metrics"));
    assert_eq!(options.ttl_seconds, None);
}

#[test]
fn test_write_options_column_family_cache_invalidates_on_schema_change() {
    let schema_v1 = Schema::new_for_column_family(
        1,
        1,
        vec![],
        vec![],
        crate::schema::ColumnFamilyOptions::default(),
    );
    let schema_v2 = Schema::new_for_column_family(
        2,
        2,
        vec![],
        vec![],
        crate::schema::ColumnFamilyOptions::default(),
    );
    let options = WriteOptions::with_column_family("remote-cf-1");

    let resolved = options.resolve_column_family_id_cached(&schema_v1).unwrap();
    assert_eq!(resolved, 1);

    let err = options
        .resolve_column_family_id_cached(&schema_v2)
        .expect_err("schema version change should invalidate cache");
    assert!(matches!(err, Error::IoError(msg) if msg.contains("Unknown column family")));
}

#[test]
fn test_read_options_cache_invalidates_via_with_column_family() {
    let schema = Schema::new_for_column_family(
        1,
        1,
        vec![],
        vec![],
        crate::schema::ColumnFamilyOptions::default(),
    );
    let options = ReadOptions::for_column(0);
    assert_eq!(options.resolve_column_family_id_cached(&schema).unwrap(), 0);

    let options = options.with_column_family("remote-cf-1");
    assert_eq!(options.resolve_column_family_id_cached(&schema).unwrap(), 1);

    let options = options.with_column_family("missing");
    let err = options
        .resolve_column_family_id_cached(&schema)
        .expect_err("with_column_family should invalidate and re-resolve");
    assert!(matches!(err, Error::IoError(msg) if msg.contains("Unknown column family")));
}

#[test]
fn test_scan_options_cache_invalidates_via_with_column_family() {
    let schema = Arc::new(Schema::new(7, 3, vec![]));
    let options = ScanOptions::for_column(0);

    let first = options.resolve_cached(&schema).unwrap();
    assert_eq!(first.effective_schema.num_columns_in_family(0), Some(1));

    let options = options.with_column_family("missing");
    let err = match options.resolve_cached(&schema) {
        Ok(_) => panic!("with_column_family should invalidate and re-resolve"),
        Err(err) => err,
    };
    assert!(matches!(err, Error::IoError(msg) if msg.contains("Unknown column family")));
}

#[test]
fn test_normalize_volume_paths_converts_local_absolute_path() {
    let mut config = Config::default();
    let local = std::env::temp_dir().join("cobble-config-normalize");
    config.volumes = VolumeDescriptor::single_volume(local.to_string_lossy().to_string());
    let config = config.normalize_volume_paths().unwrap();
    assert!(config.volumes[0].base_dir.starts_with("file://"));
}

#[test]
fn test_hybrid_cache_prefers_cache_only_volume() {
    let config = Config {
        block_cache_hybrid_enabled: true,
        block_cache_hybrid_disk_size: Some(Size::from_kib(1)),
        volumes: vec![
            VolumeDescriptor::new(
                "file:///tmp/primary-shared".to_string(),
                vec![
                    VolumeUsageKind::PrimaryDataPriorityHigh,
                    VolumeUsageKind::Cache,
                    VolumeUsageKind::Meta,
                ],
            ),
            VolumeDescriptor::new(
                "file:///tmp/cache-only".to_string(),
                vec![VolumeUsageKind::Cache],
            ),
        ],
        ..Config::default()
    };
    let plan = config
        .resolve_hybrid_cache_volume_plan(2048)
        .unwrap()
        .unwrap();
    assert_eq!(plan.volume_idx, 1);
    assert!(!plan.shared_with_primary);
    assert_eq!(plan.disk_capacity_bytes, 1024);
}

#[test]
fn test_hybrid_cache_partitions_shared_volume_limit() {
    let mut shared = VolumeDescriptor::new(
        "file:///tmp/shared".to_string(),
        vec![
            VolumeUsageKind::PrimaryDataPriorityHigh,
            VolumeUsageKind::Cache,
            VolumeUsageKind::Meta,
        ],
    );
    shared.size_limit = Some(Size::from_kib(8));
    let config = Config {
        block_cache_hybrid_enabled: true,
        block_cache_hybrid_disk_size: Some(Size::from_kib(1)),
        volumes: vec![shared],
        ..Config::default()
    };
    let plan = config.resolve_hybrid_cache_volume_plan(4096).unwrap();
    let adjusted = config
        .apply_hybrid_cache_primary_partition_with_plan(plan.as_ref())
        .unwrap();
    assert_eq!(adjusted.volumes[0].size_limit, Some(Size::from_kib(7)));
}

#[test]
fn test_hybrid_cache_rejects_non_local_cache_volume() {
    let config = Config {
        block_cache_hybrid_enabled: true,
        block_cache_hybrid_disk_size: Some(Size::from_kib(1)),
        volumes: vec![VolumeDescriptor::new(
            "s3://bucket/cache".to_string(),
            vec![VolumeUsageKind::Cache],
        )],
        ..Config::default()
    };
    let err = config.resolve_hybrid_cache_volume_plan(2048).unwrap_err();
    assert!(matches!(err, Error::ConfigError(_)));
}

#[test]
fn test_data_file_type_missing_field_is_rejected() {
    let json = r#"{
            "volumes": [{"base_dir":"file:///tmp/cobble","kinds":["meta","primary_data_priority_high"]}],
            "num_columns": 1
        }"#;
    let err = serde_json::from_str::<Config>(json).unwrap_err();
    assert!(err.to_string().contains("missing field"));
}

#[test]
fn test_data_file_type_parquet_round_trip() {
    let expected = Config {
        data_file_type: DataFileType::Parquet,
        parquet_row_group_size_bytes: Size::from_kib(8),
        ..Config::default()
    };
    let json = serde_json::to_string(&expected).expect("Cannot serialize config");
    let decoded: Config = serde_json::from_str(&json).expect("Cannot deserialize parquet config");
    assert_eq!(decoded.data_file_type, DataFileType::Parquet);
    assert_eq!(decoded.parquet_row_group_size_bytes, Size::from_kib(8));
}

#[test]
fn test_config_from_path_allows_partial_entries() {
    let json = r#"{
            "volumes": [{"base_dir":"file:///tmp/cobble","kinds":["meta","primary_data_priority_high"]}],
            "memtable_capacity": 2048
        }"#;
    let mut json_file = Builder::new()
        .suffix(".json")
        .tempfile()
        .expect("Should create temp json");
    json_file
        .write_all(json.as_bytes())
        .expect("Should be able to write json");
    json_file.flush().expect("Should be able to flush json");

    let decoded = Config::from_path(json_file.path()).expect("Cannot deserialize partial json");
    assert_eq!(decoded.memtable_capacity, Size::from_kib(2));
    assert_eq!(decoded.num_columns, Config::default().num_columns);
    assert_eq!(decoded.data_file_type, Config::default().data_file_type);
    assert!(decoded.block_checksum_enabled);
}

#[test]
fn test_config_from_json_str_allows_partial_entries() {
    let json = r#"{
            "volumes": [{"base_dir":"file:///tmp/cobble","kinds":["meta","primary_data_priority_high"]}],
            "memtable_capacity": 2048
        }"#;
    let decoded = Config::from_json_str(json).expect("Cannot deserialize partial json");
    assert_eq!(decoded.memtable_capacity, Size::from_kib(2));
    assert_eq!(decoded.memtable_type, MemtableType::Adaptive);
    assert_eq!(decoded.num_columns, Config::default().num_columns);
    assert_eq!(decoded.data_file_type, Config::default().data_file_type);
    assert!(decoded.block_checksum_enabled);
    assert_eq!(decoded.governance_mode, Config::default().governance_mode);
}

#[test]
fn test_config_from_json_str_parses_noop_governance_mode() {
    let json = r#"{
            "volumes": [{"base_dir":"file:///tmp/cobble","kinds":["meta","primary_data_priority_high"]}],
            "governance_mode": "noop"
        }"#;
    let decoded = Config::from_json_str(json).expect("Cannot deserialize noop governance json");
    assert_eq!(decoded.governance_mode, GovernanceMode::Noop);
}

#[test]
fn test_config_from_path_parses_human_readable_sizes() {
    let yaml = r#"
        volumes:
          - base_dir: "file:///tmp/cobble"
            kinds: ["meta", "primary_data_priority_high"]
            size_limit: "2GiB"
        memtable_capacity: "64MB"
        l1_base_bytes: "128MiB"
        block_cache_size: "32MB"
        block_cache_hybrid_disk_size: "1GiB"
        reader:
          pin_partition_in_memory_count: 1
          block_cache_size: "512MB"
          reload_tolerance_seconds: 10
        base_file_size: "64MiB"
        parquet_row_group_size_bytes: "256KB"
        value_separation_threshold: "4MB"
        "#;
    let mut file = Builder::new()
        .suffix(".yaml")
        .tempfile()
        .expect("should create temp yaml");
    file.write_all(yaml.as_bytes())
        .expect("should write temp yaml");
    file.flush().expect("should flush temp yaml");

    let decoded = Config::from_path(file.path()).expect("should parse human-readable sizes");
    assert_eq!(decoded.memtable_capacity, Size::from_const(64_000_000));
    assert_eq!(decoded.l1_base_bytes, Size::from_mib(128));
    assert_eq!(decoded.block_cache_size, Size::from_const(32_000_000));
    assert_eq!(
        decoded.block_cache_hybrid_disk_size,
        Some(Size::from_gib(1))
    );
    assert_eq!(
        decoded.reader.block_cache_size,
        Size::from_const(512_000_000)
    );
    assert_eq!(decoded.base_file_size, Size::from_mib(64));
    assert_eq!(
        decoded.parquet_row_group_size_bytes,
        Size::from_const(256_000)
    );
    assert_eq!(
        decoded.value_separation_threshold,
        Some(Size::from_const(4_000_000))
    );
    assert_eq!(decoded.volumes[0].size_limit, Some(Size::from_gib(2)));
}

#[test]
fn test_config_from_path_rejects_invalid_size_unit() {
    let yaml = r#"
        volumes:
          - base_dir: "file:///tmp/cobble"
            kinds: ["meta", "primary_data_priority_high"]
        memtable_capacity: "64MEGA"
        "#;
    let mut file = Builder::new()
        .suffix(".yaml")
        .tempfile()
        .expect("should create temp yaml");
    file.write_all(yaml.as_bytes())
        .expect("should write temp yaml");
    file.flush().expect("should flush temp yaml");

    let err = Config::from_path(file.path()).expect_err("invalid unit should be rejected");
    assert!(matches!(err, Error::ConfigError(_)));
}

#[test]
fn test_collect_unrecognized_entry_paths() {
    let provided = serde_json::json!({
        "num_columns": 1,
        "unknown_top": 1,
        "reader": {
            "block_cache_size": 1024,
            "unknown_nested": true
        },
        "volumes": [{
            "base_dir": "file:///tmp/cobble",
            "kinds": ["meta", "primary_data_priority_high"],
            "unknown_volume_key": "x"
        }]
    });
    let schema = serde_json::to_value(Config::default()).expect("serialize default config");
    let unknown = super::collect_unrecognized_entry_paths(&provided, &schema, "");
    assert!(unknown.contains(&"unknown_top".to_string()));
    assert!(unknown.contains(&"reader.unknown_nested".to_string()));
    assert!(unknown.contains(&"volumes[0].unknown_volume_key".to_string()));
}

#[test]
fn test_template_config_yaml_is_valid() {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../template/config.yaml");
    let parsed = Config::from_path(path).expect("template/config.yaml should be valid");
    assert_eq!(parsed.total_buckets, 1);
    assert_eq!(parsed.memtable_type, MemtableType::Skiplist);
    assert_eq!(parsed.data_file_type, DataFileType::SSTable);
    assert_eq!(
        parsed.primary_volume_offload_policy,
        PrimaryVolumeOffloadPolicyKind::Priority
    );
    assert_eq!(parsed.volumes.len(), 1);
    assert!(parsed.volumes[0].supports(VolumeUsageKind::PrimaryDataPriorityHigh));
    assert!(parsed.volumes[0].supports(VolumeUsageKind::Meta));
}
