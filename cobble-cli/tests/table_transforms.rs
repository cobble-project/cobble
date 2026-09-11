use cobble::{CompactionMode, Config, DbBuilder, RemoteCompactionFailureMode, VolumeDescriptor};
use cobble_table::catalog::{Catalog, FileCatalog, FileCatalogConfig, TableIdentifier};
use cobble_table::{
    LogicalType, ReadOnlyTableBuilder, SchemaChange, TableSchema, Value, register_schema_transforms,
};
use size::Size;
use std::net::{TcpListener, TcpStream};
use std::process::{Child, Command};
use std::sync::Arc;
use std::time::{Duration, Instant};

struct Compactor(Child);

impl Compactor {
    fn assert_running(&mut self) {
        assert!(self.0.try_wait().unwrap().is_none(), "CLI compactor exited");
    }
}

impl Drop for Compactor {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[test]
fn cli_compactors_materialize_builtin_table_transforms() {
    for dedicated in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap().to_string();
        drop(listener);
        let config = Config {
            volumes: VolumeDescriptor::single_volume(format!("file://{}", root.path().display())),
            total_buckets: 1,
            l0_file_limit: 2,
            memtable_capacity: Size::from_mib(1),
            block_cache_size: Size::from_mib(1),
            base_file_size: Size::from_mib(1),
            value_separation_threshold: Some(Size::from_const(1)),
            snapshot_disable_incremental_base_link: true,
            compaction_mode: if dedicated {
                CompactionMode::Dedicated
            } else {
                CompactionMode::Embedded
            },
            compaction_remote_addr: (!dedicated).then_some(address.clone()),
            // A local fallback would hide a missing CLI transform registration.
            compaction_remote_failure_mode: RemoteCompactionFailureMode::Skip,
            compaction_dedicated_poll_interval_ms: 50,
            compaction_threads: 1,
            log_console: false,
            ..Config::default()
        };
        let config_path = root.path().join("config.json");
        std::fs::write(&config_path, serde_json::to_vec(&config).unwrap()).unwrap();
        let mut command = Command::new(env!("CARGO_BIN_EXE_cobble-cli"));
        if dedicated {
            command
                .arg("compact")
                .arg("--config")
                .arg(&config_path)
                .arg(root.path().join("shard"));
        } else {
            command
                .arg("remote-compactor")
                .arg("--config")
                .arg(&config_path)
                .arg("--bind")
                .arg(&address);
        }

        let mut compactor = if dedicated {
            None
        } else {
            let mut child = Compactor(command.spawn().unwrap());
            let deadline = Instant::now() + Duration::from_secs(60);
            while TcpStream::connect(&address).is_err() {
                child.assert_running();
                assert!(Instant::now() < deadline, "remote CLI did not listen");
                std::thread::sleep(Duration::from_millis(50));
            }
            Some(child)
        };
        let catalog = FileCatalog::open(&config, FileCatalogConfig::new("catalog")).unwrap();
        catalog.create_namespace(vec!["test".into()]).unwrap();
        let identifier = TableIdentifier::new(["test"], "numbers");
        catalog
            .create_table(
                identifier.clone(),
                TableSchema::builder()
                    .field("id", LogicalType::int64())
                    .field("value", LogicalType::int16())
                    .primary_key(["id"])
                    .bucket_key(["id"])
                    .build()
                    .unwrap(),
            )
            .unwrap();
        // Raw core handles explicitly install the factory; CLI processes must install it themselves.
        let builder = DbBuilder::new(config.clone())
            .db_id("shard")
            .bucket_ranges(vec![0..=0]);
        register_schema_transforms(&builder).unwrap();
        let db = Arc::new(builder.open().unwrap());
        let old = catalog
            .materialize_table(Arc::clone(&db), &identifier)
            .unwrap();
        for id in 0..64 {
            old.put(&[Value::Int64(id), Value::Int16(id as i16)])
                .unwrap();
        }
        old.snapshot_and_wait().unwrap();
        drop(old);
        // No file references the intermediate Int32 schema, but both conversions are required.
        for logical_type in [LogicalType::int32(), LogicalType::int64().nullable()] {
            catalog
                .evolve_schema(
                    &identifier,
                    vec![SchemaChange::AlterFieldType {
                        field_name: "value".into(),
                        logical_type,
                    }],
                )
                .unwrap();
        }
        let table = catalog
            .materialize_table(Arc::clone(&db), &identifier)
            .unwrap();
        let target_schema = db.current_schema().version();
        for batch in 0..2 {
            for id in batch * 16..(batch + 1) * 16 {
                table
                    .put(&[Value::Int64(id), Value::Int64(300_000 + id)])
                    .unwrap();
            }
            table.snapshot_and_wait().unwrap();
        }
        if dedicated {
            compactor = Some(Compactor(command.spawn().unwrap()));
        }
        let mut compactor = compactor.unwrap();
        let deadline = Instant::now() + Duration::from_secs(60);
        let snapshot = loop {
            compactor.assert_running();
            let snapshot = table.snapshot_and_wait().unwrap();
            let manifest: serde_json::Value = serde_json::from_slice(
                &cobble::test_utils::read_metadata_payload_from_path_for_test(
                    snapshot
                        .manifest_path
                        .strip_prefix("file://")
                        .unwrap_or(&snapshot.manifest_path),
                )
                .unwrap(),
            )
            .unwrap();
            let files = manifest["tree_levels"]
                .as_array()
                .unwrap()
                .iter()
                .flat_map(|tree| tree.as_array().unwrap())
                .flat_map(|level| level["files"].as_array().unwrap())
                .collect::<Vec<_>>();
            if !files.is_empty() && files.iter().all(|file| file["schema_id"] == target_schema) {
                break snapshot;
            }
            assert!(
                Instant::now() < deadline,
                "CLI did not materialize old SSTs (dedicated={dedicated})"
            );
            std::thread::sleep(Duration::from_millis(250));
        };
        let name = snapshot
            .column_families
            .iter()
            .find(|(_, family)| {
                family
                    .options
                    .metadata
                    .as_ref()
                    .is_some_and(|metadata| metadata["format"] == "cobble-table")
            })
            .unwrap()
            .0
            .clone();
        let reader = ReadOnlyTableBuilder::new(config)
            .table_name(name)
            .shard_snapshot("shard", snapshot.snapshot_id)
            .open()
            .unwrap();
        for id in 0..64 {
            let mut key = reader.key_builder();
            key.push(Value::Int64(id));
            let key = key.build().unwrap();
            let value = if id < 32 { 300_000 + id } else { id };
            assert_eq!(
                reader.get(&key).unwrap(),
                Some(vec![Value::Int64(id), Value::Int64(value)])
            );
        }
        drop(reader);
        drop(table);
        db.close().unwrap();
    }
}
