use base64::{Engine as _, engine::general_purpose::STANDARD};
use cobble::{Config, CoordinatorConfig, DbBuilder, DbCoordinator, SingleDb, VolumeDescriptor};
use cobble_table::catalog::{Catalog, FileCatalog, FileCatalogConfig, TableIdentifier};
use cobble_table::snapshot::TableSnapshotCommitter;
use cobble_table::{LogicalType, SchemaChange, TableSchema, Value};
use cobble_web_monitor::{MonitorConfig, MonitorConfigSource, MonitorServer};
use serde_json::{Value as JsonValue, json};
use serial_test::serial;
use size::Size;
use std::path::PathBuf;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use uuid::Uuid;

fn test_root(prefix: &str) -> String {
    format!("/tmp/{}_{}", prefix, Uuid::new_v4())
}

fn write_config_file(root: &str) -> (PathBuf, Config) {
    let config = Config {
        total_buckets: 4,
        num_columns: 1,
        volumes: VolumeDescriptor::single_volume(format!("file://{}", root)),
        memtable_capacity: Size::from_const(256),
        ..Config::default()
    };
    let config_json = serde_json::to_string_pretty(&config).expect("serialize config");
    let config_path = PathBuf::from(root).join("monitor-config.json");
    std::fs::create_dir_all(root).expect("create root");
    std::fs::write(&config_path, config_json).expect("write config file");
    (config_path, config)
}

fn wait_until_ready(base_url: &str) {
    let client = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .expect("build reqwest client");
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while std::time::Instant::now() <= deadline {
        if let Ok(resp) = client.get(format!("{}/healthz", base_url)).send()
            && resp.status().is_success()
        {
            return;
        }
        thread::sleep(Duration::from_millis(25));
    }
    panic!("monitor server was not ready in time");
}

#[test]
#[serial(file)]
fn test_monitor_snapshots_mode_switch_and_inspect() {
    let root = test_root("monitor_mode_switch");
    let (_config_path, config) = write_config_file(&root);

    let db = SingleDb::open(config.clone()).unwrap();
    db.put(0, b"user:0001", 0, b"alice").unwrap();
    let snapshot_id_1 = db.snapshot().unwrap();

    db.put(0, b"user:0001", 0, b"alice-v2").unwrap();
    db.put(0, b"user:0002", 0, b"bob").unwrap();
    let snapshot_id_2 = db.snapshot().unwrap();

    let row = db
        .get(0, b"user:0001")
        .unwrap()
        .expect("source value exists");
    assert_eq!(row[0].as_ref().unwrap().as_ref(), b"alice-v2");
    db.close().unwrap();

    let mut server = MonitorServer::new(MonitorConfig {
        source: MonitorConfigSource::Config(Box::new(config.clone())),
        bind_addr: "127.0.0.1:0".to_string(),
        global_snapshot_id: Some(snapshot_id_1),
        inspect_default_limit: 1,
        inspect_max_limit: 10,
    })
    .expect("create monitor server");
    let handle = server.serve().expect("serve monitor server");
    let base_url = handle.base_url();
    wait_until_ready(&base_url);

    let client = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("build reqwest client");

    let list_resp: JsonValue = client
        .get(format!("{}/api/v1/snapshots", base_url))
        .send()
        .expect("snapshots request")
        .error_for_status()
        .expect("snapshots status")
        .json()
        .expect("snapshots json");
    let snapshots = list_resp["snapshots"].as_array().expect("snapshots array");
    assert!(snapshots.len() >= 2);

    let mode_resp: JsonValue = client
        .post(format!("{}/api/v1/mode", base_url))
        .json(&json!({"mode": "snapshot", "snapshot_id": snapshot_id_2}))
        .send()
        .expect("switch mode request")
        .error_for_status()
        .expect("switch mode status")
        .json()
        .expect("switch mode json");
    assert_eq!(mode_resp["read_mode"], "snapshot");
    assert_eq!(mode_resp["configured_snapshot_id"], snapshot_id_2);

    let meta_resp: JsonValue = client
        .get(format!("{}/api/v1/meta", base_url))
        .send()
        .expect("meta request")
        .error_for_status()
        .expect("meta status")
        .json()
        .expect("meta json");
    assert_eq!(meta_resp["read_mode"], "snapshot");
    assert_eq!(meta_resp["configured_snapshot_id"], snapshot_id_2);

    let lookup_items_payload = serde_json::to_string(&json!([
        {"bucket": 0, "key_b64": STANDARD.encode(b"user:0001")},
        {"bucket": 0, "key_b64": STANDARD.encode(b"user:9999")}
    ]))
    .expect("serialize lookup payload");
    let lookup_resp: JsonValue = client
        .get(format!("{}/api/v1/inspect", base_url))
        .query(&[
            ("mode", "lookup"),
            ("lookup_items", lookup_items_payload.as_str()),
        ])
        .send()
        .expect("inspect lookup request")
        .error_for_status()
        .expect("inspect lookup status")
        .json()
        .expect("inspect lookup json");
    assert_eq!(lookup_resp["mode"], "lookup");
    assert_eq!(lookup_resp["refreshed"], false);

    let lookup_items = lookup_resp["lookup"].as_array().expect("lookup array");
    assert_eq!(lookup_items.len(), 2);
    assert_eq!(lookup_items[0]["bucket"], 0);
    let col0_b64 = lookup_items[0]["value"][0]["b64"]
        .as_str()
        .expect("column b64");
    assert_eq!(STANDARD.decode(col0_b64).expect("decode col"), b"alice-v2");

    let current_mode_resp: JsonValue = client
        .post(format!("{}/api/v1/mode", base_url))
        .json(&json!({"mode": "current"}))
        .send()
        .expect("switch current mode request")
        .error_for_status()
        .expect("switch current mode status")
        .json()
        .expect("switch current mode json");
    assert_eq!(current_mode_resp["read_mode"], "current");

    let scan_resp: JsonValue = client
        .get(format!(
            "{}/api/v1/inspect?mode=scan&bucket=0&prefix=user:&limit=1",
            base_url
        ))
        .send()
        .expect("inspect scan request")
        .error_for_status()
        .expect("inspect scan status")
        .json()
        .expect("inspect scan json");
    assert_eq!(scan_resp["mode"], "scan");
    assert_eq!(scan_resp["refreshed"], true);

    let scan = scan_resp["scan"].as_object().expect("scan object");
    let items = scan["items"].as_array().expect("items array");
    assert_eq!(items.len(), 1);

    server.shutdown().expect("shutdown monitor server");
    let _ = std::fs::remove_dir_all(root);
}

#[test]
#[serial(file)]
fn test_table_inspection_uses_selected_snapshot_and_typed_projection() {
    let root = test_root("monitor_table_inspect");
    let config = Config {
        total_buckets: 1,
        volumes: VolumeDescriptor::single_volume(format!("file://{root}")),
        ..Config::default()
    };
    let catalog = FileCatalog::open(&config, FileCatalogConfig::new("monitor-test")).unwrap();
    catalog.create_namespace(vec!["app".into()]).unwrap();
    let table_id = TableIdentifier::new(["app"], "customers");
    catalog
        .create_table(
            table_id.clone(),
            TableSchema::builder()
                .field("id", LogicalType::int64())
                .field("name", LogicalType::string())
                .field("balance", LogicalType::decimal(18, 2))
                .field("tags", LogicalType::list(LogicalType::string()))
                .field("note", LogicalType::string().nullable())
                .primary_key(["id"])
                .bucket_key(["id"])
                .build()
                .unwrap(),
        )
        .unwrap();
    let db = Arc::new(
        DbBuilder::new(config.clone())
            .bucket_ranges(std::iter::once(0..=0).collect())
            .db_id("table-shard")
            .open()
            .unwrap(),
    );
    let table = catalog
        .materialize_table(Arc::clone(&db), &table_id)
        .unwrap();
    for (offset, name) in ["Alice", "Bob", "Carol"].iter().enumerate() {
        table
            .put(&[
                Value::Int64(9_007_199_254_740_993 + offset as i64),
                Value::String((*name).into()),
                Value::Decimal {
                    precision: 18,
                    scale: 2,
                    unscaled: 125050 + offset as i128,
                },
                Value::List(vec![Value::String("member".into())]),
                Value::Null,
            ])
            .unwrap();
    }
    let committer = TableSnapshotCommitter::new(
        Arc::new(DbCoordinator::open(CoordinatorConfig::from_config(&config)).unwrap()),
        1,
        4,
    )
    .unwrap();
    let first = committer
        .commit_batch(1, vec![table.snapshot_and_wait().unwrap()])
        .unwrap()
        .unwrap();
    drop(table);
    catalog
        .evolve_schema(
            &table_id,
            vec![SchemaChange::AddField {
                name: "region".into(),
                logical_type: LogicalType::string().nullable(),
            }],
        )
        .unwrap();
    let table = catalog
        .materialize_table(Arc::clone(&db), &table_id)
        .unwrap();
    table
        .put(&[
            Value::Int64(9_007_199_254_740_996),
            Value::String("Dora".into()),
            Value::Decimal {
                precision: 18,
                scale: 2,
                unscaled: 250000,
            },
            Value::List(vec![]),
            Value::String("new".into()),
            Value::String("Europe".into()),
        ])
        .unwrap();
    let second = committer
        .commit_batch(2, vec![table.snapshot_and_wait().unwrap()])
        .unwrap()
        .unwrap();
    drop(table);
    db.close().unwrap();

    let mut server = MonitorServer::new(MonitorConfig {
        source: MonitorConfigSource::Config(Box::new(config)),
        global_snapshot_id: Some(first.id),
        inspect_default_limit: 2,
        inspect_max_limit: 10,
        ..MonitorConfig::default()
    })
    .unwrap();
    let base = server.serve().unwrap().base_url();
    wait_until_ready(&base);
    let client = reqwest::blocking::Client::new();

    let tables: JsonValue = client
        .get(format!("{base}/api/v1/tables"))
        .send()
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .unwrap();
    assert_eq!(tables["snapshot_id"], first.id);
    let table_name = tables["tables"][0]["name"].as_str().unwrap().to_string();
    assert_eq!(tables["tables"][0]["schema"]["fields"][0]["name"], "id");
    assert_eq!(
        tables["tables"][0]["schema"]["fields"]
            .as_array()
            .unwrap()
            .len(),
        5
    );

    let fields = r#"["name","balance","tags","note"]"#;
    let first_page: JsonValue = client
        .get(format!("{base}/api/v1/table/inspect"))
        .query(&[
            ("table", table_name.as_str()),
            ("mode", "scan"),
            ("bucket", "0"),
            ("limit", "2"),
            ("fields", fields),
        ])
        .send()
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .unwrap();
    assert_eq!(
        first_page["fields"],
        json!(["name", "balance", "tags", "note"])
    );
    assert_eq!(first_page["scan"]["items"].as_array().unwrap().len(), 2);
    assert_eq!(
        first_page["scan"]["items"][0]["values"],
        json!(["Alice", "1250.50", ["member"], null])
    );
    assert_eq!(first_page["scan"]["has_more"], true);
    let cursor = serde_json::to_string(&first_page["scan"]["next_start_after"]).unwrap();
    assert_eq!(
        first_page["scan"]["next_start_after"]["key"],
        json!(["9007199254740994"])
    );
    let second_page: JsonValue = client
        .get(format!("{base}/api/v1/table/inspect"))
        .query(&[
            ("table", table_name.as_str()),
            ("mode", "scan"),
            ("bucket", "0"),
            ("limit", "2"),
            ("fields", fields),
            ("start_after", cursor.as_str()),
        ])
        .send()
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .unwrap();
    assert_eq!(second_page["scan"]["items"][0]["values"][0], "Carol");
    assert_eq!(second_page["scan"]["has_more"], false);
    let changed_projection = client
        .get(format!("{base}/api/v1/table/inspect"))
        .query(&[
            ("table", table_name.as_str()),
            ("mode", "scan"),
            ("bucket", "0"),
            ("fields", r#"["id"]"#),
            ("start_after", cursor.as_str()),
        ])
        .send()
        .unwrap();
    assert_eq!(
        changed_projection.status(),
        reqwest::StatusCode::BAD_REQUEST
    );

    let lookup: JsonValue = client
        .get(format!("{base}/api/v1/table/inspect"))
        .query(&[
            ("table", table_name.as_str()),
            ("mode", "lookup"),
            ("key", r#"["9007199254740993"]"#),
            ("fields", r#"["id","balance"]"#),
        ])
        .send()
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .unwrap();
    assert_eq!(
        lookup["lookup"]["values"],
        json!(["9007199254740993", "1250.50"])
    );
    let invalid = client
        .get(format!("{base}/api/v1/table/inspect"))
        .query(&[
            ("table", table_name.as_str()),
            ("mode", "lookup"),
            ("key", r#"["not-an-int"]"#),
        ])
        .send()
        .unwrap();
    assert_eq!(invalid.status(), reqwest::StatusCode::BAD_REQUEST);

    client
        .post(format!("{base}/api/v1/mode"))
        .json(&json!({"mode":"snapshot", "snapshot_id":second.id}))
        .send()
        .unwrap()
        .error_for_status()
        .unwrap();
    let changed: JsonValue = client
        .get(format!("{base}/api/v1/tables"))
        .send()
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .unwrap();
    assert_eq!(changed["snapshot_id"], second.id);
    assert_eq!(
        changed["tables"][0]["schema"]["fields"]
            .as_array()
            .unwrap()
            .len(),
        6
    );
    assert_eq!(
        changed["tables"][0]["schema"]["fields"][5]["name"],
        "region"
    );
    let stale = client
        .get(format!("{base}/api/v1/table/inspect"))
        .query(&[
            ("table", table_name.as_str()),
            ("mode", "scan"),
            ("bucket", "0"),
            ("limit", "2"),
            ("fields", fields),
            ("start_after", cursor.as_str()),
        ])
        .send()
        .unwrap();
    assert_eq!(stale.status(), reqwest::StatusCode::BAD_REQUEST);
    let new_row: JsonValue = client
        .get(format!("{base}/api/v1/table/inspect"))
        .query(&[
            ("table", table_name.as_str()),
            ("mode", "lookup"),
            ("key", r#"["9007199254740996"]"#),
        ])
        .send()
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .unwrap();
    assert_eq!(new_row["lookup"]["values"][1], "Dora");
    assert_eq!(new_row["lookup"]["values"][5], "Europe");
    let old_row: JsonValue = client
        .get(format!("{base}/api/v1/table/inspect"))
        .query(&[
            ("table", table_name.as_str()),
            ("mode", "lookup"),
            ("key", r#"["9007199254740993"]"#),
            ("fields", r#"["id","region"]"#),
        ])
        .send()
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .unwrap();
    assert_eq!(
        old_row["lookup"]["values"],
        json!(["9007199254740993", null])
    );

    server.shutdown().unwrap();
    let _ = std::fs::remove_dir_all(root);
}

#[test]
#[serial(file)]
fn test_monitor_config_source_config_path_works() {
    let root = test_root("monitor_config_path");
    let (config_path, config) = write_config_file(&root);

    let db = SingleDb::open(config).unwrap();
    db.put(0, b"k1", 0, b"v1").unwrap();
    let _snapshot = db.snapshot().unwrap();
    db.close().unwrap();

    let mut server = MonitorServer::new(MonitorConfig {
        source: MonitorConfigSource::ConfigPath(config_path.to_string_lossy().to_string()),
        bind_addr: "127.0.0.1:0".to_string(),
        global_snapshot_id: None,
        inspect_default_limit: 10,
        inspect_max_limit: 100,
    })
    .expect("create monitor server with path source");
    let handle = server.serve().expect("serve monitor server");
    let base_url = handle.base_url();
    wait_until_ready(&base_url);

    let client = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("build reqwest client");
    let meta_resp: JsonValue = client
        .get(format!("{}/api/v1/meta", base_url))
        .send()
        .expect("meta request")
        .error_for_status()
        .expect("meta status")
        .json()
        .expect("meta json");
    assert_eq!(meta_resp["read_mode"], "current");

    server.shutdown().expect("shutdown monitor server");
    let _ = std::fs::remove_dir_all(root);
}

#[test]
#[serial(file)]
fn test_monitor_reject_invalid_mode_switch() {
    let root = test_root("monitor_invalid_mode_switch");
    let (_config_path, config) = write_config_file(&root);

    let db = SingleDb::open(config.clone()).unwrap();
    db.put(0, b"k1", 0, b"v1").unwrap();
    let _snapshot_id = db.snapshot().unwrap();
    db.close().unwrap();

    let mut server = MonitorServer::new(MonitorConfig {
        source: MonitorConfigSource::Config(Box::new(config)),
        bind_addr: "127.0.0.1:0".to_string(),
        global_snapshot_id: None,
        inspect_default_limit: 10,
        inspect_max_limit: 100,
    })
    .expect("create monitor server");
    let handle = server.serve().expect("serve monitor server");
    let base_url = handle.base_url();
    wait_until_ready(&base_url);

    let client = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("build reqwest client");

    let resp = client
        .post(format!("{}/api/v1/mode", base_url))
        .json(&json!({"mode": "snapshot"}))
        .send()
        .expect("mode request without snapshot id");
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);

    let resp = client
        .post(format!("{}/api/v1/mode", base_url))
        .json(&json!({"mode": "bad"}))
        .send()
        .expect("mode request invalid mode");
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);

    server.shutdown().expect("shutdown monitor server");
    let _ = std::fs::remove_dir_all(root);
}

#[test]
#[serial(file)]
fn test_monitor_scan_empty_prefix_returns_rows() {
    let root = test_root("monitor_scan_empty_prefix");
    let (_config_path, config) = write_config_file(&root);

    let db = SingleDb::open(config.clone()).unwrap();
    db.put(0, b"a-key", 0, b"v-a").unwrap();
    db.put(0, b"b-key", 0, b"v-b").unwrap();
    let _snapshot_id = db.snapshot().unwrap();
    db.close().unwrap();

    let mut server = MonitorServer::new(MonitorConfig {
        source: MonitorConfigSource::Config(Box::new(config)),
        bind_addr: "127.0.0.1:0".to_string(),
        global_snapshot_id: None,
        inspect_default_limit: 10,
        inspect_max_limit: 100,
    })
    .expect("create monitor server");
    let handle = server.serve().expect("serve monitor server");
    let base_url = handle.base_url();
    wait_until_ready(&base_url);

    let client = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("build reqwest client");

    let scan_resp: JsonValue = client
        .get(format!(
            "{}/api/v1/inspect?mode=scan&bucket=0&prefix=&limit=5",
            base_url
        ))
        .send()
        .expect("inspect scan request")
        .error_for_status()
        .expect("inspect scan status")
        .json()
        .expect("inspect scan json");
    assert_eq!(scan_resp["mode"], "scan");
    let scan = scan_resp["scan"].as_object().expect("scan object");
    let items = scan["items"].as_array().expect("items array");
    assert!(!items.is_empty());

    server.shutdown().expect("shutdown monitor server");
    let _ = std::fs::remove_dir_all(root);
}

#[test]
#[serial(file)]
fn test_monitor_lookup_items_support_row_bucket() {
    let root = test_root("monitor_lookup_row_bucket");
    let (_config_path, config) = write_config_file(&root);

    let db = SingleDb::open(config.clone()).unwrap();
    db.put(0, b"k-bucket-0", 0, b"v0").unwrap();
    db.put(1, b"k-bucket-1", 0, b"v1").unwrap();
    let _snapshot = db.snapshot().unwrap();
    db.close().unwrap();

    let mut server = MonitorServer::new(MonitorConfig {
        source: MonitorConfigSource::Config(Box::new(config)),
        bind_addr: "127.0.0.1:0".to_string(),
        global_snapshot_id: None,
        inspect_default_limit: 10,
        inspect_max_limit: 100,
    })
    .expect("create monitor server");
    let handle = server.serve().expect("serve monitor server");
    let base_url = handle.base_url();
    wait_until_ready(&base_url);

    let client = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("build reqwest client");

    let row_items = serde_json::to_string(&json!([
        {"bucket": 0, "key_b64": STANDARD.encode(b"k-bucket-0")},
        {"bucket": 1, "key_b64": STANDARD.encode(b"k-bucket-1")}
    ]))
    .expect("serialize row items");
    let lookup_resp: JsonValue = client
        .get(format!("{}/api/v1/inspect", base_url))
        .query(&[("mode", "lookup"), ("lookup_items", row_items.as_str())])
        .send()
        .expect("lookup request")
        .error_for_status()
        .expect("lookup status")
        .json()
        .expect("lookup json");
    let items = lookup_resp["lookup"].as_array().expect("lookup array");
    assert_eq!(items.len(), 2);
    assert_eq!(items[0]["bucket"], 0);
    assert_eq!(items[1]["bucket"], 1);
    assert_eq!(
        STANDARD
            .decode(items[0]["value"][0]["b64"].as_str().unwrap())
            .unwrap(),
        b"v0"
    );
    assert_eq!(
        STANDARD
            .decode(items[1]["value"][0]["b64"].as_str().unwrap())
            .unwrap(),
        b"v1"
    );

    server.shutdown().expect("shutdown monitor server");
    let _ = std::fs::remove_dir_all(root);
}
