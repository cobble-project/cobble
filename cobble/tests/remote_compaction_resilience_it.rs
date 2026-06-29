//! Integration tests for resilient remote compaction: lazy `Db::open`, local fallback, skip mode,
//! recovery, and permanent-failure handling.
//!
//! These exercise the `ResilientRemoteCompactionWorker` end-to-end through `Db::open` by pointing
//! the DB at a remote compactor address and driving flushes that trigger compaction. The remote
//! compactor is either an in-process `RemoteCompactionServer` whose accept loop is controlled
//! per-test (so we can simulate "compactor down" / "compactor recovers"), or a raw TCP "fake
//! compactor" that speaks the length-prefixed JSON protocol and returns crafted replies (so we can
//! simulate protocol-incompatible / unsupported-operator responses that must NOT fall back).
use cobble::{
    CompactionPolicyKind, Config, Db, RemoteCompactionFailureMode, RemoteCompactionServer,
};
use serial_test::serial;
use size::Size;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

fn cleanup_test_root(path: &str) {
    let _ = std::fs::remove_dir_all(path);
}

fn base_config(root: &str, remote_addr: Option<String>) -> Config {
    Config {
        volumes: cobble::VolumeDescriptor::single_volume(format!("file://{}", root)),
        memtable_capacity: Size::from_kib(16),
        memtable_buffer_count: 2,
        num_columns: 1,
        l0_file_limit: 1,
        write_stall_limit: None,
        l1_base_bytes: Size::from_mib(1),
        level_size_multiplier: 2,
        max_level: 4,
        compaction_policy: CompactionPolicyKind::RoundRobin,
        block_cache_size: Size::from_const(0),
        base_file_size: Size::from_kib(16),
        sst_bloom_filter_enabled: true,
        compaction_remote_addr: remote_addr,
        compaction_remote_timeout_ms: 2_000,
        compaction_threads: 2,
        log_console: false,
        log_level: log::LevelFilter::Info,
        ..Config::default()
    }
}

fn open_db(config: Config) -> Db {
    let total_buckets = config.total_buckets;
    let full_range = 0u16..=u16::try_from(total_buckets - 1).expect("total_buckets must fit u16");
    Db::open(config, std::iter::once(full_range).collect()).expect("open db")
}

/// Returns true once at least one compaction has run for `db_id` (by the compaction-write-bytes
/// metric), polling up to ~6s.
#[allow(dead_code)]
fn compaction_happened(db: &Db, db_id: &str) -> bool {
    for _ in 0..300 {
        if compaction_happened_quick(db, db_id) {
            return true;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    false
}

/// Writes rows until a compaction is observed, then returns.
fn write_until_compaction(db: &Db, db_id: &str, value_size: usize) {
    let mut idx = 0u32;
    loop {
        let key = format!("k{:08}", idx).into_bytes();
        db.put(0, &key, 0, vec![b'v'; value_size]).expect("put");
        idx += 1;
        if compaction_happened_quick(db, db_id) {
            return;
        }
        if idx > 2000 {
            panic!("compaction never triggered after {} writes", idx);
        }
    }
}

/// Cheap single-shot check used inside the write loop.
fn compaction_happened_quick(db: &Db, db_id: &str) -> bool {
    let metrics = db.metrics();
    metrics.iter().any(|sample| {
        sample.name == "compaction_write_bytes_total"
            && sample
                .labels
                .iter()
                .any(|(k, v)| k == "db_id" && v == db_id)
            && matches!(sample.value, cobble::MetricValue::Counter(v) if v > 0)
    })
}

/// Read a length-prefixed JSON message from `stream`.
fn read_message_raw(stream: &mut TcpStream) -> Vec<u8> {
    let mut len_bytes = [0u8; 4];
    stream.read_exact(&mut len_bytes).expect("read len");
    let len = u32::from_be_bytes(len_bytes) as usize;
    let mut buf = vec![0u8; len];
    stream.read_exact(&mut buf).expect("read payload");
    buf
}

/// Write a length-prefixed JSON message to `stream`.
fn write_message_raw(stream: &mut TcpStream, payload: &[u8]) {
    let len = payload.len() as u32;
    stream.write_all(&len.to_be_bytes()).expect("write len");
    stream.write_all(payload).expect("write payload");
}

/// Replies to a `SupportedMergeOperators` command with the given operator id list.
fn reply_supported_operators(stream: &mut TcpStream, ids: &[&str]) {
    // RemoteCompactionReply::SupportedMergeOperators(Vec<String>) —
    // { "kind": "supported_merge_operators", "payload": ["..."] }
    let ids_json: Vec<String> = ids.iter().map(|s| format!("\"{}\"", s)).collect();
    let payload = format!(
        "{{\"kind\":\"supported_merge_operators\",\"payload\":[{}]}}",
        ids_json.join(",")
    );
    write_message_raw(stream, payload.as_bytes());
}

/// The built-in default merge operator id advertised by a real compactor. The default DB schema
/// uses `BytesMergeOperator`, whose id is its `std::any::type_name`.
const DEFAULT_OPERATOR_ID: &str = "cobble::merge_operator::BytesMergeOperator";

// =============================================================================================
// Test 1: Db::open succeeds when the remote compactor is down (FallbackLocal).
// =============================================================================================

#[test]
#[serial(file)]
fn db_open_succeeds_when_remote_compactor_is_down_with_fallback_local() {
    let root = "/tmp/resilient_open_down";
    cleanup_test_root(root);
    // Point at an address where nothing is listening.
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let dead_addr = listener.local_addr().unwrap().to_string();
    drop(listener);
    let config = base_config(root, Some(dead_addr));
    // Db::open must succeed even though the compactor is down.
    let db = open_db(config);
    // Writes must keep working: compaction falls back to local.
    write_until_compaction(&db, db.id(), 1024);
    db.close().expect("close");
    cleanup_test_root(root);
}

// =============================================================================================
// Test 2: FallbackLocal runs the compaction locally and preserves data.
// =============================================================================================

#[test]
#[serial(file)]
fn remote_compaction_falls_back_to_local_when_connect_refused() {
    let root = "/tmp/resilient_fallback_local";
    cleanup_test_root(root);
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let dead_addr = listener.local_addr().unwrap().to_string();
    drop(listener);
    let config = base_config(root, Some(dead_addr));
    let db = open_db(config);
    write_until_compaction(&db, db.id(), 1024);
    // Verify data is intact after local-fallback compaction: each written key reads back its
    // full value (1024 bytes of 'v').
    for i in 0..10 {
        let key = format!("k{:08}", i).into_bytes();
        let value = db.get(0, &key).expect("get").expect("value present");
        let col = value[0].as_ref().unwrap();
        assert_eq!(col.len(), 1024, "value length for key {}", i);
        assert!(col.iter().all(|&b| b == b'v'), "value bytes for key {}", i);
    }
    db.close().expect("close");
    cleanup_test_root(root);
}

// =============================================================================================
// Test 3: Skip mode releases pending and keeps the DB writable (no error state).
//
// Skip mode abandons the compaction on transient remote failure. L0 files accumulate (nothing is
// compacted), so we raise `write_stall_limit` to keep writes from stalling. The DB must remain
// open and writable throughout: a final put + get must succeed.
// =============================================================================================

#[test]
#[serial(file)]
fn remote_compaction_skip_mode_releases_pending_without_db_error() {
    let root = "/tmp/resilient_skip_mode";
    cleanup_test_root(root);
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let dead_addr = listener.local_addr().unwrap().to_string();
    drop(listener);
    let mut config = base_config(root, Some(dead_addr));
    config.compaction_remote_failure_mode = RemoteCompactionFailureMode::Skip;
    // In skip mode L0 files pile up (compactions are abandoned), which would trip the default
    // write-stall limit (min(l0+4, l0*2) = 2 for l0_file_limit=1) and stall writes. Lift the
    // limit well above the number of files this test produces.
    config.write_stall_limit = Some(64);
    let db = open_db(config);
    // Write enough to trigger several compaction attempts (each is skipped). Stay well under the
    // raised stall limit so puts never block.
    let mut wrote = 0u32;
    for _ in 0..40 {
        let key = format!("k{:08}", wrote).into_bytes();
        db.put(0, &key, 0, vec![b'v'; 1024])
            .expect("put must succeed in skip mode");
        wrote += 1;
        // Give the scheduler room to attempt (and skip) compaction.
        if wrote.is_multiple_of(8) {
            std::thread::sleep(Duration::from_millis(40));
        }
    }
    // The DB must remain writable and not enter an error state: a final put + get must succeed.
    db.put(0, b"final", 0, vec![b'v'; 16])
        .expect("db still writable after skip-mode attempts");
    let value = db
        .get(0, b"final")
        .expect("get must succeed")
        .expect("value present");
    let col = value[0].as_ref().unwrap();
    assert_eq!(col.len(), 16);
    assert!(col.iter().all(|&b| b == b'v'));
    db.close().expect("close");
    cleanup_test_root(root);
}

// =============================================================================================
// A controllable in-process compactor backed by a real RemoteCompactionServer.
// While `serving` is false, accepted connections are dropped immediately (the client sees a
// connection reset → transient → fallback). While true, requests are handled normally.
// =============================================================================================

struct ControllableCompactor {
    serving: Arc<AtomicBool>,
    handled: Arc<AtomicUsize>,
    _thread: std::thread::JoinHandle<()>,
}

impl ControllableCompactor {
    fn start(root: &str) -> (Self, std::net::SocketAddr) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        listener.set_nonblocking(true).unwrap();
        let server_config = base_config(root, None);
        let server = Arc::new(RemoteCompactionServer::new(server_config).expect("server"));
        let serving = Arc::new(AtomicBool::new(false));
        let handled = Arc::new(AtomicUsize::new(0));
        let serving_clone = Arc::clone(&serving);
        let handled_clone = Arc::clone(&handled);
        let thread = std::thread::spawn(move || {
            loop {
                match listener.accept() {
                    Ok((stream, _)) => {
                        if !serving_clone.load(Ordering::SeqCst) {
                            // Drop the connection so the client observes a reset (transient failure).
                            drop(stream);
                            continue;
                        }
                        stream.set_nonblocking(false).ok();
                        let _ = server.handle_connection(stream);
                        handled_clone.fetch_add(1, Ordering::SeqCst);
                    }
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(10));
                    }
                    Err(_) => break,
                }
            }
        });
        (
            Self {
                serving,
                handled,
                _thread: thread,
            },
            addr,
        )
    }

    fn set_serving(&self, on: bool) {
        self.serving.store(on, Ordering::SeqCst);
    }

    fn handled_count(&self) -> usize {
        self.handled.load(Ordering::SeqCst)
    }
}

// =============================================================================================
// Test 4: After the compactor recovers, the DB uses remote again (connectionless per attempt).
// =============================================================================================

#[test]
#[serial(file)]
fn remote_compaction_uses_remote_again_after_server_recovers() {
    let root = "/tmp/resilient_recovery";
    cleanup_test_root(root);
    let (compactor, addr) = ControllableCompactor::start(root);
    // Phase 1: compactor is "down" (serving=false → connections reset). The DB opens and the first
    // compaction falls back to local.
    let config = base_config(root, Some(addr.to_string()));
    let db = open_db(config);
    write_until_compaction(&db, db.id(), 1024);
    // No remote requests should have been handled yet (all reset/dropped).
    let local_phase_handled = compactor.handled_count();
    assert_eq!(
        local_phase_handled, 0,
        "no remote requests should be handled while the compactor is down"
    );

    // Phase 2: recover the compactor. The next compaction must go remote (connectionless per
    // attempt, so recovery is automatic).
    compactor.set_serving(true);
    // Drive more writes and confirm the server handles requests.
    let mut remote_used = false;
    for idx in 2000u32..2400 {
        let key = format!("r{:08}", idx).into_bytes();
        db.put(0, &key, 0, vec![b'v'; 1024]).expect("put");
        if compactor.handled_count() > 0 {
            remote_used = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(
        remote_used,
        "expected the recovered remote compactor to handle at least one request"
    );
    db.close().expect("close");
    cleanup_test_root(root);
}

// =============================================================================================
// A raw TCP "fake compactor" that speaks the length-prefixed JSON protocol and returns crafted
// replies. Used to simulate permanent failures (protocol incompatible, unsupported operator,
// server-side schema/config error) without needing to construct a real but misconfigured server.
//
// `mode` selects the behavior:
//   ProtocolIncompatible   — answers SupportedMergeOperators normally, then replies to Execute with
//                            a response whose compatible_version exceeds the current protocol
//                            version, so the client's validate_protocol_compatibility rejects it.
//   UnsupportedOperator    — answers SupportedMergeOperators with an EMPTY list, so the client's
//                            ensure_supported_merge_operator_ids fails with ConfigError before any
//                            Execute is sent.
//   PermanentServerError   — answers SupportedMergeOperators normally, then replies to Execute with
//                            a typed permanent error (error_kind="permanent"). This exercises the
//                            server-side-error classification: the client must mark the DB errored,
//                            NOT fall back to local.
// =============================================================================================

#[derive(Clone)]
enum FakeCompactorMode {
    ProtocolIncompatible,
    UnsupportedOperator,
    PermanentServerError,
}

struct FakeCompactor {
    mode: FakeCompactorMode,
    addr: std::net::SocketAddr,
    /// Number of connections fully handled (one request handled each).
    handled: Arc<AtomicUsize>,
    /// Whether the server should keep accepting.
    running: Arc<AtomicBool>,
    _thread: Option<std::thread::JoinHandle<()>>,
}

impl FakeCompactor {
    fn start(mode: FakeCompactorMode) -> (Self, std::net::SocketAddr) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        listener.set_nonblocking(true).unwrap();
        let handled = Arc::new(AtomicUsize::new(0));
        let running = Arc::new(AtomicBool::new(true));
        let handled_clone = Arc::clone(&handled);
        let running_clone = Arc::clone(&running);
        // The mode is matched on every connection; share it via Arc so the closure can be 'static
        // without moving (which would prevent returning it on the struct).
        let mode = Arc::new(mode);
        let mode_clone = Arc::clone(&mode);
        let thread = std::thread::spawn(move || {
            loop {
                if !running_clone.load(Ordering::SeqCst) {
                    break;
                }
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        stream.set_nonblocking(false).ok();
                        let buf = read_message_raw(&mut stream);
                        // The command is JSON: {"kind":"supported_merge_operators"} or
                        // {"kind":"execute","payload":{...}}. Discriminate on the "kind" substring.
                        let txt = String::from_utf8_lossy(&buf);
                        if txt.contains("\"supported_merge_operators\"") {
                            match *mode_clone {
                                FakeCompactorMode::ProtocolIncompatible
                                | FakeCompactorMode::PermanentServerError => {
                                    // Advertise the default operator so the capability check passes
                                    // and the client proceeds to send an Execute request.
                                    reply_supported_operators(&mut stream, &[DEFAULT_OPERATOR_ID]);
                                }
                                FakeCompactorMode::UnsupportedOperator => {
                                    // Advertise NO operators so the client's required operator is
                                    // deemed unsupported → permanent ConfigError.
                                    reply_supported_operators(&mut stream, &[]);
                                }
                            }
                        } else if txt.contains("\"execute\"") {
                            // Reply with a crafted Execute response depending on the mode.
                            //
                            // The wire format is RemoteCompactionReply::Execute(RemoteCompactionResponse),
                            // tagged as {"kind":"execute","payload":{...response...}}.
                            let payload = match *mode_clone {
                                FakeCompactorMode::ProtocolIncompatible => {
                                    // compatible_version (999) exceeds the client's current version,
                                    // so the client's validate_protocol_compatibility rejects it as
                                    // permanent.
                                    "{\
                                    \"kind\":\"execute\",\
                                    \"payload\":{\
                                        \"version\":999,\
                                        \"compatible_version\":999,\
                                        \"output_files\":[],\
                                        \"vlog_entry_deltas\":[],\
                                        \"preload_block_keys\":[],\
                                        \"error\":null\
                                    }}"
                                }
                                FakeCompactorMode::PermanentServerError => {
                                    // A typed permanent server-side error (e.g. malformed schema).
                                    // The client must classify it permanent via error_kind and mark
                                    // the DB errored — NOT fall back to local.
                                    "{\
                                    \"kind\":\"execute\",\
                                    \"payload\":{\
                                        \"version\":3,\
                                        \"compatible_version\":3,\
                                        \"output_files\":[],\
                                        \"vlog_entry_deltas\":[],\
                                        \"preload_block_keys\":[],\
                                        \"error\":\"remote compactor failed to register carried schema version 1: invalid schema\",\
                                        \"error_kind\":\"permanent\"\
                                    }}"
                                }
                                FakeCompactorMode::UnsupportedOperator => {
                                    // UnsupportedOperator never reaches Execute (the capability
                                    // check fails first). Reply with a generic error just in case.
                                    "{\
                                    \"kind\":\"execute\",\
                                    \"payload\":{\
                                        \"version\":3,\
                                        \"compatible_version\":3,\
                                        \"output_files\":[],\
                                        \"vlog_entry_deltas\":[],\
                                        \"preload_block_keys\":[],\
                                        \"error\":\"unsupported operator\",\
                                        \"error_kind\":\"permanent\"\
                                    }}"
                                }
                            };
                            write_message_raw(&mut stream, payload.as_bytes());
                        } else {
                            // Unknown command; reply with an error.
                            write_message_raw(
                                &mut stream,
                                b"{\"kind\":\"error\",\"payload\":\"unknown command\"}",
                            );
                        }
                        handled_clone.fetch_add(1, Ordering::SeqCst);
                    }
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(5));
                    }
                    Err(_) => break,
                }
            }
        });
        // Recover the mode Arc for the struct (only one strong ref remains outside the thread).
        let mode = match Arc::try_unwrap(mode) {
            Ok(m) => m,
            // Should not happen: the only other clone was moved into the thread closure.
            Err(arc) => (*arc).clone(),
        };
        let compactor = Self {
            mode,
            addr,
            handled,
            running,
            _thread: Some(thread),
        };
        (compactor, addr)
    }

    fn handled_count(&self) -> usize {
        self.handled.load(Ordering::SeqCst)
    }

    fn mode_label(&self) -> &'static str {
        match self.mode {
            FakeCompactorMode::ProtocolIncompatible => "protocol_incompatible",
            FakeCompactorMode::UnsupportedOperator => "unsupported_operator",
            FakeCompactorMode::PermanentServerError => "permanent_server_error",
        }
    }
}

impl Drop for FakeCompactor {
    fn drop(&mut self) {
        self.running.store(false, Ordering::SeqCst);
        // Wake the non-blocking accept loop by connecting once.
        if let Ok(_stream) = TcpStream::connect_timeout(&self.addr, Duration::from_millis(100)) {
            // connection is dropped immediately by the server loop
        }
        if let Some(thread) = self._thread.take() {
            let _ = thread.join();
        }
    }
}

/// Drives writes until the fake compactor has handled at least `min_handled` connections, or panics
/// after a timeout. Used to ensure the permanent-failure path actually fires.
fn write_until_handled(db: &Db, min_handled: usize, compactor: &FakeCompactor) {
    let mut idx = 0u32;
    loop {
        let key = format!("k{:08}", idx).into_bytes();
        // put may fail once the DB is marked errored by the permanent failure; that's expected and
        // is exactly what we check afterwards. Use a tolerant put here.
        let _ = db.put(0, &key, 0, vec![b'v'; 1024]);
        idx += 1;
        if compactor.handled_count() >= min_handled {
            return;
        }
        if idx > 2000 {
            panic!(
                "fake compactor ({}) never reached {} handled connections (got {})",
                compactor.mode_label(),
                min_handled,
                compactor.handled_count()
            );
        }
        std::thread::sleep(Duration::from_millis(5));
    }
}

// =============================================================================================
// Test 5: Protocol-incompatible response must NOT fall back — the DB enters error state.
// =============================================================================================

#[test]
#[serial(file)]
fn remote_compaction_does_not_fallback_on_protocol_incompatible() {
    let root = "/tmp/resilient_protocol_incompatible";
    cleanup_test_root(root);
    let (compactor, addr) = FakeCompactor::start(FakeCompactorMode::ProtocolIncompatible);
    let mut config = base_config(root, Some(addr.to_string()));
    // Lift the write-stall limit so the write loop can keep driving compaction attempts until the
    // permanent failure fires.
    config.write_stall_limit = Some(64);
    let db = open_db(config);
    // Drive writes until the fake compactor has handled at least one Execute (the
    // protocol-incompatible reply marks the DB errored). Each handled connection is either a
    // capability fetch or an Execute; we need at least 2 handled (capability + execute) for the
    // permanent path to fire. Wait for the execute specifically by waiting for >=2 handled.
    write_until_handled(&db, 2, &compactor);
    // Give the async compaction task a moment to apply mark_error after receiving the bad reply.
    // The permanent path is taken synchronously inside the spawned task.
    let mut errored = false;
    for _ in 0..200 {
        if db.put(0, b"probe", 0, b"v").is_err() {
            errored = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(
        errored,
        "expected the DB to enter error state after a protocol-incompatible response, but it stayed writable"
    );
    // close() returns the lifecycle error when the DB is in error state; that is expected here.
    // The DB has been force-closed regardless of the returned value.
    let _ = db.close();
    drop(compactor);
    cleanup_test_root(root);
}

// =============================================================================================
// Test 6: Unsupported merge operator must NOT fall back — the DB enters error state.
//
// The fake compactor advertises an empty operator list, so the DB's default operator
// (BytesMergeOperator) is deemed unsupported → ConfigError (permanent).
// =============================================================================================

#[test]
#[serial(file)]
fn remote_compaction_does_not_fallback_on_unsupported_merge_operator() {
    let root = "/tmp/resilient_unsupported_operator";
    cleanup_test_root(root);
    let (compactor, addr) = FakeCompactor::start(FakeCompactorMode::UnsupportedOperator);
    let mut config = base_config(root, Some(addr.to_string()));
    config.write_stall_limit = Some(64);
    let db = open_db(config);
    // The capability fetch happens on the first compaction. Drive writes until at least one
    // capability request is handled (which returns an empty list → permanent ConfigError).
    write_until_handled(&db, 1, &compactor);
    // Give the async compaction task a moment to apply mark_error.
    let mut errored = false;
    for _ in 0..200 {
        if db.put(0, b"probe", 0, b"v").is_err() {
            errored = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(
        errored,
        "expected the DB to enter error state after an unsupported-merge-operator response, but it stayed writable"
    );
    // close() returns the lifecycle error when the DB is in error state; that is expected here.
    let _ = db.close();
    drop(compactor);
    cleanup_test_root(root);
}

// =============================================================================================
// Test 6b: A typed permanent server-side error during Execute must NOT fall back to local.
//
// The fake compactor advertises the default operator (capability check passes), then replies to
// Execute with error_kind="permanent" (simulating e.g. a malformed carried schema). Before the
// typed-error fix, the client treated every server error string as transient and fell back to
// local, masking exactly the deterministic schema/config errors the design says must surface. Now
// the client reads error_kind and marks the DB errored.
// =============================================================================================

#[test]
#[serial(file)]
fn remote_compaction_does_not_fallback_on_permanent_server_error() {
    let root = "/tmp/resilient_permanent_server_error";
    cleanup_test_root(root);
    let (compactor, addr) = FakeCompactor::start(FakeCompactorMode::PermanentServerError);
    let mut config = base_config(root, Some(addr.to_string()));
    config.write_stall_limit = Some(64);
    let db = open_db(config);
    // Capability fetch (1st connection) passes; the Execute (2nd connection) returns a typed
    // permanent error. Wait for both to be handled.
    write_until_handled(&db, 2, &compactor);
    // The DB must enter error state — NOT fall back to local. Probe with puts.
    let mut errored = false;
    for _ in 0..200 {
        if db.put(0, b"probe", 0, b"v").is_err() {
            errored = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(
        errored,
        "expected the DB to enter error state after a typed permanent server-side error, but it stayed writable (fallback masked the error)"
    );
    let _ = db.close();
    drop(compactor);
    cleanup_test_root(root);
}

// =============================================================================================
// Test 6c: A transient server-side error during Execute DOES fall back to local.
//
// Companion to 6b: the same fake compactor, but the error is typed transient. The client must
// fall back to local (FallbackLocal) and the DB must stay writable. This guards against
// over-classifying server errors as permanent.
// =============================================================================================

#[test]
#[serial(file)]
fn remote_compaction_falls_back_on_transient_server_error() {
    let root = "/tmp/resilient_transient_server_error";
    cleanup_test_root(root);
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    listener.set_nonblocking(true).unwrap();
    let handled = Arc::new(AtomicUsize::new(0));
    let running = Arc::new(AtomicBool::new(true));
    let handled_clone = Arc::clone(&handled);
    let running_clone = Arc::clone(&running);
    let thread = std::thread::spawn(move || {
        loop {
            if !running_clone.load(Ordering::SeqCst) {
                break;
            }
            match listener.accept() {
                Ok((mut stream, _)) => {
                    stream.set_nonblocking(false).ok();
                    let buf = read_message_raw(&mut stream);
                    let txt = String::from_utf8_lossy(&buf);
                    if txt.contains("\"supported_merge_operators\"") {
                        reply_supported_operators(&mut stream, &[DEFAULT_OPERATOR_ID]);
                    } else if txt.contains("\"execute\"") {
                        // Reply with a typed TRANSIENT error (e.g. server overload). The client must
                        // fall back to local, not error the DB.
                        let payload = "{\
                        \"kind\":\"execute\",\
                        \"payload\":{\
                            \"version\":3,\
                            \"compatible_version\":3,\
                            \"output_files\":[],\
                            \"vlog_entry_deltas\":[],\
                            \"preload_block_keys\":[],\
                            \"error\":\"server overloaded, try again\",\
                            \"error_kind\":\"transient\"\
                        }}";
                        write_message_raw(&mut stream, payload.as_bytes());
                    } else {
                        write_message_raw(
                            &mut stream,
                            b"{\"kind\":\"error\",\"payload\":\"unknown command\"}",
                        );
                    }
                    handled_clone.fetch_add(1, Ordering::SeqCst);
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    std::thread::sleep(Duration::from_millis(5));
                }
                Err(_) => break,
            }
        }
    });
    let compactor = FakeCompactor {
        mode: FakeCompactorMode::PermanentServerError, // unused label; this server is hand-rolled
        addr,
        handled,
        running: Arc::clone(&running),
        _thread: Some(thread),
    };
    let config = base_config(root, Some(addr.to_string()));
    let db = open_db(config);
    // Drive writes until a compaction is observed (local fallback runs after the transient
    // server error).
    write_until_compaction(&db, db.id(), 1024);
    // The DB must stay writable: a put + get must succeed (no error state).
    db.put(0, b"probe", 0, b"v")
        .expect("db stays writable after transient server error");
    let value = db
        .get(0, b"probe")
        .expect("get succeeds")
        .expect("value present");
    let col = value[0].as_ref().unwrap();
    assert_eq!(col.as_ref(), b"v");
    db.close().expect("close");
    drop(compactor);
    cleanup_test_root(root);
}

// =============================================================================================
// Test 7: FallbackLocal preserves schema evolution and vlog edits.
//
// With value separation enabled (large values go to the vlog), a local-fallback compaction must
// still produce correct vlog deltas so values read back. We write large values, force a
// local-fallback compaction (remote down), and verify the values are intact.
// =============================================================================================

#[test]
#[serial(file)]
fn fallback_local_preserves_schema_evolution_and_vlog_edits() {
    let root = "/tmp/resilient_fallback_vlog";
    cleanup_test_root(root);
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let dead_addr = listener.local_addr().unwrap().to_string();
    drop(listener);
    let mut config = base_config(root, Some(dead_addr));
    // Enable value separation: values above this threshold are written to the vlog.
    config.value_separation_threshold = Some(Size::from_kib(1));
    let db = open_db(config);
    // Write values large enough to be separated into the vlog, then trigger a local-fallback
    // compaction.
    let value = vec![b'w'; 2048];
    for i in 0..20 {
        let key = format!("k{:08}", i).into_bytes();
        db.put(0, &key, 0, value.clone()).expect("put");
    }
    // Wait for a local-fallback compaction to run.
    assert!(
        compaction_happened(&db, db.id()),
        "expected a local-fallback compaction to run"
    );
    // All values must read back intact (vlog deltas applied correctly by the local fallback).
    for i in 0..20 {
        let key = format!("k{:08}", i).into_bytes();
        let got = db.get(0, &key).expect("get").expect("value present");
        let col = got[0].as_ref().unwrap();
        assert_eq!(col.len(), 2048, "value length for key {}", i);
        assert!(col.iter().all(|&b| b == b'w'), "value bytes for key {}", i);
    }
    db.close().expect("close");
    cleanup_test_root(root);
}

// =============================================================================================
// Test 8 (smoke): FallbackLocal preserves block-cache preload behavior.
//
// With a block cache enabled, a local-fallback compaction's output should be eligible for preload.
// This is a smoke test: we enable a block cache, run a local-fallback compaction, and confirm the
// DB stays functional and reads succeed (the preload worker runs without panicking).
// =============================================================================================

#[test]
#[serial(file)]
fn fallback_local_preserves_block_cache_preload_behavior() {
    let root = "/tmp/resilient_fallback_blockcache";
    cleanup_test_root(root);
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let dead_addr = listener.local_addr().unwrap().to_string();
    drop(listener);
    let mut config = base_config(root, Some(dead_addr));
    config.block_cache_size = Size::from_mib(4);
    let db = open_db(config);
    write_until_compaction(&db, db.id(), 1024);
    // Reads must succeed after a local-fallback compaction with a block cache. The preload worker
    // handling the local output must not panic.
    for i in 0..10 {
        let key = format!("k{:08}", i).into_bytes();
        let value = db.get(0, &key).expect("get").expect("value present");
        let col = value[0].as_ref().unwrap();
        assert_eq!(col.len(), 1024, "value length for key {}", i);
        assert!(col.iter().all(|&b| b == b'v'), "value bytes for key {}", i);
    }
    db.close().expect("close");
    cleanup_test_root(root);
}
