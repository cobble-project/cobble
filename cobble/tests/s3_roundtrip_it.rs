#![cfg(feature = "storage-s3")]

use cobble::{Config, SingleDb, VolumeDescriptor, VolumeUsageKind};
use size::Size;
use std::sync::mpsc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use url::Url;

const PROGRESS_LOG_INTERVAL: usize = 16 * 1024;

fn must_env(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("missing required env var: {}", name))
}

fn has_required_env() -> bool {
    [
        "COBBLE_S3_ENDPOINT",
        "COBBLE_S3_BUCKET",
        "COBBLE_S3_ACCESS_ID",
        "COBBLE_S3_SECRET_KEY",
    ]
    .iter()
    .all(|name| std::env::var(name).is_ok())
}

fn make_value(idx: usize, value_len: usize) -> Vec<u8> {
    let mut value = vec![0u8; value_len];
    for (pos, byte) in value.iter_mut().enumerate() {
        *byte = ((idx.wrapping_mul(31) + pos.wrapping_mul(17)) % 251) as u8;
    }
    value
}

fn log_stage(stage: &str, stage_start: Instant, total_start: Instant) {
    eprintln!(
        "[s3-roundtrip] stage={stage} stage_elapsed={:.3}s total_elapsed={:.3}s",
        stage_start.elapsed().as_secs_f64(),
        total_start.elapsed().as_secs_f64()
    );
}

fn log_progress(
    stage: &str,
    completed: usize,
    total: usize,
    stage_start: Instant,
    total_start: Instant,
) {
    if !completed.is_multiple_of(PROGRESS_LOG_INTERVAL) && completed != total {
        return;
    }
    eprintln!(
        "[s3-roundtrip] stage={stage} progress={completed}/{total} ({:.1}%) stage_elapsed={:.3}s total_elapsed={:.3}s",
        (completed as f64 / total as f64) * 100.0,
        stage_start.elapsed().as_secs_f64(),
        total_start.elapsed().as_secs_f64()
    );
}

#[test]
fn s3_roundtrip_write_snapshot_resume() {
    if !has_required_env() {
        eprintln!("skipping s3 roundtrip test: COBBLE_S3_* env vars are not fully set");
        return;
    }
    let total_start = Instant::now();
    let endpoint = must_env("COBBLE_S3_ENDPOINT");
    let bucket = must_env("COBBLE_S3_BUCKET");
    let access_id = must_env("COBBLE_S3_ACCESS_ID");
    let secret_key = must_env("COBBLE_S3_SECRET_KEY");
    let endpoint_url = Url::parse(&endpoint).expect("COBBLE_S3_ENDPOINT must be a valid URL");
    let endpoint_host = endpoint_url
        .host_str()
        .expect("COBBLE_S3_ENDPOINT must include host");
    let endpoint_port = endpoint_url
        .port_or_known_default()
        .expect("COBBLE_S3_ENDPOINT must include explicit or default port");
    let endpoint_scheme = endpoint_url.scheme();

    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock should be monotonic")
        .as_millis();
    let root_prefix = format!("cobble-s3-roundtrip-{}", unique);
    let base_dir = format!(
        "s3://{}:{}/{}/{}?endpoint_scheme={}&region=us-east-1&disable_config_load=true&disable_ec2_metadata=true&enable_virtual_host_style=false",
        endpoint_host, endpoint_port, bucket, root_prefix, endpoint_scheme
    );

    let mut volume = VolumeDescriptor::new(
        base_dir,
        vec![
            VolumeUsageKind::Meta,
            VolumeUsageKind::PrimaryDataPriorityHigh,
            VolumeUsageKind::Snapshot,
        ],
    );
    volume.access_id = Some(access_id);
    volume.secret_key = Some(secret_key);

    let config = Config {
        volumes: vec![volume],
        memtable_capacity: Size::from_mib(4),
        memtable_buffer_count: 2,
        num_columns: 1,
        total_buckets: 1,
        l0_file_limit: 4,
        l1_base_bytes: Size::from_mib(16),
        level_size_multiplier: 4,
        max_level: 6,
        base_file_size: Size::from_mib(4),
        block_cache_size: Size::from_mib(256),
        log_console: true,
        ..Config::default()
    };

    // 1 GiB total payload: 131_072 keys x 4 KiB values.
    let value_len = 4096usize;
    let target_bytes = 512 * 1024 * 1024usize;
    let records = target_bytes / value_len;
    eprintln!(
        "[s3-roundtrip] starting endpoint={} bucket={} root_prefix={} records={} value_len={} target_bytes={} block_cache_size={}",
        endpoint,
        bucket,
        root_prefix,
        records,
        value_len,
        target_bytes,
        config.block_cache_size.bytes()
    );

    let open_start = Instant::now();
    let db = SingleDb::open(config.clone()).expect("open single db on s3 should succeed");
    log_stage("open", open_start, total_start);

    let write_start = Instant::now();
    for idx in 0..records {
        let key = format!("bulk-key-{idx:08}");
        let value = make_value(idx, value_len);
        db.put(0, key.as_bytes(), 0, value)
            .expect("write should succeed");
        log_progress("write", idx + 1, records, write_start, total_start);
    }
    log_stage("write", write_start, total_start);

    let verify_before_snapshot_start = Instant::now();
    for idx in 0..records {
        let key = format!("bulk-key-{idx:08}");
        let expected = make_value(idx, value_len);
        let got = db
            .get(0, key.as_bytes())
            .expect("read should succeed")
            .expect("value should exist");
        let col = got[0].as_ref().expect("column 0 should exist");
        assert_eq!(col.as_ref(), expected.as_slice(), "mismatch at idx={idx}");
        log_progress(
            "verify-before-snapshot",
            idx + 1,
            records,
            verify_before_snapshot_start,
            total_start,
        );
    }
    log_stage(
        "verify-before-snapshot",
        verify_before_snapshot_start,
        total_start,
    );

    let (tx, rx) = mpsc::channel();
    let snapshot_start = Instant::now();
    let snapshot_id = db
        .snapshot_with_callback(move |result| {
            let _ = tx.send(result.map(|_| ()));
        })
        .expect("snapshot should be scheduled");
    let callback_result = rx
        .recv_timeout(Duration::from_secs(180))
        .expect("snapshot callback should complete");
    callback_result.expect("snapshot materialization should succeed");
    log_stage("snapshot", snapshot_start, total_start);

    let close_before_resume_start = Instant::now();
    db.close().expect("close db should succeed");
    log_stage(
        "close-before-resume",
        close_before_resume_start,
        total_start,
    );

    let resume_start = Instant::now();
    let resumed = SingleDb::resume(config, snapshot_id).expect("resume should succeed");
    log_stage("resume", resume_start, total_start);

    let verify_after_resume_start = Instant::now();
    for idx in 0..records {
        let key = format!("bulk-key-{idx:08}");
        let expected = make_value(idx, value_len);
        let got = resumed
            .get(0, key.as_bytes())
            .expect("read after resume should succeed")
            .expect("value should exist after resume");
        let col = got[0].as_ref().expect("column 0 should exist after resume");
        assert_eq!(
            col.as_ref(),
            expected.as_slice(),
            "mismatch after resume at idx={idx}"
        );
        log_progress(
            "verify-after-resume",
            idx + 1,
            records,
            verify_after_resume_start,
            total_start,
        );
    }
    log_stage(
        "verify-after-resume",
        verify_after_resume_start,
        total_start,
    );

    let close_after_resume_start = Instant::now();
    resumed.close().expect("close resumed db should succeed");
    log_stage("close-after-resume", close_after_resume_start, total_start);
    log_stage("complete", total_start, total_start);
}
