use super::*;

#[test]
fn test_next_prefix() {
    assert_eq!(next_prefix(b"abc"), Some(b"abd".to_vec()));
    assert_eq!(next_prefix(&[0x10, 0xFF]), Some(vec![0x11]));
    assert_eq!(next_prefix(&[0xFF, 0xFF]), None);
}

#[test]
fn test_parse_scan_query() {
    let params = InspectParams {
        mode: Some("scan".to_string()),
        bucket: Some(7),
        keys: None,
        keys_b64: None,
        lookup_items: None,
        prefix: Some("user:".to_string()),
        prefix_b64: None,
        start_after: None,
        start_after_b64: None,
        limit: Some(200),
    };
    let query = parse_inspect_query(&params, 100, 500).expect("parse query");
    assert_eq!(query.mode, InspectMode::Scan);
    assert_eq!(query.bucket, 7);
    assert_eq!(query.prefix, Some(b"user:".to_vec()));
    assert_eq!(query.limit, 200);
}

#[test]
fn test_parse_lookup_query_base64_keys() {
    let params = InspectParams {
        mode: Some("lookup".to_string()),
        bucket: Some(1),
        keys: None,
        keys_b64: Some(format!(
            "{},{}",
            STANDARD.encode(b"k1"),
            STANDARD.encode(b"k2")
        )),
        lookup_items: None,
        prefix: None,
        prefix_b64: None,
        start_after: None,
        start_after_b64: None,
        limit: None,
    };
    let query = parse_inspect_query(&params, 50, 100).expect("parse query");
    assert_eq!(query.mode, InspectMode::Lookup);
    assert_eq!(query.lookup_items.len(), 2);
    assert_eq!(query.lookup_items[0].bucket, 1);
    assert_eq!(query.lookup_items[0].key, b"k1".to_vec());
    assert_eq!(query.lookup_items[1].key, b"k2".to_vec());
    assert_eq!(query.limit, 50);
}

#[test]
fn test_parse_scan_query_reject_empty_prefix() {
    let params = InspectParams {
        mode: Some("scan".to_string()),
        bucket: Some(0),
        keys: None,
        keys_b64: None,
        lookup_items: None,
        prefix: Some(String::new()),
        prefix_b64: None,
        start_after: None,
        start_after_b64: None,
        limit: None,
    };
    let query = parse_inspect_query(&params, 10, 20).expect("empty prefix should be accepted");
    assert_eq!(query.prefix, Some(Vec::new()));
}

#[test]
fn test_parse_lookup_items_json() {
    let params = InspectParams {
        mode: Some("lookup".to_string()),
        bucket: Some(3),
        keys: None,
        keys_b64: None,
        lookup_items: Some(format!(
            "[{{\"bucket\":2,\"key_b64\":\"{}\"}},{{\"key_b64\":\"{}\"}}]",
            STANDARD.encode(b"k-a"),
            STANDARD.encode(b"k-b")
        )),
        prefix: None,
        prefix_b64: None,
        start_after: None,
        start_after_b64: None,
        limit: None,
    };
    let query = parse_inspect_query(&params, 10, 20).expect("lookup_items parse");
    assert_eq!(query.lookup_items.len(), 2);
    assert_eq!(query.lookup_items[0].bucket, 2);
    assert_eq!(query.lookup_items[0].key, b"k-a".to_vec());
    assert_eq!(query.lookup_items[1].bucket, 3);
    assert_eq!(query.lookup_items[1].key, b"k-b".to_vec());
}

#[test]
fn test_parse_lookup_without_bucket_uses_default_zero() {
    let params = InspectParams {
        mode: Some("lookup".to_string()),
        bucket: None,
        keys: Some("k0".to_string()),
        keys_b64: None,
        lookup_items: None,
        prefix: None,
        prefix_b64: None,
        start_after: None,
        start_after_b64: None,
        limit: None,
    };
    let query = parse_inspect_query(&params, 10, 20).expect("lookup without bucket");
    assert_eq!(query.bucket, 0);
    assert_eq!(query.lookup_items.len(), 1);
    assert_eq!(query.lookup_items[0].bucket, 0);
    assert_eq!(query.lookup_items[0].key, b"k0".to_vec());
}

#[test]
fn test_resolve_ui_dist_path_rejects_parent_escape() {
    assert!(normalize_ui_relative_path("../index.html").is_none());
    assert!(normalize_ui_relative_path("assets/../../secret").is_none());
}

#[test]
fn test_mime_for_path() {
    assert_eq!(
        mime_for_path("a.js"),
        "application/javascript; charset=utf-8"
    );
    assert_eq!(mime_for_path("a.css"), "text/css; charset=utf-8");
    assert_eq!(mime_for_path("a.bin"), "application/octet-stream");
}
