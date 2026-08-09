use super::normalize_storage_path_to_url;

#[test]
fn test_normalize_storage_path_to_url_keeps_remote_url() {
    let input = "s3://bucket-name/prefix";
    let normalized = normalize_storage_path_to_url(input).unwrap();
    assert_eq!(normalized, input);
}

#[test]
fn test_normalize_storage_path_to_url_file_url_roundtrip() {
    let input = "file:///tmp/cobble";
    let normalized = normalize_storage_path_to_url(input).unwrap();
    assert!(normalized.starts_with("file:///tmp/cobble"));
}

#[test]
fn test_normalize_storage_path_to_url_local_path_to_file_url() {
    let path = std::env::temp_dir().join("cobble-local-path");
    let path_str = path.to_string_lossy();
    let normalized = normalize_storage_path_to_url(&path_str).unwrap();
    assert!(normalized.starts_with("file://"));
}

#[test]
fn test_normalize_storage_path_to_url_relative_path_becomes_absolute_file_url() {
    let normalized = normalize_storage_path_to_url("./cobble-relative-path").unwrap();
    assert!(normalized.starts_with("file://"));
    let parsed = url::Url::parse(&normalized).unwrap();
    let local = parsed.to_file_path().unwrap();
    assert!(local.is_absolute());
}
