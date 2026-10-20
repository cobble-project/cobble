use super::{
    append_storage_path, map_url_path, match_storage_path, normalize_storage_path_to_url,
    storage_location_url, url_path,
};

#[test]
fn storage_locations_are_absolute_and_option_free() {
    let url = storage_location_url(
        "s3://access-id:secret@bucket/state/db-1?endpoint=http%3A%2F%2Flocalhost%3A9000#ignored",
    )
    .unwrap();
    assert_eq!(url.as_str(), "s3://bucket/state/db-1");
    assert!(url.username().is_empty());
    assert!(url.password().is_none());
    assert!(url.query().is_none());
    assert!(url.fragment().is_none());
    assert!(
        storage_location_url("relative/cache")
            .unwrap()
            .as_str()
            .starts_with("file:///")
    );
}

#[test]
fn storage_paths_append_before_options_and_preserve_manifest_routes() {
    assert_eq!(url_path("s3://host/bucket/base").unwrap(), "/bucket/base");
    assert_eq!(
        map_url_path("s3://host/bucket/base", |path| Ok(format!("{path}/file"))).unwrap(),
        "s3://host/bucket/base/file"
    );
    let base = "s3://host/bucket/base?endpoint=http://localhost:9000&root=/physical/base";
    let path = append_storage_path(base, "shard/snapshot/SNAPSHOT-7");
    assert_eq!(
        path,
        "s3://host/bucket/base/shard/snapshot/SNAPSHOT-7?endpoint=http://localhost:9000&root=/physical/base"
    );
    assert_eq!(
        crate::paths::sibling_snapshot_manifest_path(&path, 6).unwrap(),
        append_storage_path(base, "shard/snapshot/SNAPSHOT-6")
    );
    assert_eq!(
        crate::paths::schema_file_path_from_snapshot_manifest_path(&path, 3).unwrap(),
        append_storage_path(base, "shard/schema/schema-3")
    );
    assert_eq!(crate::paths::snapshot_source_db_id(&path).unwrap(), "shard");
    assert_eq!(
        append_storage_path("file:///", "tmp/file"),
        "file:///tmp/file"
    );
    assert_eq!(
        append_storage_path("s3://host/bucket/base/", "shard/data/1.cb"),
        "s3://host/bucket/base/shard/data/1.cb"
    );
}

#[test]
fn storage_paths_match_longest_directory_and_options_without_guessing() {
    let roots = [
        "s3://host/bucket/base?endpoint=http://localhost:9000&root=/a",
        "s3://host/bucket/base?root=/b&endpoint=http://localhost:9000",
        "s3://host/bucket/base/nested?root=/nested",
        "s3://host/bucket/base2?root=/c",
    ];
    let resolve = |path| match_storage_path(path, roots.iter().copied().enumerate());
    assert_eq!(
        resolve(
            "s3://host/bucket/base/shard/data/1.cb?endpoint=http%3A%2F%2Flocalhost%3A9000&root=%2Fb"
        )
        .unwrap(),
        Some((1, "shard/data/1.cb".into()))
    );
    assert!(resolve("s3://host/bucket/base/shard/data/1.cb").is_err());
    assert_eq!(
        resolve("s3://host/bucket/base/nested/shard/data/1.cb").unwrap(),
        Some((2, "shard/data/1.cb".into()))
    );
    assert_eq!(
        resolve("s3://host/bucket/base2/shard/data/1.cb").unwrap(),
        Some((3, "shard/data/1.cb".into()))
    );
    assert!(
        resolve("s3://host/bucket/base23/shard/data/1.cb")
            .unwrap()
            .is_none()
    );
    assert!(
        resolve("s3://host/bucket/base/shard/data/1.cb?root=/unknown")
            .unwrap()
            .is_none()
    );
    let plain = ["s3://host/bucket/base?root=/a", "s3://host/bucket/base"];
    assert_eq!(
        match_storage_path("s3://host/bucket/base/file", plain.into_iter().enumerate()).unwrap(),
        Some((1, "file".into()))
    );
    assert!(
        match_storage_path("file://server/share/file", [(0, "file:///")])
            .unwrap()
            .is_none()
    );
    assert_eq!(
        match_storage_path("file:///tmp/a%20file", [(0, "file:///")]).unwrap(),
        Some((0, "tmp/a file".into()))
    );
    assert_eq!(
        match_storage_path("file:///C:/data/a%20file", [(0, "file:///C:/data")]).unwrap(),
        Some((0, "a file".into()))
    );
}

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
