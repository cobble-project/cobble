use super::*;
use crate::config::{CompactionMode, VolumeDescriptor};

#[test]
fn discovery_accepts_parent_and_direct_paths_without_duplicates() {
    let dir = tempfile::tempdir().unwrap();
    let db_a = dir.path().join("db-a");
    let db_b = dir.path().join("db-b");
    std::fs::create_dir_all(db_a.join("runtime")).unwrap();
    std::fs::create_dir_all(db_b.join("snapshot")).unwrap();
    std::fs::write(db_a.join("runtime/CURRENT"), b"1\n").unwrap();
    std::fs::write(db_b.join("snapshot/SNAPSHOT-1"), b"manifest").unwrap();

    let mut config = Config {
        compaction_mode: CompactionMode::Dedicated,
        ..Config::default()
    };
    config.volumes = VolumeDescriptor::single_volume(dir.path().to_string_lossy().into_owned());
    let shards = discover_shards(&config, &[dir.path().to_path_buf(), db_a.clone()]);
    assert_eq!(shards.len(), 2);
    assert_eq!(
        shards
            .iter()
            .map(|shard| shard.db_id.as_str())
            .collect::<Vec<_>>(),
        vec!["db-a", "db-b"]
    );
    assert_eq!(
        shards[0].config.volumes[0].base_dir,
        dir.path().to_string_lossy()
    );
}

#[test]
fn direct_single_volume_path_rebinds_volume_parent() {
    let dir = tempfile::tempdir().unwrap();
    let db_dir = dir.path().join("shard-7");
    std::fs::create_dir_all(&db_dir).unwrap();
    let config = Config {
        volumes: VolumeDescriptor::single_volume("/different/root"),
        ..Config::default()
    };
    let (resolved, db_id) = config_for_db_directory(&config, &db_dir).unwrap();
    assert_eq!(db_id, "shard-7");
    assert_eq!(resolved.volumes[0].base_dir, dir.path().to_string_lossy());
}

#[test]
fn retry_backoff_is_bounded() {
    assert_eq!(
        retry_backoff(Duration::from_millis(10), 1),
        Duration::from_millis(10)
    );
    assert_eq!(
        retry_backoff(Duration::from_secs(10), 20),
        MAX_RETRY_BACKOFF
    );
}
