//! Per-DB writer properties shared with external compaction processes.

use crate::Config;
use crate::error::{Error, Result};
use crate::file::{FileManager, ReadAllFile};
use crate::util::normalize_storage_path_to_url;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use url::Url;

pub(crate) const DB_PROPERTIES_NAME: &str = "PROPERTIES";
const DB_PROPERTIES_VERSION_CURRENT: u32 = 1;
const MAX_DB_PROPERTIES_BYTES: usize = 4 * 1024 * 1024;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct DbProperties {
    version: u32,
    db_id: String,
    config: Config,
}

impl DbProperties {
    fn from_writer(db_id: &str, config: &Config) -> Self {
        let mut sanitized = config.clone();
        for volume in &mut sanitized.volumes {
            sanitize_volume(volume);
        }
        Self {
            version: DB_PROPERTIES_VERSION_CURRENT,
            db_id: db_id.to_string(),
            config: sanitized,
        }
    }

    fn validate(&self, expected_db_id: &str) -> Result<()> {
        if self.version != DB_PROPERTIES_VERSION_CURRENT {
            return Err(Error::InvalidState(format!(
                "Unsupported DB PROPERTIES version: {} (expected {})",
                self.version, DB_PROPERTIES_VERSION_CURRENT
            )));
        }
        if self.db_id != expected_db_id {
            return Err(Error::InvalidState(format!(
                "DB PROPERTIES id mismatch: expected {}, found {}",
                expected_db_id, self.db_id
            )));
        }
        if self.config.volumes.is_empty() {
            return Err(Error::InvalidState(
                "DB PROPERTIES contains no volumes".to_string(),
            ));
        }
        Ok(())
    }
}

pub(crate) fn persist_db_properties(
    file_manager: &FileManager,
    db_id: &str,
    config: &Config,
) -> Result<()> {
    let properties = DbProperties::from_writer(db_id, config);
    let toml = serialize_db_properties(&properties)?;
    file_manager.write_plain_metadata_file_atomic(DB_PROPERTIES_NAME, toml.as_bytes())
}

/// Publishes the writer's current sanitized configuration at startup.
///
/// An existing semantically equivalent TOML file is left untouched. Missing, malformed, stale,
/// or differently configured properties are atomically replaced.
pub(crate) fn refresh_db_properties(
    file_manager: &FileManager,
    db_id: &str,
    config: &Config,
) -> Result<bool> {
    let properties = DbProperties::from_writer(db_id, config);
    let desired_toml = serialize_db_properties(&properties)?;
    let desired_value: toml::Value = toml::from_str(&desired_toml).map_err(|err| {
        Error::ConfigError(format!("Failed to compare serialized DB PROPERTIES: {err}"))
    })?;

    if file_manager.metadata_file_exists_untracked(DB_PROPERTIES_NAME)? {
        let reader = file_manager.open_metadata_file_reader_untracked(DB_PROPERTIES_NAME)?;
        if reader.size() <= MAX_DB_PROPERTIES_BYTES {
            let bytes = reader.read_all()?;
            if let Ok(contents) = std::str::from_utf8(bytes.as_ref())
                && let Ok(existing_value) = toml::from_str::<toml::Value>(contents)
                && existing_value == desired_value
            {
                return Ok(false);
            }
        }
    }

    file_manager.write_plain_metadata_file_atomic(DB_PROPERTIES_NAME, desired_toml.as_bytes())?;
    Ok(true)
}

fn serialize_db_properties(properties: &DbProperties) -> Result<String> {
    toml::to_string_pretty(properties)
        .map_err(|err| Error::ConfigError(format!("Failed to serialize DB PROPERTIES: {err}")))
}

pub(crate) fn load_db_properties(
    file_manager: &FileManager,
    expected_db_id: &str,
) -> Result<DbProperties> {
    let reader = file_manager
        .open_metadata_file_reader_untracked(DB_PROPERTIES_NAME)
        .map_err(|err| {
            Error::IoError(format!(
                "Failed to open DB PROPERTIES for {expected_db_id}: {err}"
            ))
        })?;
    if reader.size() > MAX_DB_PROPERTIES_BYTES {
        return Err(Error::InvalidState(format!(
            "DB PROPERTIES for {expected_db_id} exceeds {} bytes",
            MAX_DB_PROPERTIES_BYTES
        )));
    }
    let bytes = reader.read_all()?;
    let contents = std::str::from_utf8(bytes.as_ref()).map_err(|err| {
        Error::InvalidState(format!(
            "DB PROPERTIES for {expected_db_id} is not UTF-8: {err}"
        ))
    })?;
    let properties: DbProperties = toml::from_str(contents).map_err(|err| {
        Error::InvalidState(format!(
            "Failed to parse DB PROPERTIES for {expected_db_id}: {err}"
        ))
    })?;
    properties.validate(expected_db_id)?;
    Ok(properties)
}

/// Returns the writer configuration recorded in PROPERTIES with credentials restored only from
/// the compactor process configuration. Persisted volume order, kinds, limits, and paths remain
/// authoritative for output-volume selection.
pub(crate) fn load_compactor_config(
    file_manager: &FileManager,
    db_id: &str,
    process_config: &Config,
) -> Result<Config> {
    let mut resolved = load_db_properties(file_manager, db_id)?.config;
    for persisted in &mut resolved.volumes {
        let Some(process_volume) = process_config
            .volumes
            .iter()
            .find(|candidate| volume_identity(candidate) == volume_identity(persisted))
        else {
            continue;
        };
        // Use the in-process URL so credentials embedded in URL userinfo/query remain available
        // to the filesystem implementation without ever appearing in PROPERTIES.
        persisted.base_dir = process_volume.base_dir.clone();
        persisted.access_id = process_volume.access_id.clone();
        persisted.secret_key = process_volume.secret_key.clone();
        restore_sensitive_options(
            &mut persisted.custom_options,
            process_volume.custom_options.as_ref(),
        );
    }
    // These settings control the compactor process itself rather than the DB's persisted layout.
    // Keep CLI/service overrides effective while the writer remains authoritative for storage
    // volumes and compaction semantics.
    resolved.compaction_dedicated_poll_interval_ms =
        process_config.compaction_dedicated_poll_interval_ms;
    resolved.log_path = process_config.log_path.clone();
    resolved.log_max_file_size = process_config.log_max_file_size;
    resolved.log_keep_files = process_config.log_keep_files;
    resolved.log_console = process_config.log_console;
    resolved.log_level = process_config.log_level;
    Ok(resolved)
}

fn volume_identity(volume: &crate::VolumeDescriptor) -> String {
    let mut sanitized = volume.clone();
    sanitize_volume(&mut sanitized);
    normalize_storage_path_to_url(&sanitized.base_dir)
        .unwrap_or_else(|_| sanitized.base_dir.clone())
}

fn sanitize_volume(volume: &mut crate::VolumeDescriptor) {
    volume.access_id = None;
    volume.secret_key = None;
    if let Some(options) = &mut volume.custom_options {
        options.retain(|key, _| !is_sensitive_volume_option(key));
        if options.is_empty() {
            volume.custom_options = None;
        }
    }
    let Ok(mut url) = Url::parse(&volume.base_dir) else {
        return;
    };
    let _ = url.set_username("");
    let _ = url.set_password(None);
    if url.query().is_some() {
        let retained: Vec<(String, String)> = url
            .query_pairs()
            .filter(|(key, _)| !is_sensitive_volume_option(key))
            .map(|(key, value)| (key.into_owned(), value.into_owned()))
            .collect();
        url.set_query(None);
        if !retained.is_empty() {
            url.query_pairs_mut().extend_pairs(retained);
        }
    }
    volume.base_dir = url.to_string();
}

fn restore_sensitive_options(
    persisted: &mut Option<HashMap<String, String>>,
    process: Option<&HashMap<String, String>>,
) {
    let Some(process) = process else {
        return;
    };
    let target = persisted.get_or_insert_with(HashMap::new);
    for (key, value) in process {
        if is_sensitive_volume_option(key) {
            target.insert(key.clone(), value.clone());
        }
    }
}

fn is_sensitive_volume_option(key: &str) -> bool {
    matches!(
        key.trim().to_ascii_lowercase().replace('-', "_").as_str(),
        "access_id"
            | "access_key"
            | "access_key_id"
            | "aws_access_key_id"
            | "secret_key"
            | "secret_access_key"
            | "aws_secret_access_key"
            | "session_token"
            | "aws_session_token"
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{VolumeDescriptor, VolumeUsageKind};
    use crate::file::FileManager;
    use crate::metrics_manager::MetricsManager;
    use size::Size;
    use std::sync::Arc;

    #[test]
    fn properties_are_plain_toml_without_volume_credentials() {
        let dir = tempfile::tempdir().unwrap();
        let root = format!("file://{}", dir.path().display());
        let mut volume = VolumeDescriptor::new(
            "s3://writer:embedded-secret@example-bucket/data?access_key_id=query-ak",
            vec![
                VolumeUsageKind::Meta,
                VolumeUsageKind::PrimaryDataPriorityMedium,
            ],
        );
        volume.access_id = Some("field-ak".to_string());
        volume.secret_key = Some("field-sk".to_string());
        volume.size_limit = Some(Size::from_mib(16));
        volume.custom_options = Some(HashMap::from([
            ("region".to_string(), "test-region".to_string()),
            ("secret_access_key".to_string(), "option-sk".to_string()),
        ]));
        let config = Config {
            volumes: vec![volume],
            l0_file_limit: 17,
            ..Config::default()
        };
        let storage_config = Config {
            volumes: VolumeDescriptor::single_volume(root),
            ..Config::default()
        };
        let metrics = Arc::new(MetricsManager::new("properties-test"));
        let file_manager = FileManager::from_config(&storage_config, "shard-7", metrics).unwrap();

        persist_db_properties(&file_manager, "shard-7", &config).unwrap();
        let path = dir.path().join("shard-7").join(DB_PROPERTIES_NAME);
        let contents = std::fs::read_to_string(path).unwrap();
        let decoded: DbProperties = toml::from_str(&contents).unwrap();

        assert_eq!(decoded.version, DB_PROPERTIES_VERSION_CURRENT);
        assert_eq!(decoded.db_id, "shard-7");
        assert_eq!(decoded.config.l0_file_limit, 17);
        assert_eq!(
            decoded.config.volumes[0].size_limit,
            Some(Size::from_mib(16))
        );
        assert!(decoded.config.volumes[0].supports(VolumeUsageKind::PrimaryDataPriorityMedium));
        assert_eq!(
            decoded.config.volumes[0]
                .custom_options
                .as_ref()
                .unwrap()
                .get("region")
                .map(String::as_str),
            Some("test-region")
        );
        for secret in [
            "writer",
            "embedded-secret",
            "query-ak",
            "field-ak",
            "field-sk",
            "option-sk",
        ] {
            assert!(!contents.contains(secret), "PROPERTIES leaked {secret}");
        }
    }

    #[test]
    fn compactor_uses_persisted_volume_order_and_process_credentials() {
        let dir = tempfile::tempdir().unwrap();
        let metadata_root = format!("file://{}", dir.path().display());
        let property_config = Config {
            volumes: vec![
                VolumeDescriptor::new(
                    "s3://bucket/slow",
                    vec![VolumeUsageKind::PrimaryDataPriorityLow],
                ),
                VolumeDescriptor::new(
                    "s3://bucket/fast",
                    vec![VolumeUsageKind::PrimaryDataPriorityHigh],
                ),
            ],
            ..Config::default()
        };
        let storage_config = Config {
            volumes: VolumeDescriptor::single_volume(metadata_root),
            ..Config::default()
        };
        let metrics = Arc::new(MetricsManager::new("properties-resolve-test"));
        let file_manager = FileManager::from_config(&storage_config, "shard-9", metrics).unwrap();
        persist_db_properties(&file_manager, "shard-9", &property_config).unwrap();

        let mut fast_runtime = VolumeDescriptor::new(
            "s3://bucket/fast",
            vec![VolumeUsageKind::PrimaryDataPriorityLow],
        );
        fast_runtime.access_id = Some("runtime-ak".to_string());
        fast_runtime.secret_key = Some("runtime-sk".to_string());
        let process_config = Config {
            volumes: vec![fast_runtime],
            ..Config::default()
        };
        let resolved = load_compactor_config(&file_manager, "shard-9", &process_config).unwrap();

        assert_eq!(resolved.volumes.len(), 2);
        assert_eq!(resolved.volumes[0].base_dir, "s3://bucket/slow");
        assert_eq!(resolved.volumes[1].base_dir, "s3://bucket/fast");
        assert_eq!(resolved.volumes[1].access_id.as_deref(), Some("runtime-ak"));
        assert_eq!(
            resolved.volumes[1].secret_key.as_deref(),
            Some("runtime-sk")
        );
        assert!(resolved.volumes[1].supports(VolumeUsageKind::PrimaryDataPriorityHigh));
    }

    #[test]
    fn startup_refreshes_properties_only_when_sanitized_config_changes() {
        let dir = tempfile::tempdir().unwrap();
        let metadata_root = format!("file://{}", dir.path().display());
        let storage_config = Config {
            volumes: VolumeDescriptor::single_volume(metadata_root),
            ..Config::default()
        };
        let metrics = Arc::new(MetricsManager::new("properties-refresh-test"));
        let file_manager = FileManager::from_config(&storage_config, "shard-11", metrics).unwrap();
        let mut writer_config = Config {
            volumes: VolumeDescriptor::single_volume("s3://bucket/data"),
            l0_file_limit: 7,
            ..Config::default()
        };
        writer_config.volumes[0].access_id = Some("first-ak".to_string());
        writer_config.volumes[0].secret_key = Some("first-sk".to_string());

        assert!(refresh_db_properties(&file_manager, "shard-11", &writer_config).unwrap());
        assert!(!refresh_db_properties(&file_manager, "shard-11", &writer_config).unwrap());

        writer_config.volumes[0].access_id = Some("rotated-ak".to_string());
        writer_config.volumes[0].secret_key = Some("rotated-sk".to_string());
        assert!(!refresh_db_properties(&file_manager, "shard-11", &writer_config).unwrap());

        writer_config.l0_file_limit = 13;
        assert!(refresh_db_properties(&file_manager, "shard-11", &writer_config).unwrap());
        let refreshed = load_db_properties(&file_manager, "shard-11").unwrap();
        assert_eq!(refreshed.config.l0_file_limit, 13);
    }

    #[test]
    fn startup_replaces_malformed_properties() {
        let dir = tempfile::tempdir().unwrap();
        let metadata_root = format!("file://{}", dir.path().display());
        let storage_config = Config {
            volumes: VolumeDescriptor::single_volume(metadata_root),
            ..Config::default()
        };
        let metrics = Arc::new(MetricsManager::new("properties-malformed-test"));
        let file_manager = FileManager::from_config(&storage_config, "shard-12", metrics).unwrap();
        file_manager
            .write_plain_metadata_file_atomic(DB_PROPERTIES_NAME, b"not = [valid")
            .unwrap();
        let writer_config = Config {
            volumes: VolumeDescriptor::single_volume("s3://bucket/data"),
            ..Config::default()
        };

        assert!(refresh_db_properties(&file_manager, "shard-12", &writer_config).unwrap());
        load_db_properties(&file_manager, "shard-12").unwrap();
    }
}
