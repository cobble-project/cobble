//! Per-DB writer properties shared with external compaction processes.

use crate::error::{Error, Result};
use crate::file::{FileManager, ReadAllFile};
use crate::{
    Config,
    config::{resolve_volume_descriptor_credentials, sanitize_volume_descriptor},
};
use serde::{Deserialize, Serialize};
#[cfg(test)]
use std::collections::HashMap;

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
            *volume = sanitize_volume_descriptor(volume);
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
        *persisted = resolve_volume_descriptor_credentials(persisted, process_config);
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

#[cfg(test)]
#[path = "../tests/unit/properties.rs"]
mod tests;
