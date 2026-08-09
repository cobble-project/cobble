//! TTL (Time-To-Live) management for data entries.
//! This module provides functionality to handle TTL settings, including
//! calculating expiration timestamps and checking for expiration based on the current time.

use std::sync::Arc;

/// TTL configuration.
#[derive(Clone, Debug, Default)]
pub struct TtlConfig {
    /// Whether TTL is enabled. If false, TTL metadata is ignored.
    pub enabled: bool,
    /// Default TTL duration in seconds. None means no expiration unless provided per-write.
    pub default_ttl_seconds: Option<u32>,
}

#[derive(Clone)]
pub struct TTLProvider {
    enabled: bool,
    default_ttl: Option<u32>,
    time_provider: Arc<dyn crate::time::TimeProvider>,
}

impl TTLProvider {
    pub fn new(config: &TtlConfig, time_provider: Arc<dyn crate::time::TimeProvider>) -> Self {
        Self {
            enabled: config.enabled,
            default_ttl: config.default_ttl_seconds,
            time_provider,
        }
    }

    pub fn disabled() -> Self {
        Self {
            enabled: false,
            default_ttl: None,
            time_provider: Arc::new(crate::time::SystemTimeProvider),
        }
    }

    /// Get expiration timestamp based on TTL seconds.
    /// Returns None if TTL is disabled or no TTL is set.
    pub(crate) fn get_expiration_timestamp(&self, ttl_seconds: Option<u32>) -> Option<u32> {
        if !self.enabled {
            return None;
        }
        let ttl = ttl_seconds.or(self.default_ttl);
        ttl.map(|t| self.time_provider.now_seconds() + t)
    }

    /// Check if the given expiration timestamp is expired.
    pub(crate) fn expired(&self, expired_at: &Option<u32>) -> bool {
        if !self.enabled {
            return false;
        }
        match expired_at {
            Some(ts) => self.time_provider.now_seconds() >= *ts,
            None => false,
        }
    }

    /// Expose current time for helpers.
    pub(crate) fn now_seconds(&self) -> u32 {
        self.time_provider.now_seconds()
    }

    /// Returns whether TTL expiration is enabled.
    pub(crate) fn is_enabled(&self) -> bool {
        self.enabled
    }

    pub(crate) fn time_provider(&self) -> &dyn crate::time::TimeProvider {
        self.time_provider.as_ref()
    }
}

#[cfg(test)]
#[path = "../tests/unit/ttl.rs"]
mod tests;
