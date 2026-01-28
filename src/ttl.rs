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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::time::ManualTimeProvider;

    #[test]
    fn test_ttl_provider() {
        let time_provider = Arc::new(ManualTimeProvider::new(1000));
        let ttl_provider = TTLProvider::new(
            &TtlConfig {
                enabled: true,
                default_ttl_seconds: Some(500),
            },
            time_provider.clone(),
        );

        // Test default TTL
        let expiration = ttl_provider.get_expiration_timestamp(None);
        assert_eq!(expiration, Some(1500));

        // Test custom TTL
        let expiration = ttl_provider.get_expiration_timestamp(Some(300));
        assert_eq!(expiration, Some(1300));

        // Test expiration check
        assert!(!ttl_provider.expired(&Some(1500)));
        time_provider.set_time(1600);
        assert!(ttl_provider.expired(&Some(1500)));
    }
}
