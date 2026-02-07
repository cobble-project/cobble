//! Time provider abstractions for TTL.
use log::error;
use std::sync::Arc;
use std::sync::atomic::AtomicU32;
use std::time::{SystemTime, UNIX_EPOCH};

/// Trait for providing database time in seconds.
pub trait TimeProvider: Send + Sync {
    /// Returns current time in seconds since UNIX epoch.
    fn now_seconds(&self) -> u32;

    /// Sets the current time to `next` if supported.
    fn set_time(&self, next: u32);
}

/// System time provider backed by `SystemTime`.
#[derive(Default, Clone)]
pub struct SystemTimeProvider;

impl TimeProvider for SystemTimeProvider {
    fn now_seconds(&self) -> u32 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs() as u32)
            .unwrap_or(0)
    }

    fn set_time(&self, _next: u32) {
        // No-op for system time provider.
        error!("Cannot set time on SystemTimeProvider");
    }
}

/// Manual time provider that can be advanced manually.
/// Time is monotonic and cannot go backwards.
pub struct ManualTimeProvider {
    watermark: AtomicU32,
}

impl ManualTimeProvider {
    /// Creates a new manual time provider starting at `initial`.
    pub fn new(initial: u32) -> Self {
        Self {
            watermark: AtomicU32::new(initial),
        }
    }

    /// Sets the current time to `next` if it is greater than the current time.
    pub fn set_time(&self, next: u32) {
        self.watermark
            .fetch_update(
                std::sync::atomic::Ordering::Relaxed,
                std::sync::atomic::Ordering::Relaxed,
                |current| Some(current.max(next)),
            )
            .ok();
    }
}

impl Default for ManualTimeProvider {
    fn default() -> Self {
        Self::new(0)
    }
}

impl TimeProvider for ManualTimeProvider {
    fn now_seconds(&self) -> u32 {
        self.watermark.load(std::sync::atomic::Ordering::Relaxed)
    }

    fn set_time(&self, next: u32) {
        self.set_time(next);
    }
}

/// Time provider selection for configuration.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TimeProviderKind {
    /// Use system time.
    #[default]
    System,
    /// Use a manual time provider (starts at 0).
    Manual,
}

impl TimeProviderKind {
    /// Creates a time provider instance based on the kind.
    pub fn create(&self) -> Arc<dyn TimeProvider> {
        match self {
            TimeProviderKind::System => Arc::new(SystemTimeProvider),
            TimeProviderKind::Manual => Arc::new(ManualTimeProvider::default()),
        }
    }
}
