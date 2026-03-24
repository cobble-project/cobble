use crate::Config;
use crate::db::Db;
use crate::error::Result;
use crate::governance::DbGovernance;
use std::ops::RangeInclusive;
use std::sync::Arc;

pub(crate) type DbBuilderParts = (
    Config,
    Vec<RangeInclusive<u16>>,
    Option<String>,
    Option<Arc<dyn DbGovernance>>,
);

/// Builder for opening a writable [`Db`] with optional custom runtime wiring.
pub struct DbBuilder {
    config: Config,
    bucket_ranges: Vec<RangeInclusive<u16>>,
    db_id: Option<String>,
    governance: Option<Arc<dyn DbGovernance>>,
}

impl DbBuilder {
    pub fn new(config: Config) -> Self {
        Self {
            config,
            bucket_ranges: Vec::new(),
            db_id: None,
            governance: None,
        }
    }

    pub fn bucket_ranges(mut self, bucket_ranges: Vec<RangeInclusive<u16>>) -> Self {
        self.bucket_ranges = bucket_ranges;
        self
    }

    pub fn db_id<S>(mut self, db_id: S) -> Self
    where
        S: Into<String>,
    {
        self.db_id = Some(db_id.into());
        self
    }

    pub fn governance(mut self, governance: Arc<dyn DbGovernance>) -> Self {
        self.governance = Some(governance);
        self
    }

    pub fn open(self) -> Result<Db> {
        Db::open_with_builder(self)
    }

    pub(crate) fn into_parts(self) -> DbBuilderParts {
        (self.config, self.bucket_ranges, self.db_id, self.governance)
    }
}
