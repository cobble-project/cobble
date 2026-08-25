//! Feature-gated bridge for language bindings.
//!
//! These functions expose connector-only capabilities without adding them to
//! Cobble's default Rust API surface.

use crate::{Db, Result};

#[inline]
pub fn db_direct_buffer_pool_config(db: &Db) -> Result<(usize, usize)> {
    db.jni_direct_buffer_pool_config()
}

#[inline]
pub fn build_commit_short_id() -> &'static str {
    crate::util::build_commit_short_id()
}

#[inline]
pub fn build_version_string() -> &'static str {
    crate::util::build_version_string()
}
