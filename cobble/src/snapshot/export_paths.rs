//! Storage URL identity checks shared by shard and global snapshot exports.
use crate::error::{Error, Result};
use crate::util::normalize_storage_path_to_url;
use std::path::{Path, PathBuf};
use url::Url;

pub(crate) fn storage_url(path: &str) -> Result<Url> {
    let mut url = Url::parse(&normalize_storage_path_to_url(path)?)
        .map_err(|error| Error::ConfigError(format!("Invalid export storage route: {error}")))?;
    let _ = url.set_username("");
    let _ = url.set_password(None);
    url.set_query(None);
    url.set_fragment(None);
    Ok(url)
}

pub(crate) fn append_path(base: &Url, relative: &str) -> Result<Url> {
    let mut url = base.clone();
    let mut segments = url.path_segments_mut().map_err(|_| {
        Error::ConfigError("Snapshot export requires a hierarchical storage URL".into())
    })?;
    segments.pop_if_empty();
    for component in relative
        .split('/')
        .filter(|component| !component.is_empty())
    {
        segments.push(component);
    }
    drop(segments);
    Ok(url)
}

fn canonical_future_path(path: &Path) -> Result<PathBuf> {
    let mut existing = path;
    let mut missing = Vec::new();
    loop {
        match std::fs::canonicalize(existing) {
            Ok(mut canonical) => {
                for component in missing.into_iter().rev() {
                    canonical.push(component);
                }
                return Ok(canonical);
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let name = existing.file_name().ok_or_else(|| {
                    Error::ConfigError("Cannot resolve snapshot export destination".into())
                })?;
                missing.push(name.to_os_string());
                existing = existing.parent().ok_or_else(|| {
                    Error::ConfigError("Cannot resolve snapshot export destination parent".into())
                })?;
            }
            Err(error) => return Err(Error::IoError(error.to_string())),
        }
    }
}

pub(crate) fn overlaps(left: &Url, right: &Url) -> Result<bool> {
    if left.scheme() != right.scheme()
        || left.host_str() != right.host_str()
        || left.port() != right.port()
    {
        return Ok(false);
    }
    if left.scheme() == "file" {
        let local = |url: &Url| {
            url.to_file_path()
                .map_err(|_| Error::ConfigError("Invalid local export path".into()))
                .and_then(|path| canonical_future_path(&path))
        };
        let left = local(left)?;
        let right = local(right)?;
        return Ok(left.starts_with(&right) || right.starts_with(&left));
    }
    let contains = |parent: &str, child: &str| {
        child
            .strip_prefix(parent.trim_end_matches('/'))
            .is_some_and(|rest| rest.is_empty() || rest.starts_with('/'))
    };
    Ok(contains(left.path(), right.path()) || contains(right.path(), left.path()))
}
