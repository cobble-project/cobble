//! Storage URL paths, configured filesystem routes, and option-free export identities.
use crate::error::{Error, Result};
use std::borrow::Cow;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use url::Url;

/// Normalizes a storage path input into a canonical URL string.
///
/// Rules:
/// - If input is already a valid URL, return normalized URL text.
/// - If input is a local filesystem path, convert it to a `file://` URL.
/// - Relative local paths are converted to absolute paths before URL conversion.
pub(crate) fn normalize_storage_path_to_url(input: impl AsRef<str>) -> Result<String> {
    let trimmed = input.as_ref().trim();
    if trimmed.is_empty() {
        return Err(Error::ConfigError(
            "storage path must not be empty".to_string(),
        ));
    }
    if trimmed.contains("://") {
        let url = Url::parse(trimmed).map_err(|err| {
            Error::ConfigError(format!("Invalid storage URL {}: {}", trimmed, err))
        })?;
        if url.scheme().eq_ignore_ascii_case("file") {
            let path = url
                .to_file_path()
                .map_err(|_| Error::ConfigError(format!("Invalid file URL path: {}", trimmed)))?;
            let normalized = Url::from_file_path(&path).map_err(|_| {
                Error::ConfigError(format!("Failed to normalize file URL path: {}", trimmed))
            })?;
            return Ok(normalized.to_string());
        }
        return Ok(url.to_string());
    }

    let candidate = Path::new(trimmed);
    let absolute: PathBuf = if candidate.is_absolute() {
        candidate.to_path_buf()
    } else {
        std::env::current_dir()
            .map_err(|err| {
                Error::ConfigError(format!("Failed to resolve current directory: {}", err))
            })?
            .join(candidate)
    };
    let file_url = Url::from_file_path(&absolute).map_err(|_| {
        Error::ConfigError(format!(
            "Failed to convert local path to file URL: {}",
            trimmed
        ))
    })?;
    Ok(file_url.to_string())
}

/// Append a filesystem-relative path to a normalized volume URL, not to its options.
pub(crate) fn append_storage_path(base: &str, relative: &str) -> String {
    if !base.contains(['?', '#']) {
        let separator = if base.ends_with('/') { "" } else { "/" };
        return format!("{base}{separator}{}", relative.trim_start_matches('/'));
    }
    let url = Url::parse(base).expect("volume URL is normalized");
    append_path(&url, relative)
        .expect("volume URL is hierarchical")
        .to_string()
}

/// Resolve normalized URLs against configured volume roots. Missing URL options may use a unique
/// configured route (exported manifests intentionally omit options), but never guess between routes.
pub(crate) fn match_storage_path<'a>(
    path: &str,
    bases: impl IntoIterator<Item = (usize, &'a str)>,
) -> Result<Option<(usize, String)>> {
    let relative = |path: &str, base: &str| {
        path.strip_prefix(base.trim_end_matches('/'))
            .filter(|suffix| suffix.is_empty() || suffix.starts_with('/'))
            .map(|suffix| suffix.trim_start_matches('/').to_string())
    };
    let mut parsed_path = None;
    let mut best = None;
    let mut best_query = BTreeMap::new();
    let mut ambiguous = false;
    for (index, base) in bases {
        let (suffix, prefix_len, exact_query, query) = if !path.contains(['?', '#'])
            && !base.contains(['?', '#'])
            && !base.ends_with(":///")
        {
            let Some(suffix) = relative(path, base) else {
                continue;
            };
            (
                suffix,
                base.trim_end_matches('/').len(),
                true,
                BTreeMap::new(),
            )
        } else {
            let (path_url, path_options) = match &parsed_path {
                Some(parsed) => parsed,
                None => {
                    let url = Url::parse(path)?;
                    let options = url.query_pairs().into_owned().collect::<BTreeMap<_, _>>();
                    parsed_path.insert((url, options))
                }
            };
            let base_url = Url::parse(base)?;
            let base_options = base_url
                .query_pairs()
                .into_owned()
                .collect::<BTreeMap<_, _>>();
            if path_url[..url::Position::BeforePath] != base_url[..url::Position::BeforePath]
                || (!path_options.is_empty() && *path_options != base_options)
            {
                continue;
            }
            let Some(suffix) = relative(path_url.path(), base_url.path()) else {
                continue;
            };
            (
                suffix,
                base_url[..url::Position::AfterPath]
                    .trim_end_matches('/')
                    .len(),
                *path_options == base_options,
                base_options,
            )
        };
        let score = (prefix_len, exact_query);
        match &best {
            Some((_, _, best_score)) if score < *best_score => {}
            Some((_, _, best_score)) if score == *best_score => {
                ambiguous |= query != best_query;
            }
            _ => {
                best = Some((index, suffix, score));
                best_query = query;
                ambiguous = false;
            }
        }
    }
    if ambiguous {
        return Err(Error::ConfigError(
            "Ambiguous storage path: multiple configured volumes match without URL options".into(),
        ));
    }
    Ok(best.map(|(index, suffix, _)| {
        let suffix = if path.starts_with("file://") && suffix.contains('%') {
            percent_encoding::percent_decode_str(&suffix)
                .decode_utf8_lossy()
                .into_owned()
        } else {
            suffix
        };
        (index, suffix)
    }))
}

/// Remove credentials, query options and fragments for export identity/comparison and persisted
/// compaction locations. This is not a filesystem configuration: backend routing options are lost.
pub(crate) fn storage_location_url(path: &str) -> Result<Url> {
    let mut url = Url::parse(&normalize_storage_path_to_url(path)?)
        .map_err(|error| Error::ConfigError(format!("Invalid storage URL: {error}")))?;
    let _ = url.set_username("");
    let _ = url.set_password(None);
    url.set_query(None);
    url.set_fragment(None);
    Ok(url)
}

/// Append to the URL path while preserving query options and fragments.
pub(crate) fn append_path(base: &Url, relative: &str) -> Result<Url> {
    let mut url = base.clone();
    let mut segments = url
        .path_segments_mut()
        .map_err(|_| Error::ConfigError("Storage URL requires a hierarchical path".into()))?;
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

/// Extract the encoded path of a hierarchical storage URL, or keep a local/relative path intact.
/// Normalized URLs without options can borrow their path without reparsing or allocating.
pub(crate) fn url_path(path: &str) -> Result<Cow<'_, str>> {
    let Some(authority_start) = path.find("://").map(|offset| offset + 3) else {
        return Ok(Cow::Borrowed(path));
    };
    if path.contains(['?', '#']) {
        return Ok(Cow::Owned(Url::parse(path)?.path().to_string()));
    }
    let path_start = path[authority_start..]
        .find('/')
        .map_or(path.len(), |offset| authority_start + offset);
    Ok(Cow::Borrowed(&path[path_start..]))
}

/// Rewrite an already-encoded storage URL path without treating options as path components.
pub(crate) fn map_url_path(path: &str, map: impl FnOnce(&str) -> Result<String>) -> Result<String> {
    if path.contains("://") && path.contains(['?', '#']) {
        let mut url = Url::parse(path)?;
        let mapped = map(url.path())?;
        url.set_path(&mapped);
        Ok(url.to_string())
    } else {
        let component = url_path(path)?;
        let mapped = map(&component)?;
        Ok(format!(
            "{}{}",
            &path[..path.len() - component.len()],
            mapped
        ))
    }
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

#[cfg(test)]
#[path = "../tests/unit/storage_url.rs"]
mod tests;
