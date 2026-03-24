use crate::Config;
use crate::error::{Error, Result};
use bytes::Bytes;
use std::ops::RangeInclusive;
use std::path::{Path, PathBuf};
use url::Url;

/// Creates a `Bytes` instance that shares the same underlying data as the input slice.
#[inline]
pub(crate) fn unsafe_bytes(target: &[u8]) -> Bytes {
    Bytes::from_owner(unsafe { std::slice::from_raw_parts(target.as_ptr(), target.len()) })
}

pub(crate) fn init_logging(config: &Config) {
    static INIT: std::sync::Once = std::sync::Once::new();
    INIT.call_once(|| {
        let mut builder = log4rs::Config::builder();
        let mut root = log4rs::config::Root::builder();
        if config.log_console {
            let stdout = log4rs::append::console::ConsoleAppender::builder().build();
            builder = builder
                .appender(log4rs::config::Appender::builder().build("stdout", Box::new(stdout)));
            root = root.appender("stdout");
        }
        if let Some(ref path) = config.log_path {
            let file = log4rs::append::file::FileAppender::builder()
                .build(path)
                .unwrap();
            builder =
                builder.appender(log4rs::config::Appender::builder().build("file", Box::new(file)));
            root = root.appender("file");
        }
        let root = root.build(config.log_level);
        let config = builder.build(root).unwrap();
        let _ = log4rs::init_config(config);
    });
}

pub(crate) fn ranges_overlap(left: &RangeInclusive<u16>, right: &RangeInclusive<u16>) -> bool {
    !(*left.end() < *right.start() || *right.end() < *left.start())
}

pub(crate) fn normalize_bucket_ranges(
    mut ranges: Vec<RangeInclusive<u16>>,
) -> Vec<RangeInclusive<u16>> {
    if ranges.is_empty() {
        return ranges;
    }
    ranges.sort_by_key(|range| *range.start());
    let mut merged: Vec<RangeInclusive<u16>> = Vec::with_capacity(ranges.len());
    for range in ranges {
        if let Some(last) = merged.last_mut()
            && (*range.start() as u32) <= (*last.end() as u32).saturating_add(1)
        {
            let start = *last.start();
            let end = (*last.end()).max(*range.end());
            *last = start..=end;
            continue;
        }
        merged.push(range);
    }
    merged
}

pub(crate) fn range_is_covered_by_ranges(
    range: &RangeInclusive<u16>,
    coverage: &[RangeInclusive<u16>],
) -> bool {
    (*range.start()..=*range.end())
        .all(|bucket| coverage.iter().any(|source| source.contains(&bucket)))
}

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

#[cfg(test)]
mod tests {
    use super::normalize_storage_path_to_url;

    #[test]
    fn test_normalize_storage_path_to_url_keeps_remote_url() {
        let input = "s3://bucket-name/prefix";
        let normalized = normalize_storage_path_to_url(input).unwrap();
        assert_eq!(normalized, input);
    }

    #[test]
    fn test_normalize_storage_path_to_url_file_url_roundtrip() {
        let input = "file:///tmp/cobble";
        let normalized = normalize_storage_path_to_url(input).unwrap();
        assert!(normalized.starts_with("file:///tmp/cobble"));
    }

    #[test]
    fn test_normalize_storage_path_to_url_local_path_to_file_url() {
        let path = std::env::temp_dir().join("cobble-local-path");
        let path_str = path.to_string_lossy();
        let normalized = normalize_storage_path_to_url(&path_str).unwrap();
        assert!(normalized.starts_with("file://"));
    }

    #[test]
    fn test_normalize_storage_path_to_url_relative_path_becomes_absolute_file_url() {
        let normalized = normalize_storage_path_to_url("./cobble-relative-path").unwrap();
        assert!(normalized.starts_with("file://"));
        let parsed = url::Url::parse(&normalized).unwrap();
        let local = parsed.to_file_path().unwrap();
        assert!(local.is_absolute());
    }
}

pub(crate) fn subtract_ranges(
    source: &[RangeInclusive<u16>],
    cuts: &[RangeInclusive<u16>],
) -> Vec<RangeInclusive<u16>> {
    let mut result = Vec::new();
    for range in source {
        result.extend(subtract_range_by_cuts(range, cuts));
    }
    normalize_bucket_ranges(result)
}

pub(crate) fn subtract_range_by_cuts(
    range: &RangeInclusive<u16>,
    cuts: &[RangeInclusive<u16>],
) -> Vec<RangeInclusive<u16>> {
    let mut segments = vec![range.clone()];
    for cut in cuts {
        let mut next = Vec::new();
        for segment in segments {
            if !ranges_overlap(&segment, cut) {
                next.push(segment);
                continue;
            }
            let seg_start = *segment.start();
            let seg_end = *segment.end();
            let cut_start = *cut.start();
            let cut_end = *cut.end();
            if cut_start > seg_start {
                let left_end = cut_start.saturating_sub(1);
                if left_end >= seg_start {
                    next.push(seg_start..=left_end);
                }
            }
            if cut_end < seg_end {
                let right_start = cut_end.saturating_add(1);
                if right_start <= seg_end {
                    next.push(right_start..=seg_end);
                }
            }
        }
        if next.is_empty() {
            return next;
        }
        segments = next;
    }
    segments
}
