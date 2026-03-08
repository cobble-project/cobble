use crate::Config;
use bytes::Bytes;
use std::ops::RangeInclusive;

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
