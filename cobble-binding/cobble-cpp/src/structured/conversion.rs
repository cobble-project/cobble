use std::ops::RangeInclusive;

use cobble_binding::structured::{
    ListConfig, ListRetainMode, StructuredColumnType, StructuredSchema, StructuredWriteOptions,
};
use cobble_binding::{Config, RecoveryMode};

use crate::structured_bridge::ffi;

use super::BridgeResult;

pub(crate) fn format_error(error: cobble_binding::Error) -> String {
    crate::error::format_cobble_error(error)
}

pub(crate) fn input_error(message: impl AsRef<str>) -> String {
    crate::error::input_error(message.as_ref())
}

pub(crate) fn parse_config_json(value: &str) -> BridgeResult<Config> {
    Config::from_json_str(value).map_err(format_error)
}

pub(crate) fn parse_config_file(value: &str) -> BridgeResult<Config> {
    Config::from_path(value).map_err(format_error)
}

pub(crate) fn recovery_mode(value: u8) -> BridgeResult<RecoveryMode> {
    match value {
        0 => Ok(RecoveryMode::SnapshotOnly),
        1 => Ok(RecoveryMode::LatestWithWal),
        _ => Err(input_error("unknown recovery mode")),
    }
}

pub(crate) fn full_range(config: &Config) -> BridgeResult<Vec<RangeInclusive<u16>>> {
    if config.total_buckets == 0 || config.total_buckets > u32::from(u16::MAX) + 1 {
        return Err(input_error("total_buckets must be in range 1..=65536"));
    }
    Ok(vec![
        0..=u16::try_from(config.total_buckets - 1)
            .map_err(|_| input_error("total_buckets exceeds the bucket id range"))?,
    ])
}

pub(crate) fn ranges(
    config: &Config,
    values: Vec<ffi::NativeBucketRange>,
) -> BridgeResult<Vec<RangeInclusive<u16>>> {
    full_range(config)?;
    if values.is_empty() {
        return Err(input_error("bucket ranges must not be empty"));
    }
    values
        .into_iter()
        .map(|value| {
            if value.start_inclusive > value.end_inclusive {
                return Err(input_error("bucket range is reversed"));
            }
            if u32::from(value.end_inclusive) >= config.total_buckets {
                return Err(input_error("bucket range exceeds total_buckets"));
            }
            Ok(value.start_inclusive..=value.end_inclusive)
        })
        .collect()
}

pub(crate) fn write_options(
    value: &ffi::NativeWriteOptions,
) -> BridgeResult<StructuredWriteOptions> {
    if value.has_column_family && value.column_family.is_empty() {
        return Err(input_error("column family must not be empty"));
    }
    let mut raw = cobble_binding::WriteOptions::default().with_await_durable(value.await_durable);
    if value.has_ttl_seconds {
        raw = cobble_binding::WriteOptions::with_ttl(value.ttl_seconds)
            .with_await_durable(value.await_durable);
    }
    if value.has_column_family {
        raw.column_family = Some(value.column_family.clone());
    }
    Ok(raw.into())
}

pub(crate) fn list_config(value: &ffi::NativeListConfig) -> BridgeResult<ListConfig> {
    let retain_mode = match value.retain_mode {
        0 => ListRetainMode::First,
        1 => ListRetainMode::Last,
        _ => return Err(input_error("unknown list retain mode")),
    };
    Ok(ListConfig {
        max_elements: value
            .has_max_elements
            .then(|| usize::try_from(value.max_elements))
            .transpose()
            .map_err(|_| input_error("max_elements does not fit usize"))?,
        retain_mode,
        preserve_element_ttl: value.preserve_element_ttl,
    })
}

pub(crate) fn borrowed_elements(values: &[ffi::NativeBytesDescriptor]) -> BridgeResult<Vec<&[u8]>> {
    values
        .iter()
        .map(|value| {
            if value.length == 0 {
                return Ok(&[][..]);
            }
            if value.data == 0 {
                return Err(input_error(
                    "non-empty list element has a null data pointer",
                ));
            }
            if value.length > isize::MAX as usize {
                return Err(input_error("list element length exceeds isize::MAX"));
            }
            // SAFETY: the C++ wrapper constructs every descriptor from a BytesView and keeps all
            // borrowed spans alive for this synchronous bridge call. The checks above reject the
            // invalid null/oversized cases before materializing the slice.
            Ok(unsafe { std::slice::from_raw_parts(value.data as *const u8, value.length) })
        })
        .collect()
}

pub(crate) fn native_schema(schema: StructuredSchema) -> ffi::NativeStructuredSchema {
    let mut families = Vec::new();
    for (name, family) in schema.column_families() {
        let id = schema.column_family_ids.get(&name).copied().unwrap_or(0);
        let columns = family
            .columns
            .into_iter()
            .map(|(index, column_type)| match column_type {
                StructuredColumnType::Bytes => ffi::NativeStructuredColumn {
                    index,
                    kind: 0,
                    list: native_list_config(&ListConfig::default()),
                },
                StructuredColumnType::List(config) => ffi::NativeStructuredColumn {
                    index,
                    kind: 1,
                    list: native_list_config(&config),
                },
            })
            .collect();
        families.push(ffi::NativeStructuredFamily { name, id, columns });
    }
    ffi::NativeStructuredSchema { families }
}

pub(crate) fn native_list_config(config: &ListConfig) -> ffi::NativeListConfig {
    ffi::NativeListConfig {
        has_max_elements: config.max_elements.is_some(),
        max_elements: config.max_elements.unwrap_or_default() as u64,
        retain_mode: match config.retain_mode {
            ListRetainMode::First => 0,
            ListRetainMode::Last => 1,
        },
        preserve_element_ttl: config.preserve_element_ttl,
    }
}
