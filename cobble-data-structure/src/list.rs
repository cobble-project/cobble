use bytes::{Buf, BufMut, Bytes, BytesMut};
use cobble::{Error, MergeOperator, Result, TimeProvider, ValueType};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::collections::VecDeque;
use std::mem::size_of;
use std::sync::Arc;

pub(crate) const LIST_OPERATOR_ID: &str = "cobble.list.v1";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListConfig {
    pub max_elements: Option<usize>,
    pub retain_mode: ListRetainMode,
    pub preserve_element_ttl: bool,
}

impl Default for ListConfig {
    fn default() -> Self {
        Self {
            max_elements: None,
            retain_mode: ListRetainMode::Last,
            preserve_element_ttl: false,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ListRetainMode {
    First,
    #[default]
    Last,
}

#[derive(Clone)]
struct ListMergeOperator {
    config: ListConfig,
}

impl ListMergeOperator {
    fn new(config: ListConfig) -> Self {
        Self { config }
    }
}

impl MergeOperator for ListMergeOperator {
    fn id(&self) -> String {
        LIST_OPERATOR_ID.to_string()
    }

    fn metadata(&self) -> Option<JsonValue> {
        serde_json::to_value(&self.config).ok()
    }

    fn merge(
        &self,
        existing_value: Bytes,
        value: Bytes,
        time_provider: Option<&dyn TimeProvider>,
    ) -> Result<(Bytes, Option<ValueType>)> {
        self.merge_batch(existing_value, vec![value], time_provider)
    }

    fn merge_batch(
        &self,
        existing_value: Bytes,
        operands: Vec<Bytes>,
        time_provider: Option<&dyn TimeProvider>,
    ) -> Result<(Bytes, Option<ValueType>)> {
        if operands.is_empty() {
            return Ok((existing_value, None));
        }

        // Fast path: validate payloads, then splice raw encoded bodies without decode/re-encode.
        if let Some(merged) = try_fast_append_batch(&existing_value, &operands, &self.config)? {
            return Ok((merged, None));
        }

        let now_seconds = time_provider
            .map(|provider| provider.now_seconds())
            .unwrap_or(0);
        if let (ListRetainMode::Last, Some(max_elements)) =
            (self.config.retain_mode, self.config.max_elements)
        {
            let (elements, reached_last_cap) = collect_last_from_newest(
                &existing_value,
                &operands,
                &self.config,
                now_seconds,
                max_elements,
            )?;
            let output = encode_list_payload(&elements, &self.config)?;
            let value_type = if reached_last_cap && !self.config.preserve_element_ttl {
                Some(ValueType::Put)
            } else {
                None
            };
            return Ok((output, value_type));
        }
        let mut accumulator = ListAccumulator::new(&self.config);
        accumulator.ingest_payload(&existing_value, now_seconds)?;
        if !accumulator.should_stop() {
            for operand in &operands {
                accumulator.ingest_payload(operand, now_seconds)?;
                if accumulator.should_stop() {
                    break;
                }
            }
        }
        let (elements, reached_last_cap) = accumulator.into_parts();
        let output = encode_list_payload(&elements, &self.config)?;
        let value_type = if reached_last_cap
            && self.config.retain_mode == ListRetainMode::Last
            && !self.config.preserve_element_ttl
        {
            Some(ValueType::Put)
        } else {
            None
        };
        Ok((output, value_type))
    }
}

pub(crate) fn list_operator(config: ListConfig) -> Arc<dyn MergeOperator> {
    Arc::new(ListMergeOperator::new(config))
}

pub(crate) fn list_operator_from_metadata(
    id: &str,
    metadata: Option<&JsonValue>,
) -> Option<Arc<dyn MergeOperator>> {
    if id != LIST_OPERATOR_ID {
        return None;
    }
    let config = serde_json::from_value::<ListConfig>(metadata?.clone()).ok()?;
    Some(list_operator(config))
}

pub(crate) fn encode_list_for_write(
    elements: Vec<Bytes>,
    config: &ListConfig,
    ttl_seconds: Option<u32>,
    now_seconds: u32,
) -> Result<Bytes> {
    let expires_at_secs = if config.preserve_element_ttl {
        ttl_seconds.map(|ttl| now_seconds.saturating_add(ttl))
    } else {
        None
    };
    let decoded = elements
        .into_iter()
        .map(|value| DecodedListElement {
            value,
            expires_at_secs,
        })
        .collect::<Vec<_>>();
    encode_list_payload(&decoded, config)
}

pub(crate) fn decode_list_for_read(
    raw: &Bytes,
    config: &ListConfig,
    now_seconds: u32,
) -> Result<Vec<Bytes>> {
    let mut accumulator = ListAccumulator::new(config);
    accumulator.ingest_payload(raw, now_seconds)?;
    let (elements, _) = accumulator.into_parts();
    Ok(elements.into_iter().map(|element| element.value).collect())
}

#[derive(Clone)]
struct DecodedListElement {
    value: Bytes,
    expires_at_secs: Option<u32>,
}

/// Cursor over a single encoded list payload.
///
/// `remaining` owns a cheap clone of the original `Bytes`, so each `split_to` is zero-copy.
struct ListPayloadCursor {
    remaining: Bytes,
    preserve_element_ttl: bool,
    remaining_elements: usize,
}

impl ListPayloadCursor {
    fn new(payload: &Bytes, preserve_element_ttl: bool) -> Result<Self> {
        if payload.is_empty() {
            return Ok(Self {
                remaining: Bytes::new(),
                preserve_element_ttl,
                remaining_elements: 0,
            });
        }
        let mut remaining = payload.clone();
        if remaining.remaining() < size_of::<u32>() {
            return Err(Error::FileFormatError(
                "invalid list payload: missing element count".to_string(),
            ));
        }
        let remaining_elements = remaining.get_u32_le() as usize;
        Ok(Self {
            remaining,
            preserve_element_ttl,
            remaining_elements,
        })
    }

    fn next(&mut self) -> Result<Option<DecodedListElement>> {
        if self.remaining_elements == 0 {
            if self.remaining.has_remaining() {
                return Err(Error::InvalidState(
                    "invalid list payload: trailing bytes found".to_string(),
                ));
            }
            return Ok(None);
        }
        let expires_at_secs = if self.preserve_element_ttl {
            if self.remaining.remaining() < size_of::<u32>() {
                return Err(Error::InvalidState(
                    "invalid list payload: missing element ttl timestamp".to_string(),
                ));
            }
            let expires_at = self.remaining.get_u32_le();
            if expires_at == 0 {
                None
            } else {
                Some(expires_at)
            }
        } else {
            None
        };
        if self.remaining.remaining() < size_of::<u32>() {
            return Err(Error::InvalidState(
                "invalid list payload: missing element length".to_string(),
            ));
        }
        let element_len = self.remaining.get_u32_le() as usize;
        if self.remaining.remaining() < element_len {
            return Err(Error::InvalidState(format!(
                "invalid list payload: element length {} exceeds remaining {}",
                element_len,
                self.remaining.remaining()
            )));
        }
        self.remaining_elements -= 1;
        Ok(Some(DecodedListElement {
            value: self.remaining.split_to(element_len),
            expires_at_secs,
        }))
    }
}

/// Streaming accumulator used by merge/read paths.
///
/// It filters TTL inline and applies first/last policies while scanning payloads, so merge does
/// not need to decode everything into a temporary full vector.
struct ListAccumulator {
    config: ListConfig,
    mode: ListAccumulatorMode,
    reached_last_cap: bool,
}

enum ListAccumulatorMode {
    All(Vec<DecodedListElement>),
    First {
        max: usize,
        kept: Vec<DecodedListElement>,
    },
    Last {
        max: usize,
        kept: VecDeque<DecodedListElement>,
    },
}

impl ListAccumulator {
    fn new(config: &ListConfig) -> Self {
        let mode = match (config.max_elements, config.retain_mode) {
            (Some(max), ListRetainMode::First) => ListAccumulatorMode::First {
                max,
                kept: Vec::with_capacity(max),
            },
            (Some(max), ListRetainMode::Last) => ListAccumulatorMode::Last {
                max,
                kept: VecDeque::with_capacity(max),
            },
            (None, _) => ListAccumulatorMode::All(Vec::new()),
        };
        Self {
            config: config.clone(),
            mode,
            reached_last_cap: false,
        }
    }

    fn ingest_payload(&mut self, payload: &Bytes, now_seconds: u32) -> Result<()> {
        let mut cursor = ListPayloadCursor::new(payload, self.config.preserve_element_ttl)?;
        while let Some(element) = cursor.next()? {
            if self.config.preserve_element_ttl
                && element
                    .expires_at_secs
                    .is_some_and(|expires_at| expires_at <= now_seconds)
            {
                continue;
            }
            match &mut self.mode {
                ListAccumulatorMode::All(values) => {
                    values.push(element);
                }
                ListAccumulatorMode::First { max, kept } => {
                    if kept.len() < *max {
                        kept.push(element);
                    }
                }
                ListAccumulatorMode::Last { max, kept } => {
                    kept.push_back(element);
                    if kept.len() > *max {
                        let _ = kept.pop_front();
                    }
                    if kept.len() == *max {
                        self.reached_last_cap = true;
                    }
                }
            }
            if self.should_stop() {
                break;
            }
        }
        Ok(())
    }

    fn should_stop(&self) -> bool {
        match &self.mode {
            ListAccumulatorMode::First { max, kept } => kept.len() >= *max,
            ListAccumulatorMode::All(_) | ListAccumulatorMode::Last { .. } => false,
        }
    }

    fn into_parts(self) -> (Vec<DecodedListElement>, bool) {
        let elements = match self.mode {
            ListAccumulatorMode::All(values) => values,
            ListAccumulatorMode::First { kept, .. } => kept,
            ListAccumulatorMode::Last { kept, .. } => kept.into_iter().collect(),
        };
        (elements, self.reached_last_cap)
    }
}

fn encode_list_payload(elements: &[DecodedListElement], config: &ListConfig) -> Result<Bytes> {
    if elements.len() > u32::MAX as usize {
        return Err(Error::InputError(format!(
            "too many list elements to encode: {}",
            elements.len()
        )));
    }
    let ttl_bytes = if config.preserve_element_ttl {
        size_of::<u32>()
    } else {
        0
    };
    let total_size = size_of::<u32>()
        + elements
            .iter()
            .map(|item| ttl_bytes + size_of::<u32>() + item.value.len())
            .sum::<usize>();
    let mut out = BytesMut::with_capacity(total_size);
    out.put_u32_le(elements.len() as u32);
    for element in elements {
        if config.preserve_element_ttl {
            out.put_u32_le(element.expires_at_secs.unwrap_or(0));
        }
        out.put_u32_le(element.value.len() as u32);
        out.extend_from_slice(element.value.as_ref());
    }
    Ok(out.freeze())
}

fn try_fast_append_batch(
    existing: &Bytes,
    operands: &[Bytes],
    config: &ListConfig,
) -> Result<Option<Bytes>> {
    if config.preserve_element_ttl {
        return Ok(None);
    }
    let (mut total_count, existing_body) = parse_payload_body(existing)?;
    let mut operand_bodies = Vec::with_capacity(operands.len());
    for operand in operands {
        let (count, body) = parse_payload_body(operand)?;
        total_count = total_count.checked_add(count).ok_or_else(|| {
            Error::InputError(format!(
                "list element count overflow during merge: {} + {}",
                total_count, count
            ))
        })?;
        operand_bodies.push(body);
    }
    if let Some(max_elements) = config.max_elements
        && total_count > max_elements
    {
        return Ok(None);
    }
    if total_count > u32::MAX as usize {
        return Err(Error::InputError(format!(
            "too many list elements to encode: {}",
            total_count
        )));
    }
    let total_body_size =
        existing_body.len() + operand_bodies.iter().map(Bytes::len).sum::<usize>();
    let mut out = BytesMut::with_capacity(size_of::<u32>() + total_body_size);
    out.put_u32_le(total_count as u32);
    out.extend_from_slice(existing_body.as_ref());
    for body in operand_bodies {
        out.extend_from_slice(body.as_ref());
    }
    Ok(Some(out.freeze()))
}

// Fast-path helper: only decode header count and body slice.
// We intentionally skip full payload scan here to keep merge append path minimal.
fn parse_payload_body(payload: &Bytes) -> Result<(usize, Bytes)> {
    if payload.is_empty() {
        return Ok((0, Bytes::new()));
    }
    if payload.len() < size_of::<u32>() {
        return Err(Error::FileFormatError(
            "invalid list payload: missing element count".to_string(),
        ));
    }
    let mut header = payload.slice(..size_of::<u32>());
    let element_count = header.get_u32_le() as usize;
    Ok((element_count, payload.slice(size_of::<u32>()..)))
}

/// Collect `retain last N` result by traversing payloads from newest to oldest.
///
/// This allows merge to stop once N items are collected, skipping older payloads entirely.
fn collect_last_from_newest(
    existing_value: &Bytes,
    operands: &[Bytes],
    config: &ListConfig,
    now_seconds: u32,
    max_elements: usize,
) -> Result<(Vec<DecodedListElement>, bool)> {
    if max_elements == 0 {
        return Ok((Vec::new(), true));
    }
    let mut newest_to_oldest = Vec::with_capacity(max_elements);
    for payload in operands.iter().rev().chain(std::iter::once(existing_value)) {
        if newest_to_oldest.len() >= max_elements {
            break;
        }
        let needed = max_elements - newest_to_oldest.len();
        collect_last_from_single_payload(
            payload,
            config,
            now_seconds,
            needed,
            &mut newest_to_oldest,
        )?;
    }
    let reached_last_cap = newest_to_oldest.len() >= max_elements;
    newest_to_oldest.reverse();
    Ok((newest_to_oldest, reached_last_cap))
}

fn collect_last_from_single_payload(
    payload: &Bytes,
    config: &ListConfig,
    now_seconds: u32,
    needed: usize,
    out_newest_to_oldest: &mut Vec<DecodedListElement>,
) -> Result<()> {
    if needed == 0 {
        return Ok(());
    }
    let mut cursor = ListPayloadCursor::new(payload, config.preserve_element_ttl)?;
    let mut tail = VecDeque::with_capacity(needed);
    while let Some(element) = cursor.next()? {
        if config.preserve_element_ttl
            && element
                .expires_at_secs
                .is_some_and(|expires_at| expires_at <= now_seconds)
        {
            continue;
        }
        tail.push_back(element);
        if tail.len() > needed {
            let _ = tail.pop_front();
        }
    }
    while let Some(element) = tail.pop_back() {
        out_newest_to_oldest.push(element);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_list_round_trip() {
        let config = ListConfig {
            max_elements: Some(2),
            retain_mode: ListRetainMode::Last,
            preserve_element_ttl: false,
        };
        let encoded = encode_list_for_write(
            vec![Bytes::from_static(b"a"), Bytes::from_static(b"b")],
            &config,
            None,
            10,
        )
        .unwrap();
        let decoded = decode_list_for_read(&encoded, &config, 10).unwrap();
        assert_eq!(
            decoded,
            vec![Bytes::from_static(b"a"), Bytes::from_static(b"b")]
        );
    }

    #[test]
    fn test_list_ttl_uses_supplied_time() {
        let config = ListConfig {
            max_elements: None,
            retain_mode: ListRetainMode::Last,
            preserve_element_ttl: true,
        };
        let encoded =
            encode_list_for_write(vec![Bytes::from_static(b"a")], &config, Some(5), 100).unwrap();
        assert_eq!(
            decode_list_for_read(&encoded, &config, 104).unwrap(),
            vec![Bytes::from_static(b"a")]
        );
        assert!(
            decode_list_for_read(&encoded, &config, 105)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn test_merge_batch_fast_append_keeps_valid_payload() {
        let config = ListConfig {
            max_elements: Some(4),
            retain_mode: ListRetainMode::Last,
            preserve_element_ttl: false,
        };
        let operator = ListMergeOperator::new(config.clone());
        let left = encode_list_for_write(
            vec![Bytes::from_static(b"a"), Bytes::from_static(b"b")],
            &config,
            None,
            0,
        )
        .unwrap();
        let right =
            encode_list_for_write(vec![Bytes::from_static(b"c")], &config, None, 0).unwrap();
        let merged = operator.merge_batch(left, vec![right], None).unwrap();
        assert_eq!(
            decode_list_for_read(&merged.0, &config, 0).unwrap(),
            vec![
                Bytes::from_static(b"a"),
                Bytes::from_static(b"b"),
                Bytes::from_static(b"c"),
            ]
        );
    }

    #[test]
    fn test_merge_over_cap_falls_back_to_retain_policy() {
        let config = ListConfig {
            max_elements: Some(2),
            retain_mode: ListRetainMode::Last,
            preserve_element_ttl: false,
        };
        let operator = ListMergeOperator::new(config.clone());
        let left = encode_list_for_write(vec![Bytes::from_static(b"a")], &config, None, 0).unwrap();
        let right = encode_list_for_write(
            vec![Bytes::from_static(b"b"), Bytes::from_static(b"c")],
            &config,
            None,
            0,
        )
        .unwrap();
        let merged = operator.merge_batch(left, vec![right], None).unwrap();
        assert_eq!(
            decode_list_for_read(&merged.0, &config, 0).unwrap(),
            vec![Bytes::from_static(b"b"), Bytes::from_static(b"c")]
        );
    }

    #[test]
    fn test_merge_last_cap_returns_put_value_type() {
        let config = ListConfig {
            max_elements: Some(2),
            retain_mode: ListRetainMode::Last,
            preserve_element_ttl: false,
        };
        let operator = ListMergeOperator::new(config.clone());
        let left = encode_list_for_write(vec![Bytes::from_static(b"a")], &config, None, 0).unwrap();
        let right = encode_list_for_write(
            vec![Bytes::from_static(b"b"), Bytes::from_static(b"c")],
            &config,
            None,
            0,
        )
        .unwrap();
        let merged = operator.merge_batch(left, vec![right], None).unwrap();
        assert_eq!(merged.1, Some(ValueType::Put));
    }

    #[test]
    fn test_merge_last_stops_before_older_payloads() {
        let config = ListConfig {
            max_elements: Some(2),
            retain_mode: ListRetainMode::Last,
            preserve_element_ttl: true,
        };
        let operator = ListMergeOperator::new(config.clone());
        // Older payload is malformed; merge should still succeed because retain-last stops after
        // consuming enough elements from newer operands.
        let malformed_existing = Bytes::from_static(b"\x01");
        let op1 = encode_list_for_write(vec![Bytes::from_static(b"a")], &config, None, 0).unwrap();
        let op2 = encode_list_for_write(vec![Bytes::from_static(b"b")], &config, None, 0).unwrap();
        let merged = operator
            .merge_batch(malformed_existing, vec![op1, op2], None)
            .unwrap();
        assert_eq!(
            decode_list_for_read(&merged.0, &config, 0).unwrap(),
            vec![Bytes::from_static(b"a"), Bytes::from_static(b"b")]
        );
    }

    #[test]
    fn test_merge_first_cap_stops_and_keeps_merge_type() {
        let config = ListConfig {
            max_elements: Some(2),
            retain_mode: ListRetainMode::First,
            preserve_element_ttl: false,
        };
        let operator = ListMergeOperator::new(config.clone());
        let left = encode_list_for_write(vec![Bytes::from_static(b"a")], &config, None, 0).unwrap();
        let right = encode_list_for_write(
            vec![
                Bytes::from_static(b"b"),
                Bytes::from_static(b"c"),
                Bytes::from_static(b"d"),
            ],
            &config,
            None,
            0,
        )
        .unwrap();
        let merged = operator.merge_batch(left, vec![right], None).unwrap();
        assert_eq!(merged.1, None);
        assert_eq!(
            decode_list_for_read(&merged.0, &config, 0).unwrap(),
            vec![Bytes::from_static(b"a"), Bytes::from_static(b"b")]
        );
    }

    #[test]
    fn test_list_operator_from_metadata_requires_metadata() {
        assert!(list_operator_from_metadata(LIST_OPERATOR_ID, None).is_none());
    }
}
