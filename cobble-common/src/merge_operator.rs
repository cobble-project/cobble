use crate::error::{Error, Result};
use crate::time::TimeProvider;
use crate::r#type::ValueType;
use bytes::{Bytes, BytesMut};
use serde_json::Value as JsonValue;
use std::sync::{Arc, LazyLock};

/// User-defined merge semantics for a column.
pub trait MergeOperator: Send + Sync {
    /// Stable operator identifier used by remote compaction capability matching.
    fn id(&self) -> String {
        std::any::type_name::<Self>().to_string()
    }

    /// Optional serializable metadata for this merge operator.
    fn metadata(&self) -> Option<JsonValue> {
        None
    }

    /// Merge a single operand into the current value.
    ///
    /// Returns `(merged_bytes, override_value_type)`.
    /// `override_value_type` is reserved for terminalization and should only return
    /// `Some(ValueType::Put)` or `Some(ValueType::Delete)`.
    fn merge(
        &self,
        existing_value: Bytes,
        value: Bytes,
        time_provider: Option<&dyn TimeProvider>,
    ) -> Result<(Bytes, Option<ValueType>)>;

    /// Merge multiple operands into the current value.
    fn merge_batch(
        &self,
        existing_value: Bytes,
        operands: Vec<Bytes>,
        time_provider: Option<&dyn TimeProvider>,
    ) -> Result<(Bytes, Option<ValueType>)> {
        let mut merged = existing_value;
        let mut merged_type = None;
        for operand in operands {
            let (output, override_type) = self.merge(merged, operand, time_provider)?;
            merged = output;
            merged_type = override_type;
        }
        Ok((merged, merged_type))
    }
}

/// Default merge operator: byte concatenation.
#[derive(Default)]
pub struct BytesMergeOperator;

/// Built-in merge operator: sums little-endian `u32` values.
#[derive(Default)]
pub struct U32CounterMergeOperator;

/// Built-in merge operator: sums little-endian `u64` values.
#[derive(Default)]
pub struct U64CounterMergeOperator;

static DEFAULT_MERGE_OPERATOR: LazyLock<Arc<dyn MergeOperator>> =
    LazyLock::new(|| Arc::new(BytesMergeOperator));

/// Resolver for looking up merge operators by ID, used for custom merge operators and remote compaction.
pub trait MergeOperatorResolver: Send + Sync {
    fn resolve(&self, id: &str, metadata: Option<&JsonValue>) -> Option<Arc<dyn MergeOperator>>;
}

/// Blanket implementation to allow using a simple closure as a `MergeOperatorResolver`.
impl<F> MergeOperatorResolver for F
where
    F: Fn(&str, Option<&JsonValue>) -> Option<Arc<dyn MergeOperator>> + Send + Sync,
{
    fn resolve(&self, id: &str, metadata: Option<&JsonValue>) -> Option<Arc<dyn MergeOperator>> {
        self(id, metadata)
    }
}

pub(crate) fn default_merge_operator() -> Arc<dyn MergeOperator> {
    Arc::clone(&DEFAULT_MERGE_OPERATOR)
}

pub(crate) fn default_merge_operator_ref() -> &'static Arc<dyn MergeOperator> {
    &DEFAULT_MERGE_OPERATOR
}

pub(crate) fn merge_operator_by_id(
    id: &str,
    metadata: Option<&JsonValue>,
    resolver: Option<&Arc<dyn MergeOperatorResolver>>,
) -> Result<Arc<dyn MergeOperator>> {
    if id == BytesMergeOperator.id().as_str() {
        Ok(Arc::new(BytesMergeOperator))
    } else if id == U32CounterMergeOperator.id().as_str() {
        Ok(Arc::new(U32CounterMergeOperator))
    } else if id == U64CounterMergeOperator.id().as_str() {
        Ok(Arc::new(U64CounterMergeOperator))
    } else {
        if let Some(resolver) = resolver
            && let Some(operator) = resolver.resolve(id, metadata)
        {
            return Ok(operator);
        }
        Err(Error::FileFormatError(format!(
            "Unknown merge operator id '{}' and no resolver could resolve it",
            id
        )))
    }
}

fn decode_u32_counter(value: &Bytes, label: &str) -> Result<u32> {
    if value.is_empty() {
        return Ok(0);
    }
    if value.len() != std::mem::size_of::<u32>() {
        return Err(Error::InputError(format!(
            "U32CounterMergeOperator expects {} bytes for {}, got {}",
            std::mem::size_of::<u32>(),
            label,
            value.len()
        )));
    }
    let bytes: [u8; std::mem::size_of::<u32>()] =
        value.as_ref().try_into().expect("length checked");
    Ok(u32::from_le_bytes(bytes))
}

fn decode_u64_counter(value: &Bytes, label: &str) -> Result<u64> {
    if value.is_empty() {
        return Ok(0);
    }
    if value.len() != std::mem::size_of::<u64>() {
        return Err(Error::InputError(format!(
            "U64CounterMergeOperator expects {} bytes for {}, got {}",
            std::mem::size_of::<u64>(),
            label,
            value.len()
        )));
    }
    let bytes: [u8; std::mem::size_of::<u64>()] =
        value.as_ref().try_into().expect("length checked");
    Ok(u64::from_le_bytes(bytes))
}

impl MergeOperator for BytesMergeOperator {
    fn merge(
        &self,
        existing_value: Bytes,
        value: Bytes,
        _time_provider: Option<&dyn TimeProvider>,
    ) -> Result<(Bytes, Option<ValueType>)> {
        let existing = existing_value;
        if existing.is_empty() {
            return Ok((value, None));
        }
        if value.is_empty() {
            return Ok((existing, None));
        }
        let mut merged = BytesMut::with_capacity(existing.len() + value.len());
        merged.extend_from_slice(existing.as_ref());
        merged.extend_from_slice(value.as_ref());
        Ok((merged.freeze(), None))
    }

    fn merge_batch(
        &self,
        existing_value: Bytes,
        operands: Vec<Bytes>,
        _time_provider: Option<&dyn TimeProvider>,
    ) -> Result<(Bytes, Option<ValueType>)> {
        if operands.is_empty() {
            return Ok((existing_value, None));
        }
        if existing_value.is_empty() && operands.len() == 1 {
            return Ok((operands.into_iter().next().expect("len checked"), None));
        }
        let total_len = existing_value.len() + operands.iter().map(Bytes::len).sum::<usize>();
        let mut merged = BytesMut::with_capacity(total_len);
        merged.extend_from_slice(existing_value.as_ref());
        for operand in operands {
            merged.extend_from_slice(operand.as_ref());
        }
        Ok((merged.freeze(), None))
    }
}

impl MergeOperator for U32CounterMergeOperator {
    fn merge(
        &self,
        existing_value: Bytes,
        value: Bytes,
        _time_provider: Option<&dyn TimeProvider>,
    ) -> Result<(Bytes, Option<ValueType>)> {
        let existing = decode_u32_counter(&existing_value, "existing_value")?;
        let delta = decode_u32_counter(&value, "value")?;
        let merged = existing.checked_add(delta).ok_or_else(|| {
            Error::InputError(format!(
                "U32CounterMergeOperator overflow: {} + {}",
                existing, delta
            ))
        })?;
        Ok((Bytes::copy_from_slice(&merged.to_le_bytes()), None))
    }

    fn merge_batch(
        &self,
        existing_value: Bytes,
        operands: Vec<Bytes>,
        _time_provider: Option<&dyn TimeProvider>,
    ) -> Result<(Bytes, Option<ValueType>)> {
        let mut merged = decode_u32_counter(&existing_value, "existing_value")?;
        for operand in operands {
            let delta = decode_u32_counter(&operand, "operand")?;
            merged = merged.checked_add(delta).ok_or_else(|| {
                Error::InputError(format!("U32CounterMergeOperator overflow: {}", merged))
            })?;
        }
        Ok((Bytes::copy_from_slice(&merged.to_le_bytes()), None))
    }
}

impl MergeOperator for U64CounterMergeOperator {
    fn merge(
        &self,
        existing_value: Bytes,
        value: Bytes,
        _time_provider: Option<&dyn TimeProvider>,
    ) -> Result<(Bytes, Option<ValueType>)> {
        let existing = decode_u64_counter(&existing_value, "existing_value")?;
        let delta = decode_u64_counter(&value, "value")?;
        let merged = existing.checked_add(delta).ok_or_else(|| {
            Error::InputError(format!(
                "U64CounterMergeOperator overflow: {} + {}",
                existing, delta
            ))
        })?;
        Ok((Bytes::copy_from_slice(&merged.to_le_bytes()), None))
    }

    fn merge_batch(
        &self,
        existing_value: Bytes,
        operands: Vec<Bytes>,
        _time_provider: Option<&dyn TimeProvider>,
    ) -> Result<(Bytes, Option<ValueType>)> {
        let mut merged = decode_u64_counter(&existing_value, "existing_value")?;
        for operand in operands {
            let delta = decode_u64_counter(&operand, "operand")?;
            merged = merged.checked_add(delta).ok_or_else(|| {
                Error::InputError(format!("U64CounterMergeOperator overflow: {}", merged))
            })?;
        }
        Ok((Bytes::copy_from_slice(&merged.to_le_bytes()), None))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct BracketMergeOperator;

    impl MergeOperator for BracketMergeOperator {
        fn merge(
            &self,
            existing_value: Bytes,
            value: Bytes,
            _time_provider: Option<&dyn TimeProvider>,
        ) -> Result<(Bytes, Option<ValueType>)> {
            let existing = existing_value.as_ref();
            Ok((
                format!(
                    "[{}+{}]",
                    String::from_utf8_lossy(existing),
                    String::from_utf8_lossy(value.as_ref())
                )
                .into_bytes()
                .into(),
                None,
            ))
        }
    }

    #[test]
    fn test_bytes_merge_operator_merge() {
        let op = BytesMergeOperator;
        let (merged, _) = op
            .merge(Bytes::from_static(b"old"), Bytes::from_static(b"new"), None)
            .unwrap();
        assert_eq!(merged.as_ref(), b"oldnew");
    }

    #[test]
    fn test_bytes_merge_operator_merge_batch() {
        let op = BytesMergeOperator;
        let (merged, _) = op
            .merge_batch(
                Bytes::from_static(b"base"),
                vec![
                    Bytes::from_static(b"a"),
                    Bytes::from_static(b"b"),
                    Bytes::from_static(b"c"),
                ],
                None,
            )
            .unwrap();
        assert_eq!(merged.as_ref(), b"baseabc");
    }

    #[test]
    fn test_merge_operator_default_batch_uses_merge() {
        let op = BracketMergeOperator;
        let (merged, _) = op
            .merge_batch(
                Bytes::from_static(b"base"),
                vec![Bytes::from_static(b"a"), Bytes::from_static(b"b")],
                None,
            )
            .unwrap();
        assert_eq!(merged.as_ref(), b"[[base+a]+b]");
    }

    #[test]
    fn test_u32_counter_merge_operator() {
        let op = U32CounterMergeOperator;
        let (merged, _) = op
            .merge(
                Bytes::copy_from_slice(&3u32.to_le_bytes()),
                Bytes::copy_from_slice(&4u32.to_le_bytes()),
                None,
            )
            .unwrap();
        let value = u32::from_le_bytes(merged.as_ref().try_into().unwrap());
        assert_eq!(value, 7);
    }

    #[test]
    fn test_u64_counter_merge_operator() {
        let op = U64CounterMergeOperator;
        let (merged, _) = op
            .merge_batch(
                Bytes::copy_from_slice(&1u64.to_le_bytes()),
                vec![
                    Bytes::copy_from_slice(&2u64.to_le_bytes()),
                    Bytes::copy_from_slice(&3u64.to_le_bytes()),
                ],
                None,
            )
            .unwrap();
        let value = u64::from_le_bytes(merged.as_ref().try_into().unwrap());
        assert_eq!(value, 6);
    }
}
