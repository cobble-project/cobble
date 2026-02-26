use crate::error::{Error, Result};
use arc_swap::ArcSwap;
use bytes::{Bytes, BytesMut};
use std::sync::{Arc, LazyLock};

/// User-defined merge semantics for a column.
pub trait MergeOperator: Send + Sync {
    /// Stable operator identifier used by remote compaction capability matching.
    fn id(&self) -> String {
        std::any::type_name::<Self>().to_string()
    }

    /// Merge a single operand into the current value.
    fn merge(&self, existing_value: Bytes, value: Bytes) -> Result<Bytes>;

    /// Merge multiple operands into the current value.
    fn merge_batch(&self, existing_value: Bytes, operands: Vec<Bytes>) -> Result<Bytes> {
        let mut merged = existing_value;
        for operand in operands {
            merged = self.merge(merged, operand)?;
        }
        Ok(merged)
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

pub(crate) fn default_merge_operator() -> Arc<dyn MergeOperator> {
    Arc::clone(&DEFAULT_MERGE_OPERATOR)
}

pub(crate) fn default_merge_operator_ref() -> &'static Arc<dyn MergeOperator> {
    &DEFAULT_MERGE_OPERATOR
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
    fn merge(&self, existing_value: Bytes, value: Bytes) -> Result<Bytes> {
        let existing = existing_value;
        if existing.is_empty() {
            return Ok(value);
        }
        if value.is_empty() {
            return Ok(existing);
        }
        let mut merged = BytesMut::with_capacity(existing.len() + value.len());
        merged.extend_from_slice(existing.as_ref());
        merged.extend_from_slice(value.as_ref());
        Ok(merged.freeze())
    }

    fn merge_batch(&self, existing_value: Bytes, operands: Vec<Bytes>) -> Result<Bytes> {
        if operands.is_empty() {
            return Ok(existing_value);
        }
        if existing_value.is_empty() && operands.len() == 1 {
            return Ok(operands.into_iter().next().expect("len checked"));
        }
        let total_len = existing_value.len() + operands.iter().map(Bytes::len).sum::<usize>();
        let mut merged = BytesMut::with_capacity(total_len);
        merged.extend_from_slice(existing_value.as_ref());
        for operand in operands {
            merged.extend_from_slice(operand.as_ref());
        }
        Ok(merged.freeze())
    }
}

impl MergeOperator for U32CounterMergeOperator {
    fn merge(&self, existing_value: Bytes, value: Bytes) -> Result<Bytes> {
        let existing = decode_u32_counter(&existing_value, "existing_value")?;
        let delta = decode_u32_counter(&value, "value")?;
        let merged = existing.checked_add(delta).ok_or_else(|| {
            Error::InputError(format!(
                "U32CounterMergeOperator overflow: {} + {}",
                existing, delta
            ))
        })?;
        Ok(Bytes::copy_from_slice(&merged.to_le_bytes()))
    }

    fn merge_batch(&self, existing_value: Bytes, operands: Vec<Bytes>) -> Result<Bytes> {
        let mut merged = decode_u32_counter(&existing_value, "existing_value")?;
        for operand in operands {
            let delta = decode_u32_counter(&operand, "operand")?;
            merged = merged.checked_add(delta).ok_or_else(|| {
                Error::InputError(format!("U32CounterMergeOperator overflow: {}", merged))
            })?;
        }
        Ok(Bytes::copy_from_slice(&merged.to_le_bytes()))
    }
}

impl MergeOperator for U64CounterMergeOperator {
    fn merge(&self, existing_value: Bytes, value: Bytes) -> Result<Bytes> {
        let existing = decode_u64_counter(&existing_value, "existing_value")?;
        let delta = decode_u64_counter(&value, "value")?;
        let merged = existing.checked_add(delta).ok_or_else(|| {
            Error::InputError(format!(
                "U64CounterMergeOperator overflow: {} + {}",
                existing, delta
            ))
        })?;
        Ok(Bytes::copy_from_slice(&merged.to_le_bytes()))
    }

    fn merge_batch(&self, existing_value: Bytes, operands: Vec<Bytes>) -> Result<Bytes> {
        let mut merged = decode_u64_counter(&existing_value, "existing_value")?;
        for operand in operands {
            let delta = decode_u64_counter(&operand, "operand")?;
            merged = merged.checked_add(delta).ok_or_else(|| {
                Error::InputError(format!("U64CounterMergeOperator overflow: {}", merged))
            })?;
        }
        Ok(Bytes::copy_from_slice(&merged.to_le_bytes()))
    }
}

/// Holds the merge operators for all columns, allowing retrieval by column index.
#[derive(Clone)]
pub(crate) struct ValueMergeOperator {
    operators: Arc<Vec<Arc<dyn MergeOperator>>>,
}

impl ValueMergeOperator {
    pub(crate) fn new(operators: Arc<Vec<Arc<dyn MergeOperator>>>) -> Self {
        Self { operators }
    }

    pub(crate) fn operator(&self, column_idx: usize) -> &dyn MergeOperator {
        self.operators
            .get(column_idx)
            .unwrap_or_else(|| default_merge_operator_ref())
            .as_ref()
    }

    pub(crate) fn operator_ids(&self, num_columns: usize) -> Vec<String> {
        (0..num_columns)
            .map(|column_idx| self.operator(column_idx).id())
            .collect()
    }

    pub(crate) fn empty() -> Arc<Self> {
        Arc::new(Self {
            operators: Arc::new(vec![]),
        })
    }
}

/// Registry for managing merge operators for all columns, allowing dynamic updates.
#[derive(Clone)]
pub(crate) struct MergeOperatorRegistry {
    operators: Arc<ArcSwap<ValueMergeOperator>>,
}

impl MergeOperatorRegistry {
    pub(crate) fn new(num_columns: usize) -> Self {
        let default_op = default_merge_operator();
        Self {
            operators: Arc::new(ArcSwap::from_pointee(ValueMergeOperator::new(Arc::new(
                vec![default_op; num_columns],
            )))),
        }
    }

    pub(crate) fn set_column_operator(
        &self,
        column_idx: usize,
        operator: Arc<dyn MergeOperator>,
    ) -> Result<()> {
        let current = self.operators.load_full();
        let mut updated = current.operators.as_ref().clone();
        if updated.is_empty() && column_idx > 0 {
            let mut updated = vec![default_merge_operator(); column_idx + 1];
            updated[column_idx] = operator;
            self.operators
                .store(Arc::new(ValueMergeOperator::new(Arc::new(updated))));
            return Ok(());
        }
        if column_idx >= updated.len() {
            updated.resize_with(column_idx + 1, default_merge_operator);
        }
        updated[column_idx] = operator;
        self.operators
            .store(Arc::new(ValueMergeOperator::new(Arc::new(updated))));
        Ok(())
    }

    pub(crate) fn set_all(&self, operators: Vec<Arc<dyn MergeOperator>>) -> Result<()> {
        self.operators
            .store(Arc::new(ValueMergeOperator::new(Arc::new(operators))));
        Ok(())
    }

    pub(crate) fn value_merge_operators(&self) -> Arc<ValueMergeOperator> {
        self.operators.load_full()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct BracketMergeOperator;

    impl MergeOperator for BracketMergeOperator {
        fn merge(&self, existing_value: Bytes, value: Bytes) -> Result<Bytes> {
            let existing = existing_value.as_ref();
            Ok(format!(
                "[{}+{}]",
                String::from_utf8_lossy(existing),
                String::from_utf8_lossy(value.as_ref())
            )
            .into_bytes()
            .into())
        }
    }

    #[test]
    fn test_bytes_merge_operator_merge() {
        let op = BytesMergeOperator;
        let merged = op
            .merge(Bytes::from_static(b"old"), Bytes::from_static(b"new"))
            .unwrap();
        assert_eq!(merged.as_ref(), b"oldnew");
    }

    #[test]
    fn test_bytes_merge_operator_merge_batch() {
        let op = BytesMergeOperator;
        let merged = op
            .merge_batch(
                Bytes::from_static(b"base"),
                vec![
                    Bytes::from_static(b"a"),
                    Bytes::from_static(b"b"),
                    Bytes::from_static(b"c"),
                ],
            )
            .unwrap();
        assert_eq!(merged.as_ref(), b"baseabc");
    }

    #[test]
    fn test_merge_operator_default_batch_uses_merge() {
        let op = BracketMergeOperator;
        let merged = op
            .merge_batch(
                Bytes::from_static(b"base"),
                vec![Bytes::from_static(b"a"), Bytes::from_static(b"b")],
            )
            .unwrap();
        assert_eq!(merged.as_ref(), b"[[base+a]+b]");
    }

    #[test]
    fn test_registry_set_column_operator() {
        let registry = MergeOperatorRegistry::new(2);
        registry
            .set_column_operator(1, Arc::new(BracketMergeOperator))
            .unwrap();
        let merge_ops = registry.value_merge_operators();
        let op = merge_ops.operator(1);
        let merged = op
            .merge(Bytes::from_static(b"x"), Bytes::from_static(b"y"))
            .unwrap();
        assert_eq!(merged.as_ref(), b"[x+y]");
    }

    #[test]
    fn test_registry_falls_back_to_default_for_missing_column() {
        let registry = MergeOperatorRegistry::new(1);
        let merge_ops = registry.value_merge_operators();
        let op = merge_ops.operator(10);
        let merged = op
            .merge(Bytes::from_static(b"a"), Bytes::from_static(b"b"))
            .unwrap();
        assert_eq!(merged.as_ref(), b"ab");
    }

    #[test]
    fn test_registry_set_column_operator_extends_with_defaults() {
        let registry = MergeOperatorRegistry::new(1);
        registry
            .set_column_operator(3, Arc::new(BracketMergeOperator))
            .unwrap();
        let merge_ops = registry.value_merge_operators();
        let merged0 = merge_ops
            .operator(0)
            .merge(Bytes::from_static(b"a"), Bytes::from_static(b"b"))
            .unwrap();
        let merged3 = merge_ops
            .operator(3)
            .merge(Bytes::from_static(b"a"), Bytes::from_static(b"b"))
            .unwrap();
        assert_eq!(merged0.as_ref(), b"ab");
        assert_eq!(merged3.as_ref(), b"[a+b]");
    }

    #[test]
    fn test_u32_counter_merge_operator() {
        let op = U32CounterMergeOperator;
        let merged = op
            .merge(
                Bytes::copy_from_slice(&3u32.to_le_bytes()),
                Bytes::copy_from_slice(&4u32.to_le_bytes()),
            )
            .unwrap();
        let value = u32::from_le_bytes(merged.as_ref().try_into().unwrap());
        assert_eq!(value, 7);
    }

    #[test]
    fn test_u64_counter_merge_operator() {
        let op = U64CounterMergeOperator;
        let merged = op
            .merge_batch(
                Bytes::copy_from_slice(&1u64.to_le_bytes()),
                vec![
                    Bytes::copy_from_slice(&2u64.to_le_bytes()),
                    Bytes::copy_from_slice(&3u64.to_le_bytes()),
                ],
            )
            .unwrap();
        let value = u64::from_le_bytes(merged.as_ref().try_into().unwrap());
        assert_eq!(value, 6);
    }
}
