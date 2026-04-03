---
title: Merge Operators
parent: Architecture
nav_order: 11
---

# Merge Operators

Many applications need to update a value based on its current state — incrementing a counter, appending to a list, or updating a field in a record. The naive approach is read-then-write: read the current value, modify it, write it back. This requires a full round trip and doesn't compose well under concurrent writes.

Cobble's **merge operators** eliminate this problem. Instead of reading the current value, you write a *merge operand* — a description of the change you want to make. Cobble defers the actual merge until the value is read or [compacted](compaction), combining all accumulated operands efficiently.

## How Merges Are Resolved

When you read a key that has merge operands, Cobble finds all versions of that key across [memtable](memtable) and [LSM tree](lsm-tree) levels and applies the merge operator to combine them. The operator is called in chronological order, accumulating the result:

- If a `Put` is encountered, it becomes the base value.
- If a `Delete` is encountered, it terminates the chain (the key is deleted).
- Each `Merge` operand is folded into the accumulated value using the operator.

During [compaction](compaction), merge chains are resolved permanently — the result is written as a single `Put` value, so future reads no longer need to traverse the chain. This means merge resolution cost is amortized over time.

> **Limitation**: The merge operator should be **associative** to ensure correct results regardless of operand order. Cobble does not guarantee the order of merge operand, so non-associative operators may produce inconsistent results.
> 
> e.g. a "sum" operator is fine, a + b + c == (a + b) + c == a + (b + c).

## Built-in Operators

Cobble includes several operators for common patterns:

**BytesMergeOperator** (default) — Concatenates operands. Useful for building up strings or binary blobs incrementally.

**U32CounterMergeOperator** — Treats values as little-endian u32 integers and sums them. Perfect for counters, scores, or any additive metric.

**U64CounterMergeOperator** — Same as above, but for u64 values. Use this for larger counters or when u32 range is insufficient.

## Custom Operators

For application-specific merge semantics, implement the `MergeOperator` trait:

```rust
pub trait MergeOperator: Send + Sync {
    fn id(&self) -> String;
    fn metadata(&self) -> Option<JsonValue>;
    fn merge(
        &self,
        existing_value: Bytes,
        operand: Bytes,
        time_provider: Option<&dyn TimeProvider>,
    ) -> Result<(Bytes, Option<ValueType>)>;
}
```

The return value includes an optional `ValueType` that can **terminalize** the merge chain — returning `Some(ValueType::Put)` means "this is the final value, stop merging." This is useful for operators that can determine when further merges are unnecessary.

Custom operators are registered with a unique string ID that is persisted in the [schema](schema-evolution). This ensures the correct operator is used even after restarts or restores from [snapshots](snapshot).

## Per-Column Operators

Since Cobble supports [multi-column values](schema-evolution#multi-column-values), each column can have its own merge operator. For example, you might use a counter operator for column 0 (a view count) and a list-append operator for column 1 (a tag list). When you call `merge()` for a specific column, only that column's operator is invoked.

## Interaction with Value Separation

Merge operators work transparently with [key-value separation](key-value-separation). When merge operands are stored in VLOG files, Cobble dereferences the pointers before passing actual values to the operator. The result may be re-separated if it exceeds the threshold. Lazy merge chains (`MergeSeparatedArray`) further optimize this by deferring resolution until the value is actually needed.
