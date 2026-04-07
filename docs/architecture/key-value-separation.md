---
title: Key-Value Separation
parent: Architecture
nav_order: 4
---

# Key-Value Separation (VLOG)

In a standard LSM tree, every value is stored alongside its key in the SST files and gets rewritten each time [compaction](compaction) moves data between levels. For workloads with large values (images, documents, serialized objects), this causes significant **write amplification** — each piece of data might be rewritten 10–30 times as it flows through the LSM levels.

Key-value separation addresses this by storing large values in a dedicated **Value Log (VLOG)** file. The [LSM tree](lsm-tree) stores only small 8-byte pointers, so compaction becomes much cheaper — it only moves tiny pointers instead of the full values.

## When to Use Value Separation

Value separation is controlled by `value_separation_threshold`. Any value larger than this threshold is stored in the VLOG; smaller values remain inline in the SST.

```rust
use size::Size;

config.value_separation_threshold = Some(Size::from_const(1024)); // separate values > 1 KB
```

Set `value_separation_threshold` to `None` to disable separation. Enable it when:

- Your values are consistently **larger than 1–4 KB**.
- **Write throughput** is more important than single-read latency.
- You want to reduce the I/O cost of [compaction](compaction).

Keep it disabled when values are small (tens to hundreds of bytes), since the 8-byte pointer overhead and extra VLOG read per access would be counterproductive.

## How It Works

During [memtable](memtable) flush, Cobble examines each value. If it exceeds the threshold, the value is appended to a VLOG file and replaced with a pointer containing the file sequence number and byte offset. The SST file stores only these lightweight pointers.

On read, when Cobble encounters a separated value, it transparently dereferences the pointer by seeking to the correct position in the VLOG file. This extra I/O is the trade-off: reads may need one additional random access, but compaction moves far less data.

## Interaction with Merge Operators

[Merge operators](merge-operator) work transparently with separated values. When a merge chain involves separated operands, Cobble dereferences the VLOG pointers before passing the actual values to the merge operator. The result may be re-separated if it still exceeds the threshold.

For efficiency, Cobble supports **lazy merge chains** — during compaction, multiple separated merge operands can be grouped without immediately resolving them, deferring the actual merge until the value is read. This avoids unnecessary VLOG I/O during compaction when the merged result may never be read.

## Trade-offs

| Aspect | With separation | Without separation |
|--------|----------------|-------------------|
| Write amplification | Lower — only pointers are compacted | Higher — full values rewritten per level |
| Point read latency | Slightly higher — extra VLOG dereference | Lower — value inline in SST |
| Scan performance | May need random VLOG I/O | Sequential SST reads |
| Space overhead | 8-byte pointer per entry | None |
| Best for | Large values (>1 KB), write-heavy | Small values, read-heavy |

## VLOG File Lifecycle

VLOG files are created during each [memtable](memtable) flush (one per flush) and tracked by the [file management](file-management) layer. A VLOG file is eligible for cleanup once all SST entries pointing into it have been compacted away — meaning no live [snapshot](snapshot) or LSM tree version references any pointer within that file.
