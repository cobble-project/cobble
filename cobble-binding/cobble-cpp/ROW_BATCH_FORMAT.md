# Caller-owned row batch format

`ScanCursor::NextBatchInto` writes a compact, versioned byte stream into the
caller's buffer. Integers are little-endian and fields are packed; consumers
must not use aligned pointer casts. Read integers with `memcpy` or equivalent.

## Version 1

The 24-byte batch header is:

| Offset | Type | Meaning |
| ---: | --- | --- |
| 0 | `byte[4]` | ASCII magic `CBRB` |
| 4 | `u16` | format version (`1`) |
| 6 | `u16` | header size (`24`) |
| 8 | `u32` | flags: bit 0 `end`, bit 1 `block boundary` |
| 12 | `u32` | row count |
| 16 | `u64` | total encoded size, including this header |

Each row follows immediately:

| Type | Meaning |
| --- | --- |
| `u16` | bucket id |
| `u16` | row flags; reserved in version 1 and written as zero |
| `u32` | key length |
| `u32` | column count |
| `byte[key length]` | key payload |

Each column then contains a `u64` length followed by that many payload bytes.
The sentinel `UINT64_MAX` represents an absent column and has no payload.

Rows and columns are contiguous and have no padding. Unknown header flag bits
and nonzero row flags must be ignored by version-1 readers so compatible flags
can be introduced later.

If a caller buffer is too small, `NextBatchInto` returns
`BufferStatus::kBufferTooSmall` and the exact `bytes_required`. It retains the
pending rows, so a retry with a sufficiently large buffer produces the same
batch without skipping scan results.

## Structured version 1

Structured point reads, multi-get, and scans use a separate `CSRB` version-1
stream. It has the same 24-byte header layout, with magic `CSRB`. Each row has
a fixed 16-byte header followed by its key:

| Offset | Type | Meaning |
| ---: | --- | --- |
| 0 | `u16` | bucket id |
| 2 | `u16` | flags: bit 0 means the row was found |
| 4 | `u32` | key length |
| 8 | `u32` | projected column count |
| 12 | `u32` | reserved, zero in version 1 |

Each projected column then has a 16-byte header: a `u8` tag (`0` null, `1`
BYTES, `2` LIST), `u8` flags, `u16` reserved, `u32` LIST element count, and
`u64` payload size. BYTES payload follows directly. LIST payload contains a
little-endian `u64` length followed by bytes for each element. This preserves
null versus empty BYTES versus empty LIST without sentinel values.

All sizes and additions are checked. A too-small caller buffer remains entirely
unchanged. Structured scan cursors retain that exact pending batch until a
successful retry, so no row is skipped or repeated.

Structured priority-queue point and batch caller-buffer reads encode the queue
key plus one BYTES column in this same `CSRB` format. Peek retries do not change
the queue cursor. Poll performs complete metadata and capacity preflight first;
if the buffer is too small, the output and queue are unchanged. A retry must
match the operation, bucket, and optional limit. With sufficient capacity,
poll advances the truncation cursor before writing the already-validated
encoding, so an advance failure cannot expose a successfully polled result.
`limit = 0` returns an empty batch and never advances the cursor, while an
absent limit requests one physical-boundary batch.
