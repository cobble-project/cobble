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
