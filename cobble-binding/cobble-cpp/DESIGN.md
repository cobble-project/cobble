# Cobble C++ binding design

## Scope

The C++ API wraps both `SingleDb` and the sharded `Db` raw bucket/key/column
data model. It includes database open/resume/restore, point and multi-key
reads, writes, write batches, range scans, raw schema evolution, metrics,
runtime lifecycle controls, typed global and shard snapshot operations, active
snapshot switching, bucket rescaling, current and pinned multi-shard readers,
exact-snapshot read-only databases, coordinator operations, and distributed
scan planning. Table codecs and structured Table schema APIs are intentionally
outside this binding.

The public C++ header does not expose generated `cxx` types or Rust storage
layout. This leaves room to evolve the Rust bridge without changing every C++
consumer and keeps the public API usable from ordinary CMake projects.

## Source layout

`cobble.hpp` is a compatibility umbrella over the standalone public headers:
`types.hpp`, `options.hpp`, `write_batch.hpp`, `scan.hpp`, `multi_get.hpp`,
`schema.hpp`, `snapshot.hpp`, `metrics.hpp`, `lifecycle.hpp`, `rescale.hpp`,
`single_db.hpp`, `database.hpp`, `db.hpp`, `read_only_db.hpp`, `reader.hpp`,
`coordinator.hpp`, and `scan_plan.hpp`.
The C++ implementation is split by the same responsibilities, with private
bridge, conversion, error-translation, and PImpl declarations under
`cpp/detail`. Rust keeps the `cxx` declaration in `src/lib.rs` so the generated
header and `cobble::ffi` namespace stay stable; database, write-batch, scan,
multi-get, snapshot, schema, metrics, lifecycle, read-only, reader,
coordinator, distributed-scan, options, encoding, and error behavior live in
focused private modules.

This is a source-organization boundary only: neither the C++ public ABI nor
the CBRB caller-buffer format changes.

## Ownership and data movement

The binding provides two complementary read paths:

- Owned results keep Rust `bytes::Bytes` values alive behind a move-only C++
  RAII object. Column and key access returns non-owning C++ views into those
  allocations, with no Rust-to-C++ payload copy.
- `GetColumnInto` and `NextBatchInto` write into a caller-owned reusable buffer. This
  performs the unavoidable materialization copy but does not allocate a payload
  buffer on the Rust side.

`MultiGet` builds a compact descriptor array and makes one synchronous bridge
call. Descriptors borrow the original C++ spans; the Rust side validates count,
alignment, address arithmetic, null pointers, and slice length limits before
creating temporary slices. Returned payloads remain Rust-owned and are exposed
through `OwnedMultiGetResult` without a payload copy.

Synchronous write calls accept non-owning C++ byte views. `cxx` passes these as
borrowed Rust slices, so crossing the language boundary does not copy the input.
The storage engine may still copy or encode data when it must retain it after
the call. A Rust-owned write-batch builder performs that required ownership
transfer once and is consumed by `Write`.

Owned row and batch views remain valid only while their owning C++ object is
alive. Caller-buffer views remain valid only until that buffer is modified or
destroyed.

Scan-plan and split boundaries are owned byte vectors so zero bytes and bytes
above `0x7f` retain their exact ordering and never acquire text semantics. A
synchronous `BytesView` setter performs one cold-path metadata copy. Split JSON
is a compatibility/persistence representation; scanner creation uses the typed
DTO and performs no JSON conversion. Core-to-C++ snapshot and split metadata is
copied, but row, key, and value payloads keep the owned or caller-buffer paths
above.

## Lifetime and concurrency

`Database` remains the ABI-compatible class name, while `SingleDb` is its
canonical source-level alias. The sharded `Db` is an independent PImpl class,
so adding it does not change the existing class layout. Database handles are
move-only and safe for concurrent calls to operations that take a const
handle. Scan cursors are move-only and require external synchronization. A
cursor retains both an owned core iterator and a concrete `SingleDb`-or-`Db`
owner, without a trait-object dispatch layer, so destroying the original
database handle cannot invalidate the cursor or make database shutdown wait on
a cursor owned by the same object graph.

Explicit close requires outstanding cursors and schema builders to be released
first. Normal C++ RAII destruction is ordered safely by their retained database
owners.

A schema builder retains a database owner in addition to its core access guard.
This allows the public database handle to be destroyed before the builder while
ensuring the guard is released before the final database owner during commit or
destruction. Pending snapshots are move-only, single-consumer handles; waiting
consumes their native handle, while destruction without waiting does not cancel
the underlying snapshot.

`Db::SwitchToSnapshot` performs a controlled restart on the existing handle.
It uses exclusive ownership of the core `Arc` rather than a global lock or
unsafe aliasing. A retained cursor or schema builder makes that ownership
unavailable, so the operation reports `ErrorCode::kInvalidState` and tells the
caller to release those children. Callers must externally serialize the switch
with other operations and cross-process snapshot metadata mutation.

`Db::Open(config)` parses and validates `total_buckets` in the inclusive range
1..=65536 before constructing the full bucket range. Explicit opens accept
inclusive `BucketRange` spans, reject empty/reversed/out-of-bounds input, and
leave normalization and the final non-overlap check to the core database.
Recovery defaults are deliberate: `Resume` means the latest snapshot with WAL
replay; `ResumeFromSnapshot` means the exact snapshot boundary without WAL
replay. Bucket expand uses typed optional snapshot/range inputs and a typed
storage mode, while adoption wait uses `std::chrono::milliseconds`.

`ReadOnlyDb` owns the exact shard manifest selected by `(snapshot_id, db_id)`.
Its cursors retain a concrete read-only owner. `Reader` owns the core reader;
its iterators retain their own database arcs, so a cursor remains valid after
the C++ reader handle is destroyed. Current readers may refresh to the latest
global snapshot; pinned readers reject refresh with
`ErrorCode::kInvalidState`. Reader methods are synchronous and mutable, and
callers provide external synchronization.

`DbCoordinator::MaterializeGlobalSnapshot` accepts typed shard snapshots. The
binding verifies that every inclusive range is within `total_buckets` and that
the ranges collectively cover every bucket exactly once before calling the
core, which additionally verifies column-family consistency. Scan split
scanners reject block-boundary stopping at construction because the distributed
scanner cannot resume that core state safely.

## ABI and compatibility

The public API requires C++20 for `std::span`. Implementation classes use PImpl,
and the generated `cxx` headers are private build inputs. Public enums and the
caller-owned batch wire format have explicit numeric values and versions. New
operations can be added without exposing the Rust implementation types.

Errors cross the `cxx` boundary as exceptions with a stable Cobble error-code
prefix. The C++ wrapper converts them to `cobble::Error`, retaining both the
error category and message.

## Initial non-goals

- Table logical types, codecs, and structured rows
- C++ callbacks for custom merge operators or filesystems
- Borrowing C++ memory beyond a synchronous call
- Async operations that retain arbitrary C++ spans

These can be added behind new owned interfaces without changing the raw KV data
model or the ownership rules above.
