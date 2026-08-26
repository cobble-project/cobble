# Cobble C++ binding design

## Scope

The first stable C++ API wraps `SingleDb` and the raw bucket/key/column data
model. It includes database open/resume, point and multi-key reads, writes,
write batches, range scans, raw schema evolution, metrics, runtime lifecycle
controls, and typed global snapshot operations. Table codecs and structured
Table schema APIs are intentionally outside this binding.

The public C++ header does not expose generated `cxx` types or Rust storage
layout. This leaves room to evolve the Rust bridge without changing every C++
consumer and keeps the public API usable from ordinary CMake projects.

## Source layout

`cobble.hpp` is a compatibility umbrella over the standalone public headers:
`types.hpp`, `options.hpp`, `write_batch.hpp`, `scan.hpp`, `multi_get.hpp`,
`schema.hpp`, `snapshot.hpp`, `metrics.hpp`, `lifecycle.hpp`, `single_db.hpp`,
and `database.hpp`.
The C++ implementation is split by the same responsibilities, with private
bridge, conversion, error-translation, and PImpl declarations under
`cpp/detail`. Rust keeps the `cxx` declaration in `src/lib.rs` so the generated
header and `cobble::ffi` namespace stay stable; database, write-batch, scan,
multi-get, snapshot, schema, metrics, lifecycle, options, encoding, and error
behavior live in focused private modules.

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

## Lifetime and concurrency

`Database` remains the ABI-compatible class name, while `SingleDb` is its
canonical source-level alias. Database handles are move-only and safe for
concurrent calls to operations that take a const handle. Scan cursors are move-only and require external
synchronization. A cursor retains both an owned core iterator and a database
owner so destroying the original database handle cannot invalidate the cursor
or make database shutdown wait on a cursor owned by the same object graph.

Explicit close requires outstanding cursors and schema builders to be released
first. Normal C++ RAII destruction is ordered safely by their retained database
owners.

A schema builder retains a database owner in addition to its core access guard.
This allows the public database handle to be destroyed before the builder while
ensuring the guard is released before the final database owner during commit or
destruction. Pending snapshots are move-only, single-consumer handles; waiting
consumes their native handle, while destruction without waiting does not cancel
the underlying snapshot.

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
- Distributed shard orchestration APIs
- Borrowing C++ memory beyond a synchronous call
- Async operations that retain arbitrary C++ spans

These can be added behind new owned interfaces without changing the raw KV data
model or the ownership rules above.
