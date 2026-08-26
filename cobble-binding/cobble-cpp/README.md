# Cobble C++

`cobble-cpp` is the C++20 binding for Cobble's raw bucket/key/column API. It is
built with [`cxx`](https://cxx.rs/) and presents an ordinary C++ header and
CMake target; generated bridge types are private implementation details.

The raw API includes:

- `SingleDb` open and global-snapshot resume;
- sharded `Db` open over all buckets or explicit inclusive bucket ranges;
- exact-snapshot, latest-with-WAL, new-database, and manifest recovery;
- point and one-crossing multi-get, put, delete, and merge operations;
- atomic write batches;
- projected range scans with owned or caller-buffer results;
- synchronous and asynchronous typed global snapshots, retention, expiration,
  listing, and inspection;
- typed shard snapshots, active snapshot switching, and bucket
  expand/adopt/shrink operations;
- current or pinned multi-shard `Reader` handles and exact-snapshot
  `ReadOnlyDb` handles;
- typed `DbCoordinator` global-snapshot materialization and binary-safe scan
  plans, splits, and resumable split scanners;
- typed raw schema inspection/evolution, lifecycle controls, and labeled
  metrics.

The separate `<cobble/structured.hpp>` entry point adds typed BYTES/LIST
`structured::Db` and `structured::SingleDb` APIs without changing the raw
umbrella or ABI. This surface includes point CRUD, reusable projected reads,
detached staged schema evolution, typed snapshot/lifecycle controls, recovery,
sharded rescaling, one-crossing batch and multi-get operations, owned and
caller-buffer scans, typed distributed scan plans, and detached priority queues.

## Requirements

- Rust toolchain compatible with the Cobble workspace
- CMake 3.22 or newer
- A C++20 compiler

## Build

```bash
cmake -S cobble-binding/cobble-cpp -B build/cobble-cpp \
  -DCMAKE_BUILD_TYPE=Release \
  -DCOBBLE_CPP_BUILD_TESTS=ON
cmake --build build/cobble-cpp --parallel
ctest --test-dir build/cobble-cpp --output-on-failure
```

CTest runs a focused binding test, a complete public-API/snapshot-recovery
test, raw `SingleDb` and sharded `Db` capability tests, and a bulk end-to-end
test. The tests cover JSON and file-based open/resume, both recovery modes, WAL
replay, TTL, mutation and projection options, caller-owned buffers, multi-run
block-boundary resume, one-crossing multi-get, schema evolution,
lifecycle/metrics, typed snapshots, snapshot retention/expiration, active
snapshot switching, and bucket rescaling. The bulk test writes 20,000
two-column rows (12.8 MB of values) across 16 buckets, verifies
point reads, both scan ownership modes, snapshot creation, close, resume, and a
second full scan of the restored database. A read-surface capability test also
covers current and pinned readers, read-only exact snapshots, coordinator range
validation, binary split boundaries, malformed split JSON, and owned plus
caller-buffer split scans.

The default build produces the shared target `cobble::cobble`. Cargo builds the
private Rust static library as part of the CMake build.

Optional storage backends can be passed as a comma-separated Cargo feature
list:

```bash
cmake -S cobble-binding/cobble-cpp -B build/cobble-cpp \
  -DCMAKE_BUILD_TYPE=Release \
  -DCOBBLE_CPP_CARGO_FEATURES="storage-s3,storage-oss"
```

The S3 protocol test is opt-in because it needs an existing S3-compatible
endpoint and bucket. Enable `storage-s3` and the test target, then provide the
test credentials when running CTest:

```bash
cmake -S cobble-binding/cobble-cpp -B build/cobble-cpp-s3 \
  -DCMAKE_BUILD_TYPE=Release \
  -DCOBBLE_CPP_BUILD_TESTS=ON \
  -DCOBBLE_CPP_BUILD_S3_E2E_TEST=ON \
  -DCOBBLE_CPP_CARGO_FEATURES=storage-s3
cmake --build build/cobble-cpp-s3 --parallel
COBBLE_S3_ENDPOINT=http://127.0.0.1:9000 \
COBBLE_S3_BUCKET=cobble-cpp-test \
COBBLE_S3_ACCESS_ID=rustfsadmin \
COBBLE_S3_SECRET_KEY=rustfsadmin \
ctest --test-dir build/cobble-cpp-s3 -R cobble_cpp_s3_e2e \
  --output-on-failure
```

The test writes 512 raw rows and 384 structured BYTES+LIST rows under separate
S3 prefixes. It materializes snapshots, closes both databases, resumes
the exact snapshots with `SnapshotOnly`, and verifies point reads plus full
ordered scans. CI runs the same flow against a pinned RustFS container.

## Use from CMake

After installation:

```cmake
find_package(cobble-cpp CONFIG REQUIRED)
target_link_libraries(my_app PRIVATE cobble::cobble)
target_compile_features(my_app PRIVATE cxx_std_20)
```

The package installs `cobble/cobble.hpp` and the shared library. Consumers do
not need to include a generated `cxx` header. `cobble.hpp` remains the complete
compatibility umbrella; consumers that prefer narrower dependencies can include
`types.hpp`, `options.hpp`, `write_batch.hpp`, `scan.hpp`, `multi_get.hpp`,
`schema.hpp`, `snapshot.hpp`, `metrics.hpp`, `lifecycle.hpp`, `rescale.hpp`,
`single_db.hpp`, `database.hpp`, `db.hpp`, `read_only_db.hpp`, `reader.hpp`,
`coordinator.hpp`, or `scan_plan.hpp` directly.

Structured consumers include `<cobble/structured.hpp>` explicitly. It is not
included by `<cobble/cobble.hpp>`, so existing raw-only translation units do
not acquire new declarations or dependencies.

## Data ownership and zero-copy paths

Input `BytesView` values are borrowed for the synchronous call and cross the
C++/Rust boundary as `rust::Slice`, without an interop copy. Cobble may still
encode or retain the data internally as required by the storage operation.

`Database::Get`, `Db::Get`, `ReadOnlyDb::Get`, `Reader::Get`, and
`ScanCursor::Next` return move-only RAII objects whose payload remains in Rust
`Bytes` allocations. Their key and column accessors return `std::span` views
without copying payload into `std::vector` or `std::string`.

`Database::MultiGet`, `Db::MultiGet`, `ReadOnlyDb::MultiGet`, and
`Reader::MultiGet` cross the bridge once. Their descriptor array borrows the
original C++ key spans synchronously, so key payloads are not concatenated or
copied at the binding boundary. `OwnedMultiGetResult` keeps the returned Rust
`Bytes` allocations alive and exposes zero-copy column views.

For reusable C++ memory, every raw read handle provides `GetColumnInto`, and
`ScanCursor::NextBatchInto` writes scans into a caller-owned span. A too-small
scan buffer reports the required size and retains the pending rows so retrying
does not skip data.

Structured BYTES inputs are borrowed at the bridge and copied once when Cobble
takes ownership. LIST inputs cross as one descriptor vector and are encoded
directly into the final Cobble wire buffer; the binding does not first copy
each element into an intermediate `Bytes`. `structured::OwnedRow` keeps the
returned Rust `Bytes` and decoded LIST element slices alive, and its accessors
return zero-copy spans. Reusing `structured::ReadOptions` also reuses the native
schema projection cache.

`structured::Db::Write(span)` and `structured::SingleDb::Write(span)` are the
high-throughput synchronous batch path: operation, key, BYTES, and LIST-element
views are borrowed only until the call returns and cross FFI once. The reusable
`structured::WriteBatch` instead copies every appended payload and option into
C++-owned storage, so temporary source containers may be destroyed before
`Write(batch)`. Successful flush and `Clear` retain builder capacity.

Structured `GetInto`, `MultiGetInto`, and `ScanCursor::NextBatchInto` share the
versioned `CSRB` format documented in [ROW_BATCH_FORMAT.md](ROW_BATCH_FORMAT.md).
Owned multi-get and scan results keep Rust `Bytes` and LIST element allocations
alive and expose them through zero-copy spans.

`structured::PriorityQueue` caches its validated family descriptor and native
read/write options. Offer inputs are borrowed synchronously; owned entries and
batches expose the returned Rust `Bytes` without a payload copy. Point and
batch caller-buffer operations also use `CSRB`. A too-small poll keeps both the
output and queue cursor unchanged; retry must use the same operation, bucket,
and optional limit.

`ScanPlan` and typed split DTOs own their binary boundary keys. Constructing a
plan from `BytesView` copies those cold-path metadata bytes once; split JSON is
provided for durable compatibility and is not used on the scan data path.

The full ownership contract is recorded in [DESIGN.md](DESIGN.md).

## Exceptions and thread safety

Binding operations throw `cobble::Error`, which contains a stable `ErrorCode`
and the Cobble error message. Database methods that take a const handle may be
called concurrently. A scan cursor is mutable and requires external
synchronization.

Release all scan cursors, schema builders, and priority queues before an explicit
`Database::Close` or `Db::Close`. Normal RAII destruction is safe because these
dependent objects retain their database owner.

`Db::SwitchToSnapshot` is an exclusive operation on the same handle. It fails
with `ErrorCode::kInvalidState` while a scan cursor, schema builder, or priority queue retains
that database; release those children and externally serialize the switch with
other operations. `Db::Resume` selects the latest snapshot and replays the WAL
by default, while `Db::ResumeFromSnapshot` selects the exact snapshot boundary
without WAL replay by default.
