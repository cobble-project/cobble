# Cobble C++

`cobble-cpp` is the C++20 binding for Cobble's raw bucket/key/column API. It is
built with [`cxx`](https://cxx.rs/) and presents an ordinary C++ header and
CMake target; generated bridge types are private implementation details.

The raw single-node API includes:

- single-node database open and snapshot resume;
- point and one-crossing multi-get, put, delete, and merge operations;
- atomic write batches;
- projected range scans with owned or caller-buffer results;
- synchronous and asynchronous typed global snapshots, retention, expiration,
  listing, and inspection;
- typed raw schema inspection/evolution, lifecycle controls, and labeled
  metrics.

Table codecs and structured Table rows are not part of this binding.

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
test, a raw `SingleDb` capability test, and a bulk end-to-end test. The tests
cover JSON and file-based
open/resume, both recovery modes, WAL replay, TTL, mutation and projection
options, caller-owned buffers, multi-run block-boundary resume, one-crossing
multi-get, schema evolution, lifecycle/metrics, typed snapshots, and snapshot
retention/expiration. The bulk test writes 20,000 two-column rows (12.8 MB of
values) across 16 buckets, verifies
point reads, both scan ownership modes, snapshot creation, close, resume, and a
second full scan of the restored database.

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

The test writes 512 rows, materializes an S3-backed snapshot, closes the
database, resumes that exact snapshot, and verifies point reads plus a full
ordered scan. CI runs the same flow against a pinned RustFS container.

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
`schema.hpp`, `snapshot.hpp`, `metrics.hpp`, `lifecycle.hpp`, `single_db.hpp`,
or `database.hpp` directly.

## Data ownership and zero-copy paths

Input `BytesView` values are borrowed for the synchronous call and cross the
C++/Rust boundary as `rust::Slice`, without an interop copy. Cobble may still
encode or retain the data internally as required by the storage operation.

`Database::Get` and `ScanCursor::Next` return move-only RAII objects whose
payload remains in Rust `Bytes` allocations. Their key and column accessors
return `std::span` views without copying payload into `std::vector` or
`std::string`.

`Database::MultiGet` crosses the bridge once. Its descriptor array borrows the
original C++ key spans synchronously, so key payloads are not concatenated or
copied at the binding boundary. `OwnedMultiGetResult` keeps the returned Rust
`Bytes` allocations alive and exposes zero-copy column views.

For reusable C++ memory, `Database::GetColumnInto` and
`ScanCursor::NextBatchInto` write into a caller-owned span. A too-small scan
buffer reports the required size and retains the pending rows so retrying does
not skip data.

The full ownership contract is recorded in [DESIGN.md](DESIGN.md).

## Exceptions and thread safety

Binding operations throw `cobble::Error`, which contains a stable `ErrorCode`
and the Cobble error message. Database methods that take a const handle may be
called concurrently. A scan cursor is mutable and requires external
synchronization.

Release all scan cursors and schema builders before an explicit
`Database::Close`. Normal RAII destruction is safe because these dependent
objects retain their database owner.
