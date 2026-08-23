# Cobble C++

`cobble-cpp` is the C++20 binding for Cobble's raw bucket/key/column API. It is
built with [`cxx`](https://cxx.rs/) and presents an ordinary C++ header and
CMake target; generated bridge types are private implementation details.

The initial API includes:

- single-node database open and snapshot resume;
- point get, put, delete, and merge operations;
- atomic write batches;
- projected range scans with owned or caller-buffer results;
- global snapshot creation, retention, expiration, listing, and inspection.

Table codecs and structured Table rows are not part of this binding.

## Requirements

- Rust toolchain compatible with the Cobble workspace
- CMake 3.22 or newer
- A C++20 compiler

## Build

```bash
cmake -S cobble-cpp -B build/cobble-cpp \
  -DCMAKE_BUILD_TYPE=Release \
  -DCOBBLE_CPP_BUILD_TESTS=ON
cmake --build build/cobble-cpp --parallel
ctest --test-dir build/cobble-cpp --output-on-failure
```

The default build produces the shared target `cobble::cobble`. Cargo builds the
private Rust static library as part of the CMake build.

Optional storage backends can be passed as a comma-separated Cargo feature
list:

```bash
cmake -S cobble-cpp -B build/cobble-cpp \
  -DCMAKE_BUILD_TYPE=Release \
  -DCOBBLE_CPP_CARGO_FEATURES="storage-s3,storage-oss"
```

## Use from CMake

After installation:

```cmake
find_package(cobble-cpp CONFIG REQUIRED)
target_link_libraries(my_app PRIVATE cobble::cobble)
target_compile_features(my_app PRIVATE cxx_std_20)
```

The package installs `cobble/cobble.hpp` and the shared library. Consumers do
not need to include a generated `cxx` header.

## Data ownership and zero-copy paths

Input `BytesView` values are borrowed for the synchronous call and cross the
C++/Rust boundary as `rust::Slice`, without an interop copy. Cobble may still
encode or retain the data internally as required by the storage operation.

`Database::Get` and `ScanCursor::Next` return move-only RAII objects whose
payload remains in Rust `Bytes` allocations. Their key and column accessors
return `std::span` views without copying payload into `std::vector` or
`std::string`.

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

Release all scan cursors before an explicit `Database::Close`. Normal RAII
destruction is safe because every cursor retains its database owner.
