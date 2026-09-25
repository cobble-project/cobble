---
title: C++
parent: FFI Bindings
nav_order: 2
---

# C++ Bindings

`cobble-cpp` provides C++20 APIs for embedded databases, sharded writers,
snapshot readers, and distributed scans. Include `<cobble/cobble.hpp>` for
raw byte columns or `<cobble/structured.hpp>` for BYTES and LIST columns.
The C++ binding does not currently expose the Table/Catalog APIs.

## API overview

| API | Use it for |
|-----|------------|
| `SingleDb` | Embedded applications: open a database, read/write data, and save or restore snapshots. |
| `Db` | Distributed writers: manage one shard and its assigned buckets. |
| `Reader` | Read across shards from a fixed global snapshot or follow new committed snapshots. |
| `ReadOnlyDb` | Read one shard at a specific snapshot. |
| `DbCoordinator` | Combine shard snapshots into a global snapshot. |
| `ScanPlan` / `ScanSplit` | Divide a snapshot scan into tasks for multiple workers. |
| `structured::…` | Work with BYTES and LIST columns, including priority queues. |

Use `Put`, `Get`, and `Delete` for individual records; `MultiGet` and
`WriteBatch` for batches; and `Scan` for a range of keys. Read and write options
let you select columns or customize individual operations.

## Build and link

Build from a Cobble source checkout with a compatible Rust toolchain,
CMake 3.22 or newer, and a C++20 compiler:

```bash
cmake -S cobble-binding/cobble-cpp -B build/cobble-cpp \
  -DCMAKE_BUILD_TYPE=Release
cmake --build build/cobble-cpp --parallel
cmake --install build/cobble-cpp --prefix /absolute/path/to/cobble-install
```

To enable optional storage backends, add, for example,
`-DCOBBLE_CPP_CARGO_FEATURES=storage-s3` to the configure command.

In your application's `CMakeLists.txt`:

```cmake
find_package(cobble-cpp CONFIG REQUIRED)
add_executable(my_app main.cpp)
target_link_libraries(my_app PRIVATE cobble::cobble)
target_compile_features(my_app PRIVATE cxx_std_20)
```

Configure your application with
`-DCMAKE_PREFIX_PATH=/absolute/path/to/cobble-install`. When deploying, make
the installed shared library available to your platform's library loader.

## Basic reads and writes

`SingleDb` manages a single-machine database. `Open` accepts JSON configuration;
`OpenFile` accepts a configuration file path.

```cpp
#include <cobble/cobble.hpp>
#include <iostream>
#include <string>
#include <string_view>

cobble::BytesView Bytes(std::string_view value) {
  return {reinterpret_cast<const cobble::Byte*>(value.data()), value.size()};
}

int main() {
  const std::string config = R"({
    "volumes": [{
      "base_dir": "file:///tmp/cobble-example",
      "kinds": ["meta", "primary_data_priority_high", "snapshot"]
    }],
    "num_columns": 1,
    "total_buckets": 1
  })";

  try {
    auto db = cobble::SingleDb::Open(config);
    db.Put(0, Bytes("user:1"), 0, Bytes("Alice"));
    {
      auto row = db.Get(0, Bytes("user:1"));
      if (row.Found() && row.HasColumn(0)) {
        const auto value = row.Column(0);
        std::cout << std::string(value.begin(), value.end()) << '\n';
      }
    }
    const auto snapshot = db.TakeSnapshot(); // Waits for completion.
    (void)db.RetainSnapshot(snapshot.id);
    std::cout << "Snapshot: " << snapshot.id << '\n';
  } catch (const cobble::Error& error) {
    std::cerr << error.what() << '\n';
    return 1;
  }
}
```

Use `SingleDb::Resume(config, snapshot_id)` to reopen a saved global snapshot.
For shard-level writes and recovery, use `Db`; it returns a `ShardSnapshot`
from `TakeSnapshot()`. A `DbCoordinator` combines shard reports into a global
snapshot. See [Distributed Deployment](../getting-started/distributed).

## Snapshot readers

Use `Reader::OpenCurrent(config)` to follow committed global snapshots, and
`Refresh()` to explicitly check for a newer commit. Use
`Reader::Open(config, snapshot_id)` for a fixed snapshot, or `ReadOnlyDb` to
open one shard snapshot.

You can also load metadata without opening a database, then open a fixed reader
from the resulting object:

```cpp
auto snapshot = cobble::LoadGlobalSnapshotMetadata(config, manifest_path);
auto reader = cobble::Reader::Open(config, snapshot);
auto row = reader.Get(0, Bytes("user:1"));
```

`manifest_path` must be an absolute path or URL inside a configured metadata
volume. `LoadShardSnapshotMetadata(config, db_id, shard_manifest_path)` also
returns the shard's captured schema metadata. Both loaders have `...File`
variants accepting a configuration file path.

Opening from a `GlobalSnapshot` does not reload its global manifest. The reader
owns its metadata and stays on that snapshot; it cannot be refreshed. Keep
referenced snapshots retained until readers and scans finish using them.

For distributed scanning, create `ScanPlan::FromGlobalSnapshot(snapshot)`,
obtain `Splits()`, and call `OpenScanner(config)` on each split. Splits support
`ToJson()` / `FromJson()` for transfer to workers. See
[Reader & Distributed Scan](../getting-started/reader-and-scan).

## Structured columns

`cobble::structured` provides `Db`, `SingleDb`, `Reader`, and `ReadOnlyDb` with
BYTES and LIST columns. For example, using the `Bytes` helper above:

```cpp
#include <cobble/structured.hpp>
#include <array>
#include <optional>

auto db = cobble::structured::SingleDb::Open(config);
auto builder = db.UpdateSchema();
builder.AddListColumn(std::nullopt, 1, cobble::structured::ListConfig{});
const auto schema = builder.Commit();

db.PutBytes(0, Bytes("user:1"), 0, Bytes("Alice"));
const std::array<cobble::BytesView, 2> tags = {Bytes("reader"), Bytes("writer")};
db.PutList(0, Bytes("user:1"), 1, tags);
auto row = db.Get(0, Bytes("user:1"));
if (row.Found() && row.HasColumn(1)) {
  auto first_tag = row.ListElement(1, 0);
  // Consume first_tag while row is alive.
}
```

Run this example against a separate database from the raw example. Structured
readers support the same current, fixed-ID, and existing-`GlobalSnapshot` modes.

## Usage notes

- Objects release resources automatically when they leave scope. Finish using
  scans, schema builders, and queues before explicitly closing their database.
- Keep the returned row or batch alive while using its data; copy values if
  you need to keep them longer.
- Catch `cobble::Error` to handle failures; `what()` describes the error.
