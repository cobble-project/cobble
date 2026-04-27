---
title: Java
parent: FFI Bindings
nav_order: 1
---

# Java Bindings

The `cobble-java` crate provides JNI bindings that expose Cobble's functionality to JVM-based applications. The Java API mirrors the Rust API closely, covering all major components: `SingleDb`, `Db`, `Reader`, structured wrappers, and distributed scan.

## Dependency

We have published the Java API to Maven Central. You can add it as a dependency in your `pom.xml`:

```xml
<dependency>
    <groupId>io.github.cobble-project</groupId>
    <artifactId>cobble</artifactId>
    <version>0.2.0</version>
</dependency>
```

## Java API Overview

### SingleDb

For single-machine embedded use. See [Single-Machine Embedded DB](../getting-started/single-db) for the full workflow.

```java
import io.cobble.GlobalSnapshot;
import io.cobble.SingleDb;

String configPath = "config.yaml";
SingleDb db = SingleDb.open(configPath);

// Write
db.put(0, "user:1".getBytes(), 0, "Alice".getBytes());

// Read
byte[] value = db.get(0, "user:1".getBytes(), 0);

// Snapshot and resume
GlobalSnapshot snapshot = db.snapshot();
db.close();
db = SingleDb.resume(configPath, snapshot.id);
```

### Db (Distributed)

For multi-shard deployments. See [Distributed Deployment](../getting-started/distributed) for the full workflow.

```java
import io.cobble.Db;
import io.cobble.ShardSnapshot;

Db db = Db.open("config.yaml");
db.put(0, "key".getBytes(), 0, "value".getBytes());

// Create snapshot input for coordinator
ShardSnapshot input = db.snapshot();

// Restore into the same db identity
Db restored = Db.restore("config.yaml", input.snapshotId, db.id());

// Restore into a fresh db identity
Db restoredFresh = Db.restore("config.yaml", input.snapshotId, db.id(), true);

// Restore from an explicit manifest path (always creates a fresh db identity)
Db restoredFromManifest = Db.restoreWithManifest("xxx/SNAPSHOT-1", input.manifestPath);
```

### Reader

For snapshot-following reads. Reader visibility advances when new snapshots are materialized and reader refresh picks them up. See [Reader & Distributed Scan](../getting-started/reader-and-scan).

```java
import io.cobble.Reader;

Reader reader = Reader.openCurrent("config.yaml");
byte[] value = reader.get(0, "key".getBytes(), 0);
reader.refresh();
```

## Column Families

The Java API exposes column families in two ways:

- convenience overloads with `String columnFamily` on `Db`, `SingleDb`, `ReadOnlyDb`, `Reader`, and structured wrappers;
- reusable options objects (`io.cobble.*Options` for raw APIs, `io.cobble.structured.*Options` for structured APIs).

`ScanPlan` / `StructuredScanPlan` stay bucket-only. To scan a named family, build the plan from a global snapshot as usual, then pass `ScanOptions.columnFamily(...)` when opening the scanner. That scanner-open step is where the family becomes effective.

### Raw API

```java
import io.cobble.Db;
import io.cobble.ReadOptions;
import io.cobble.ScanCursor;
import io.cobble.ScanOptions;
import io.cobble.Schema;
import io.cobble.WriteOptions;

Db db = Db.open("config.yaml");
db.updateSchema().addColumn("metrics", 0, null, null).commit();

try (WriteOptions write = WriteOptions.withColumnFamily("metrics")) {
    db.putWithOptions(0, "user:1".getBytes(), 0, "42".getBytes(), write);
}

try (ReadOptions read = ReadOptions.forColumnInFamily("metrics", 0)) {
    byte[][] row = db.getWithOptions(0, "user:1".getBytes(), read);
}

try (ScanOptions scan = ScanOptions.forColumns(0).columnFamily("metrics")) {
    try (ScanCursor cursor =
            db.scanWithOptions(0, "user:".getBytes(), "user;".getBytes(), scan)) {
        for (ScanCursor.Entry entry : cursor) {
            byte[] key = entry.key;
            byte[][] columns = entry.columns;
        }
    }
}

Schema schema = db.currentSchema();
assert schema.columnFamily("metrics") != null;
```

`ReadOptions` also provides `forColumnsInFamily(...)` and `defaultsInFamily(...)`. `ScanOptions` intentionally uses `forColumns(...).columnFamily(...)`.

For Java `*WithOptions(...)` methods, passing `null` means "no explicit options": JNI routes directly
to Cobble core no-options APIs (it does not synthesize default options objects in JNI).

### Raw API Direct ByteBuffer Path

For lower JNI overhead on non-structured `Db`, Java bindings also provide direct-buffer APIs:

```java
import io.cobble.Db;
import io.cobble.DirectColumns;
import io.cobble.DirectScanCursor;
import io.cobble.DirectScanEntry;
import io.cobble.ReadOptions;
import io.cobble.ScanOptions;
import io.cobble.WriteOptions;
import java.nio.ByteBuffer;
import java.nio.Buffer;

Db db = Db.open("config.yaml");
ByteBuffer ioBuffer = ByteBuffer.allocateDirect(2048);
ByteBuffer valueBuffer = ByteBuffer.allocateDirect(1024);

byte[] key = "user:1".getBytes();
byte[] value = "alice".getBytes();

((Buffer) ioBuffer).clear();
ioBuffer.put(key);
((Buffer) valueBuffer).clear();
valueBuffer.put(value);

try (WriteOptions write = WriteOptions.forColumn(0)) {
    db.putDirectWithOptions(0, ioBuffer, key.length, 0, valueBuffer, value.length, write);
}

try (ReadOptions read = ReadOptions.forColumn(0)) {
    ByteBuffer encoded = db.getEncodedDirectWithOptions(0, ioBuffer, key.length, read);
    // encoded == ioBuffer => reused input buffer
    // encoded != ioBuffer => temporary larger direct buffer was allocated
}

// Pooled direct API (no caller-managed IO buffer), one direct buffer per selected column.
try (ReadOptions read = ReadOptions.forColumn(0)) {
    ByteBuffer[] columns = db.getDirectWithOptions(0, key, read);
}

// Zero-copy pooled direct API. Close it when done so pooled buffers can be reused.
try (ReadOptions read = ReadOptions.forColumns(0);
     DirectColumns columns = db.getDirectColumnsWithOptions(0, key, read)) {
    ByteBuffer column0 = columns.get(0);
}

ByteBuffer scanStart = ByteBuffer.allocateDirect(64);
ByteBuffer scanEnd = ByteBuffer.allocateDirect(64);
((Buffer) scanStart).clear();
scanStart.put("user:".getBytes());
((Buffer) scanEnd).clear();
scanEnd.put("user;".getBytes());

try (ScanOptions scan = new ScanOptions().batchSize(128).columns(0);
     DirectScanCursor cursor =
             db.scanDirectWithOptions(
                     0,
                     scanStart,
                     "user:".length(),
                     scanEnd,
                     "user;".length(),
                     scan)) {
    for (DirectScanEntry entry : cursor) {
        ByteBuffer directKey = entry.getKey();
        ByteBuffer directValue = entry.getColumn(0);
    }
}
```

`getEncodedDirectWithOptions(...)` returns:

- `null` if key is not found
- the same `ioBuffer` when payload fits (`encoded == ioBuffer`)
- a new temporary direct buffer when payload is larger than `ioBuffer.capacity()`

`getDirectWithOptions(...)` returns `ByteBuffer[]` (one element per selected column, `null` for
missing columns), and each non-null column is a direct `ByteBuffer`.

`getDirectColumnsWithOptions(...)` is the zero-copy variant of `getDirectWithOptions(...)`: it
returns a `DirectColumns` view over the JNI-owned direct payload instead of copying each selected
column into a fresh direct buffer.

`scanDirectWithOptions(...)` returns a `DirectScanCursor` whose batches are encoded into pooled
direct buffers. Each `DirectScanEntry` exposes:

- `getKey()` → direct `ByteBuffer`
- `getColumn(i)` → direct `ByteBuffer` (or `null` for a missing raw column)

Like structured direct scan, one direct scan batch remains valid only until the cursor advances to
the next batch or closes.

Direct buffer pool settings are loaded from the opened DB config
(`jni_direct_buffer_size`, `jni_direct_buffer_pool_size`) during open/restore/resume. Runtime
updates use `Db.configureDirectBufferPool(sizeBytes, maxPoolSize)`, which is **grow-only**:
shrinking either value returns `false`.

Snapshot DTOs also preserve the mapping:

```java
import io.cobble.Db;
import io.cobble.DbCoordinator;
import io.cobble.GlobalSnapshot;
import io.cobble.ShardSnapshot;
import java.util.List;

Db db = Db.open("config.yaml");
DbCoordinator coordinator = DbCoordinator.open("coordinator.yaml");
ShardSnapshot shardSnapshot = db.snapshot();
int totalBuckets = 1024;
long globalSnapshotId = 42L;

GlobalSnapshot globalSnapshot =
        coordinator.materializeGlobalSnapshot(
                totalBuckets, globalSnapshotId, List.of(shardSnapshot));
```

### Structured Wrappers

Structured wrappers are schema-driven and currently support typed `Bytes` and `List` columns. See [Structured DB](../getting-started/structured-db).

On Java APIs, options class names stay unchanged but package paths are split:
structured wrappers take `io.cobble.structured.ReadOptions`,
`io.cobble.structured.ScanOptions`, and `io.cobble.structured.WriteOptions`.
Raw wrappers keep using `io.cobble.ReadOptions`, `io.cobble.ScanOptions`,
and `io.cobble.WriteOptions`.

Structured `*WithOptions(...)` methods follow the same `null` behavior as raw APIs: `null` routes to
core no-options paths directly.

Structured direct-read APIs (`getDirect`, `getDirectWithOptions`) use an internal direct-buffer
pool. Pool settings are loaded from the opened DB config (`jni_direct_buffer_size`,
`jni_direct_buffer_pool_size`) during open/restore/resume:

- `jni_direct_buffer_size` (default `2KiB`)
- `jni_direct_buffer_pool_size` (default `64`)

Runtime updates use `Db.configureDirectBufferPool(sizeBytes, maxPoolSize)`, and are grow-only:
shrinking either value returns `false`.

Structured direct scan follows the same pooled direct-buffer model:

```java
import io.cobble.structured.DirectRow;
import io.cobble.structured.DirectScanCursor;
import io.cobble.structured.DirectScanRow;
import io.cobble.structured.ScanOptions;
import java.nio.ByteBuffer;
import java.nio.Buffer;

ByteBuffer start = ByteBuffer.allocateDirect(64);
ByteBuffer end = ByteBuffer.allocateDirect(64);
((Buffer) start).clear();
start.put("user:".getBytes());
((Buffer) end).clear();
end.put("user;".getBytes());

try (ScanOptions scan = new ScanOptions().batchSize(128).columns(0);
     DirectScanCursor cursor =
             db.scanDirectWithOptions(0, start, "user:".length(), end, "user;".length(), scan)) {
    for (DirectScanRow row : cursor) {
        ByteBuffer keyBytes = row.getKey();
        ByteBuffer valueBytes = row.getBytes(0);
    }
}
```

Structured `DirectScanRow` reuses the existing `DirectRow` column encoding:

- `getKey()` → direct key slice
- `getBytes(i)` → direct `ByteBuffer`
- `getList(i)` → `List<ByteBuffer>`

```java
import io.cobble.structured.SingleDb;
import io.cobble.structured.ColumnValue;
import io.cobble.structured.Row;

SingleDb db = SingleDb.open("config.yaml");
db.updateSchema()
    .addListColumn(1, io.cobble.structured.ListConfig.of(100, io.cobble.structured.ListRetainMode.LAST))
    .commit();
db.put(0, "user:1".getBytes(), 0, ColumnValue.ofBytes("Alice".getBytes()));
Row row = db.get(0, "user:1".getBytes());
```

Structured schema builders also accept named families, and structured schema views are family-aware:

```java
db.updateSchema()
    .addListColumn("metrics", 0, io.cobble.structured.ListConfig.of(
            100, io.cobble.structured.ListRetainMode.LAST))
    .commit();

db.put(
        0,
        "user:1".getBytes(),
        "metrics",
        0,
        ColumnValue.ofList(new byte[][] {"a".getBytes()}));
Row metricsRow = db.get(0, "user:1".getBytes(), "metrics");
var namedFamilies = db.currentSchema().columnFamilies();
assert namedFamilies.containsKey("default");
assert namedFamilies.containsKey("metrics");
```

### Distributed Scan

For parallel analytical scans across shards. See [Reader & Distributed Scan](../getting-started/reader-and-scan).

```java
import io.cobble.ScanPlan;
import io.cobble.ScanOptions;
import io.cobble.ScanSplit;
import io.cobble.ScanCursor;

ScanPlan plan = ScanPlan.fromGlobalSnapshot(manifest);
List<ScanSplit> splits = plan.splits();
ScanOptions scanOptions = ScanOptions.forColumns(0).columnFamily("metrics");

for (ScanSplit split : splits) {
    try (ScanCursor cursor = split.openScannerWithOptions("config.yaml", scanOptions)) {
        for (ScanCursor.Entry entry : cursor) {
            byte[] key = entry.key;
            byte[][] columns = entry.columns;
        }
    }
}
```

Like Rust, the worker-side `ScanCursor` keeps the full `ScanOptions`, so the selected `columnFamily(...)` still applies after a split is serialized and reopened elsewhere. If you omit it, the scanner reads the default family.

## Memory Management

Java objects hold opaque pointers to Rust `Arc<T>` allocations. Calling `close()` on a Java wrapper releases the underlying Rust resources. If `close()` is not called, resources will not be freed and **may lead to memory leaks**. It is recommended to use try-with-resources or ensure `close()` is called in a `finally` block.

## Building

If you want to customize your java APIs, you can compile by yourself. The Java binding consists of two parts:

1. **Native library** (Rust → shared library via `cdylib`)
2. **Java API** (Maven project with JNI wrappers)

`cobble-java` re-exposes `storage-*` features and forwards them to both `cobble` and `cobble-data-structure`.
For the full feature list and usage examples, refer to `cobble`:
https://crates.io/crates/cobble

Java native build commands use `--features storage-all` on `cobble-java`, so Java artifacts include all Cobble optional OpenDAL backend features (`alluxio`, `cos`, `oss`, `s3`, `ftp`, `hdfs`, `sftp`).

```bash
# Local debug build (current platform only, includes debug + release JNI libs)
cd cobble-java/java
./mvnw package
```

Local `mvn package` only bundles the current host platform into the jar (for fast debugging).

To produce a **single multi-platform jar** (including `debug` + `release` JNI libs for common platforms), use the manual GitHub Actions workflow:

- Workflow: `.github/workflows/java-multi-platform-jar.yml`
- Trigger: **Actions -> Build Java Multi-platform JAR -> Run workflow**
- Output: downloadable jar artifact from the workflow run

For publishing, the same workflow also runs on **GitHub Release published** events and deploys the multi-platform build to **Maven Central** (via Sonatype `central-publishing-maven-plugin`).

Required repository secrets for release publishing:

- `MAVEN_CENTRAL_USERNAME` (Central Portal user token username)
- `MAVEN_CENTRAL_PASSWORD` (Central Portal user token password)
- `MAVEN_GPG_PRIVATE_KEY` (ASCII-armored private key, or base64-encoded armored key)
- `MAVEN_GPG_PASSPHRASE`

- `macos-aarch64`
- `macos-x86_64`
- `linux-x86_64`
- `linux-aarch64`
- `windows-x86_64`
