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
import io.cobble.PendingSnapshot;
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
Db restoredFromManifest = Db.restoreWithManifest("config.yaml", input.manifestPath);
```

### Reader

For snapshot-following reads. Reader visibility advances when new snapshots are materialized and reader refresh picks them up. See [Reader & Distributed Scan](../getting-started/reader-and-scan).

```java
import io.cobble.Reader;

Reader reader = Reader.openCurrent("config.yaml");
byte[] value = reader.get(0, "key".getBytes(), 0);
reader.refresh();
```

## Process-Level Filesystem Fallback APIs

Java bindings expose process-level filesystem extension APIs so host frameworks can plug in their
own filesystem stack for Cobble volume access.

### Key Interfaces

- `ProcessFileSystems`: process-scoped register/clear entrypoints.
- `CustomFileSystemRegistry`: resolves `ProcessFileSystemRequest` into a `CustomFileSystem`.
- `CustomFileSystem`: filesystem operations used by Cobble native runtime.
- `CustomRandomAccessFile` / `CustomSequentialWriteFile`: file handles for read/write operations.

### Direct I/O Capability Contract

- `supportDirect()` declares whether the file handle supports direct `ByteBuffer` paths.
- If `supportDirect()` is `false`, JNI uses `byte[]` methods only.
- If `supportDirect()` is `true`, JNI may call `readAtDirect(...)` / `writeDirect(...)`.

### Minimal Registration Example

```java
ProcessFileSystems.registerCustomRegistry(
        request -> {
            String baseDir =
                    request.normalizedBaseDir() != null
                            ? request.normalizedBaseDir()
                            : request.baseDir();
            if (baseDir == null || baseDir.isBlank()) {
                return null;
            }
            return new MyCustomFileSystem(baseDir);
        });
```

Clear the process fallback registry when your process no longer needs it:

```java
ProcessFileSystems.clearCustomRegistry();
```

## Write APIs

Column family can be passed either by `String columnFamily` overloads or via options objects
(`io.cobble.*Options` / `io.cobble.structured.*Options`).

### 1) `byte[]` style (simple)

```java
Db db = Db.open("config.yaml");
db.put(0, "user:1".getBytes(), 0, "Alice".getBytes());
```

### 2) Direct buffer style (low JNI overhead)

```java
ByteBuffer key = ByteBuffer.allocateDirect(64);
ByteBuffer value = ByteBuffer.allocateDirect(256);
// write key/value bytes...
db.putDirectWithOptions(0, key, keyLen, 0, value, valueLen, null);
```

### 3) Structured style (typed columns)

```java
io.cobble.structured.Db sdb = io.cobble.structured.Db.open("config.yaml");
sdb.put(0, "user:1".getBytes(), 0, io.cobble.structured.ColumnValue.ofBytes("Alice".getBytes()));
```

### 4) Structured list-direct style (best for list writes)

```java
DirectListValueBuilder b = new DirectListValueBuilder(256);
b.append("a".getBytes());
b.append("b".getBytes());
sdb.mergeEncodedListDirectWithOptions(0, keyBuf, keyLen, 0, b.buffer(), b.length(), null);
```

## Read APIs

### 1) `byte[]` style (simple)

```java
byte[] value = db.get(0, "user:1".getBytes(), 0);
```

### 2) Direct view style (`DirectColumns` / `DirectRow`)

```java
try (DirectColumns cols = db.getDirectColumnsWithOptions(0, "user:1".getBytes(), ReadOptions.forColumns(0))) {
    ByteBuffer v = cols.get(0);
}
```

```java
try (io.cobble.structured.DirectRow row =
        sdb.getDirectWithOptions(0, "user:1".getBytes(), io.cobble.structured.ReadOptions.forColumns(0))) {
    ByteBuffer v = row.getBytes(0);
}
```

### 3) Encoded direct style (`DirectEncodedRow`) — recommended hot path

```java
try (DirectEncodedRow row = db.getDirectEncodedRowWithOptions(0, "user:1".getBytes(), ReadOptions.forColumns(0))) {
    byte[] v = row.decodeColumn(0, in -> decodeBytes(in));
}
```

```java
try (io.cobble.structured.DirectEncodedRow row =
        sdb.getDirectEncodedRowWithOptions(0, keyBuf, keyLen, io.cobble.structured.ReadOptions.forColumns(0))) {
    List<MyType> list = row.decodeListColumn(0, in -> decodeMyType(in));
}
```

### 4) Scan (local + distributed)

```java
try (ScanCursor cursor = db.scanWithOptions(0, start, end, ScanOptions.forColumns(0))) {
    for (ScanCursor.Entry entry = cursor.nextEntry();
            entry != null;
            entry = cursor.nextEntry()) {
        consume(entry.bucket, entry.key, entry.columns[0]);
    }
}
```

```java
try (DirectScanCursor cursor = db.scanDirectWithOptions(0, startBuf, startLen, endBuf, endLen, null)) {
    for (DirectScanEntry e : cursor) {
        int bucket = e.getBucket();
        ByteBuffer k = e.getKey();
    }
}
```

```java
try (io.cobble.structured.ScanCursor cursor =
        sdb.scanWithOptions(0, start, end, io.cobble.structured.ScanOptions.forColumns(0))) {
    for (io.cobble.structured.Row row = cursor.nextRow();
            row != null;
            row = cursor.nextRow()) {
        consume(row.getBucket(), row.getKey(), row.getBytes(0));
    }
}
```

```java
try (io.cobble.structured.DirectScanCursor cursor =
        sdb.scanDirectWithOptions(0, startBuf, startLen, endBuf, endLen, null)) {
    for (io.cobble.structured.DirectScanRow row = cursor.nextRow();
            row != null;
            row = cursor.nextRow()) {
        consume(row.getBucket(), row.getKey(), row.decodeBytesColumn(0, MyCodec::decode));
    }
}
```

The Java scan methods can accept open bounds:

- `byte[]` scan APIs (`Db` / `ReadOnlyDb` / structured `Db`) treat `null` `start` or `end` as an
  unbounded side.
- direct scan APIs treat `null` `ByteBuffer` plus length `0` as an unbounded side; JNI forwards
  that missing bound as native address `-1`.

Structured `DirectScanRow` exposes both the split bucket and the key as direct metadata; column
values are decoded on demand from the encoded row payload.

```java
ScanPlan plan = ScanPlan.fromGlobalSnapshot(manifest);
for (ScanSplit split : plan.splits()) {
    try (ScanCursor c = split.openScannerWithOptions("config.yaml", ScanOptions.forColumns(0))) {
        for (ScanCursor.Entry entry : c) {
            consume(entry.bucket, entry.key, entry.columns[0]);
        }
    }
}
```

Structured distributed scan behaves the same way: `StructuredScanSplit` is serializable, preserves
optional boundary metadata, and `splitAfter(bucket, keyInclusive)` returns `before` / `after`
halves around that row boundary.

## Structured Priority Queue APIs

Structured `Db` and `SingleDb` expose column-family-scoped priority queues through
`io.cobble.structured.PriorityQueue`.

```java
io.cobble.structured.Db db = io.cobble.structured.Db.open("config.yaml");

// Create a new queue, open an existing one, or open/create on first use.
PriorityQueue queue = db.getOrNewPriorityQueue("timers");
```

Each queue owns one dedicated structured column family with one bytes column. Items are ordered by
key within each bucket. Queue consumption is cursor based: advancing or polling moves a monotonic
cursor, making the consumed key and all earlier keys invisible. Offering an item at or before the
current cursor does not make it visible again.

`PriorityQueue` is an `AutoCloseable` native object. The parent `Db` or `SingleDb` only creates the
queue handle; after construction, queue operations go directly through the queue native handle,
which caches the resolved column-family metadata used by cursor operations. Close queue handles
before closing the parent DB handle.

### Heap-buffer APIs

```java
queue.offer(0, "timer:001".getBytes(), payload);
queue.offer(0, "timer:002".getBytes(), payload);

PriorityQueue.Entry next = queue.peek(0);       // does not advance
PriorityQueue.Entry consumed = queue.poll(0);   // advances to consumed key

List<PriorityQueue.Entry> fixed = queue.peekBatch(0, 128);
List<PriorityQueue.Entry> dynamic = queue.pollBatch(0);

queue.advance(0, "timer:010".getBytes());
byte[] cursor = queue.cursor(0);
queue.delete(0, "timer:020".getBytes());
```

- `newPriorityQueue(name)` creates a new queue and fails if it already exists.
- `getPriorityQueue(name)` opens an existing queue and validates that it is a queue column family.
- `getOrNewPriorityQueue(name)` opens an existing queue or creates it on first use.
- `peek(...)` / `peekBatch(...)` read without moving the cursor.
- `poll(...)` / `pollBatch(...)` return visible entries and advance once to the last consumed key.
- Passing an explicit `batchSize` bounds the returned count; passing `0` returns an empty batch.
- The no-`batchSize` batch overload returns one physical-boundary-sized batch, stopping after the
  next SST block, Parquet row group, or file boundary when the source has such a boundary.

### Direct-buffer APIs

Use direct APIs on timer or queue hot paths to avoid JNI heap-array materialization.

```java
ByteBuffer key = ByteBuffer.allocateDirect(64);
ByteBuffer value = ByteBuffer.allocateDirect(256);
// write key/value bytes into [0, keyLen) and [0, valueLen)

queue.offerDirect(0, key, keyLen, value, valueLen);

try (DirectPriorityQueueEntry entry = queue.peekDirect(0)) {
    if (entry != null) {
        ByteBuffer keyView = entry.getKey();
        ByteBuffer valueView = entry.getValue();
    }
}

try (DirectPriorityQueueBatch batch = queue.peekBatchDirect(0)) {
    for (DirectPriorityQueueEntry entry : batch) {
        ByteBuffer keyView = entry.getKey();
        ByteBuffer valueView = entry.getValue();
    }
}

queue.advance(0, key, keyLen);
```

Direct single-entry results own their payload and must be closed. Entries obtained from
`DirectPriorityQueueBatch` are borrowed views and stay valid only until the parent batch is closed.
There is no separate `advanceDirect` method; direct cursor advancement is exposed as the
`advance(int bucket, ByteBuffer keyBuffer, int keyLength)` overload.

## Performance Guidance

For hot paths, prefer direct buffer APIs.

| Style | Typical cost | Recommendation |
|---|---|---|
| `byte[]` | Most JNI copy/allocation | Good for simplicity |
| Direct view (`DirectColumns`/`DirectRow`) | Lower copy, lower allocation | Good default for performance |
| `DirectEncodedRow` | Lowest decode overhead, best control | **Best performance path** |

## Snapshot Lifecycle APIs

Distributed `Db` also exposes asynchronous snapshot lifecycle control for checkpoint-style integrations:

```java
PendingSnapshot<ShardSnapshot> pending = db.startAsyncSnapshot();
long snapshotId = pending.snapshotId();

// Cancel while the snapshot is still in flight.
boolean cancelled = db.cancelSnapshot(snapshotId);

// Or wait for the materialized shard snapshot payload.
ShardSnapshot snapshot = pending.future().get();

// Later, retain/expire the completed snapshot explicitly if needed.
db.retainSnapshot(snapshot.snapshotId);
db.expireSnapshot(snapshot.snapshotId);

// Out-of-band prune tool (no Db instance required):
SnapshotTools.pruneShardSnapshot(config, snapshot.dbId, snapshot.snapshotId);
```

- `asyncSnapshot()` returns only the `CompletableFuture<ShardSnapshot>`.
- `startAsyncSnapshot()` returns both the snapshot id and the future via `PendingSnapshot`.
- `cancelSnapshot(snapshotId)` only succeeds before manifest publication completes.
- `SnapshotTools.pruneShardSnapshot(config, dbId, snapshotId)` is the explicit out-of-band shard cleanup API.
- The same snapshot lifecycle APIs are also available on `io.cobble.structured.Db`.

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

The platforms included in the multi-platform jar are:

- `macos-aarch64`
- `macos-x86_64`
- `linux-x86_64`
- `linux-aarch64`
- `windows-x86_64`

For publishing, the same workflow also runs on **GitHub Release published** events and deploys the multi-platform build to **Maven Central** (via Sonatype `central-publishing-maven-plugin`).

Required repository secrets for release publishing:

- `MAVEN_CENTRAL_USERNAME` (Central Portal user token username)
- `MAVEN_CENTRAL_PASSWORD` (Central Portal user token password)
- `MAVEN_GPG_PRIVATE_KEY` (ASCII-armored private key, or base64-encoded armored key)
- `MAVEN_GPG_PASSPHRASE`
