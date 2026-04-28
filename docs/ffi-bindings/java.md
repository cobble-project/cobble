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
try (DirectScanCursor cursor = db.scanDirectWithOptions(0, startBuf, startLen, endBuf, endLen, null)) {
    for (DirectScanEntry e : cursor) {
        ByteBuffer k = e.getKey();
    }
}
```

```java
try (io.cobble.structured.DirectScanCursor cursor =
        sdb.scanDirectWithOptions(0, startBuf, startLen, endBuf, endLen, null)) {
    while (true) {
        try (io.cobble.structured.DirectEncodedScanBatch batch = cursor.nextEncodedBatch()) {
            if (batch.size() == 0) {
                break;
            }
            for (io.cobble.structured.DirectEncodedScanRow row = batch.nextRow();
                    row != null;
                    row = batch.nextRow()) {
                consume(row.getKey(), row.decodeBytesColumn(0, MyCodec::decode));
            }
        }
    }
}
```

```java
ScanPlan plan = ScanPlan.fromGlobalSnapshot(manifest);
for (ScanSplit split : plan.splits()) {
    try (ScanCursor c = split.openScannerWithOptions("config.yaml", ScanOptions.forColumns(0))) {}
}
```

## Performance Guidance

For hot paths, prefer direct buffer APIs.

| Style | Typical cost | Recommendation |
|---|---|---|
| `byte[]` | Most JNI copy/allocation | Good for simplicity |
| Direct view (`DirectColumns`/`DirectRow`) | Lower copy, lower allocation | Good default for performance |
| `DirectEncodedRow` | Lowest decode overhead, best control | **Best performance path** |

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
