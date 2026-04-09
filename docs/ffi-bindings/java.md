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
    <version>0.1.1</version>
</dependency>
```

## Java API Overview

### SingleDb

For single-machine embedded use. See [Single-Machine Embedded DB](../getting-started/single-db) for the full workflow.

```java
import io.cobble.SingleDb;
import io.cobble.Config;
import io.cobble.ReadOptions;

Config config = Config.fromPath("config.yaml");
SingleDb db = SingleDb.open(config);

// Write
db.put(0, "user:1".getBytes(), 0, "Alice".getBytes());

// Read
byte[] value = db.get(0, "user:1".getBytes(), 0);

// Snapshot and resume
long snapshotId = db.snapshot();
db.close();
db = SingleDb.resume(config, snapshotId);
```

### Db (Distributed)

For multi-shard deployments. See [Distributed Deployment](../getting-started/distributed) for the full workflow.

```java
import io.cobble.Db;
import io.cobble.ShardSnapshot;

Db db = Db.open(config);
db.put(0, "key".getBytes(), 0, "value".getBytes());

// Create snapshot input for coordinator
ShardSnapshot input = db.snapshot();
```

### Reader

For snapshot-following reads. Reader visibility advances when new snapshots are materialized and reader refresh picks them up. See [Reader & Distributed Scan](../getting-started/reader-and-scan).

```java
import io.cobble.Reader;

Reader reader = Reader.openCurrent(config);
byte[] value = reader.get(0, "key".getBytes(), 0);
reader.refresh();
```

### Structured Wrappers

Structured wrappers are schema-driven and currently support typed `Bytes` and `List` columns. See [Structured DB](../getting-started/structured-db).

```java
import io.cobble.structured.SingleDb;
import io.cobble.structured.ColumnValue;
import io.cobble.structured.Row;

SingleDb db = SingleDb.open(config);
db.updateSchema()
    .addListColumn(1, io.cobble.structured.ListConfig.of(100, io.cobble.structured.ListRetainMode.LAST))
    .commit();
db.put(0, "user:1".getBytes(), 0, ColumnValue.ofBytes("Alice".getBytes()));
Row row = db.get(0, "user:1".getBytes());
```

### Distributed Scan

For parallel analytical scans across shards. See [Reader & Distributed Scan](../getting-started/reader-and-scan).

```java
import io.cobble.ScanPlan;
import io.cobble.ScanSplit;
import io.cobble.ScanCursor;

ScanPlan plan = ScanPlan.fromGlobalSnapshot(manifest);
List<ScanSplit> splits = plan.splits();

for (ScanSplit split : splits) {
    try (ScanCursor cursor = split.openScannerWithOptions(config, scanOptions)) {
        for (ScanCursor.Entry entry : cursor) {
            byte[] key = entry.key;
            byte[][] columns = entry.columns;
        }
    }
}
```

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
