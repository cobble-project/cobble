package io.cobble;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.*;

class DbBindingTest {
    @Test
    void dbBulkReadWriteWithConfigJson() throws IOException {
        Path dataDir = Files.createTempDirectory("cobble-java-db-json-");
        Config config = new Config().addVolume(dataDir.toString()).numColumns(2).totalBuckets(1);

        try (Db db = Db.open(config)) {
            int count = 320;
            for (int i = 0; i < count; i++) {
                byte[] key = keyBytes("db-json", i);
                byte[] value = valueBytes("db-json-v", i);
                db.put(0, key, 0, value);
            }

            for (int i = 0; i < count; i++) {
                byte[] key = keyBytes("db-json", i);
                byte[] expected = valueBytes("db-json-v", i);
                assertArrayEquals(expected, db.get(0, key, 0));
            }

            try (ReadOptions options = ReadOptions.forColumn(0)) {
                for (int i = 0; i < count; i++) {
                    byte[] key = keyBytes("db-json", i);
                    byte[] expected = valueBytes("db-json-v", i);
                    assertArrayEquals(
                            expected, requiredSingleColumn(db.getWithOptions(0, key, options)));
                }
            }
            try (ReadOptions options = ReadOptions.forColumns(0, 1)) {
                for (int i = 0; i < count; i++) {
                    byte[] key = keyBytes("db-json", i);
                    byte[][] columns = db.getWithOptions(0, key, options);
                    assertNotNull(columns);
                    assertEquals(2, columns.length);
                    assertArrayEquals(valueBytes("db-json-v", i), columns[0]);
                    assertNull(columns[1]);
                }
            }
            for (int i = 0; i < count; i++) {
                byte[] key = keyBytes("db-json", i);
                byte[] expected = valueBytes("db-json-v", i);
                assertArrayEquals(expected, requiredSingleColumn(db.get(0, key)));
            }

            for (int i = 0; i < count; i += 3) {
                byte[] key = keyBytes("db-json", i);
                db.delete(0, key, 0);
            }

            for (int i = 0; i < count; i++) {
                byte[] key = keyBytes("db-json", i);
                byte[] value = db.get(0, key, 0);
                if (i % 3 == 0) {
                    assertNull(value);
                } else {
                    assertArrayEquals(valueBytes("db-json-v", i), value);
                }
            }
        }
    }

    @Test
    void singleDbBulkReadWriteWithConfigPath() throws IOException {
        Path dataDir = Files.createTempDirectory("cobble-java-single-path-");
        Path configPath =
                writeConfigFile(
                        dataDir,
                        new Config().addVolume(dataDir.toString()).numColumns(2).totalBuckets(1));

        try (SingleDb db = SingleDb.open(configPath.toString())) {
            int count = 280;
            for (int i = 0; i < count; i++) {
                byte[] key = keyBytes("single-path", i);
                byte[] value = valueBytes("single-path-v", i);
                db.put(0, key, 0, value);
            }

            for (int i = 0; i < count; i++) {
                byte[] key = keyBytes("single-path", i);
                assertArrayEquals(valueBytes("single-path-v", i), db.get(0, key, 0));
            }
            for (int i = 0; i < count; i++) {
                byte[] key = keyBytes("single-path", i);
                assertArrayEquals(
                        valueBytes("single-path-v", i), requiredSingleColumn(db.get(0, key)));
            }
            try (ReadOptions options = ReadOptions.forColumns(0, 1)) {
                for (int i = 0; i < count; i++) {
                    byte[] key = keyBytes("single-path", i);
                    byte[][] columns = db.getWithOptions(0, key, options);
                    assertNotNull(columns);
                    assertEquals(2, columns.length);
                    assertArrayEquals(valueBytes("single-path-v", i), columns[0]);
                    assertNull(columns[1]);
                }
            }

            for (int i = 1; i < count; i += 4) {
                db.delete(0, keyBytes("single-path", i), 0);
            }

            for (int i = 0; i < count; i++) {
                byte[] key = keyBytes("single-path", i);
                byte[] value = db.get(0, key, 0);
                if (i % 4 == 1) {
                    assertNull(value);
                } else {
                    assertArrayEquals(valueBytes("single-path-v", i), value);
                }
            }
        }
    }

    @Test
    void structuredDbBulkReadWriteWithConfigJson() throws IOException {
        Path dataDir = Files.createTempDirectory("cobble-java-structured-json-");
        Config config = new Config().addVolume(dataDir.toString()).numColumns(2).totalBuckets(1);

        try (StructuredDb db = StructuredDb.open(config)) {
            int count = 300;
            for (int i = 0; i < count; i++) {
                byte[] key = keyBytes("structured-json", i);
                byte[] value = valueBytes("structured-json-v", i);
                db.put(0, key, 0, value);
            }

            byte[] mergeKey = keyBytes("structured-json-merge", 1);
            db.put(0, mergeKey, 0, "base".getBytes(StandardCharsets.UTF_8));
            db.merge(0, mergeKey, 0, "-m1".getBytes(StandardCharsets.UTF_8));
            assertEquals("base-m1", new String(db.get(0, mergeKey, 0), StandardCharsets.UTF_8));
            try (WriteOptions options = WriteOptions.withTtl(60)) {
                db.putWithOptions(
                        0, mergeKey, 1, "ttl-structured".getBytes(StandardCharsets.UTF_8), options);
                db.mergeWithOptions(
                        0, mergeKey, 0, "-m2".getBytes(StandardCharsets.UTF_8), options);
            }
            assertEquals("base-m1-m2", new String(db.get(0, mergeKey, 0), StandardCharsets.UTF_8));
            assertArrayEquals(
                    "ttl-structured".getBytes(StandardCharsets.UTF_8), db.get(0, mergeKey, 1));

            for (int i = 0; i < count; i++) {
                byte[] key = keyBytes("structured-json", i);
                assertArrayEquals(valueBytes("structured-json-v", i), db.get(0, key, 0));
            }

            for (int i = 2; i < count; i += 5) {
                db.delete(0, keyBytes("structured-json", i), 0);
            }

            for (int i = 0; i < count; i++) {
                byte[] key = keyBytes("structured-json", i);
                byte[] value = db.get(0, key, 0);
                if (i % 5 == 2) {
                    assertNull(value);
                } else {
                    assertArrayEquals(valueBytes("structured-json-v", i), value);
                }
            }
        }
    }

    @Test
    void snapshotReadonlyReaderAndCoordinatorFlow() throws IOException {
        Path dataDir = Files.createTempDirectory("cobble-java-snapshot-");
        Config config = new Config().addVolume(dataDir.toString()).numColumns(2).totalBuckets(1);
        Path configPath = writeConfigFile(dataDir, config);
        try (Db db = Db.open(config)) {
            String dbId = db.id();
            int count = 220;
            for (int i = 0; i < count; i++) {
                db.put(0, keyBytes("snapshot", i), 0, valueBytes("snapshot-v", i));
            }
            Future<ShardSnapshot> shardSnapshotFuture = db.asyncSnapshot();
            ShardSnapshot shardSnapshot = awaitSnapshot(shardSnapshotFuture);
            assertNotNull(shardSnapshot);
            assertTrue(shardSnapshot.snapshotId >= 0);
            assertNotNull(shardSnapshot.manifestPath);
            assertFalse(shardSnapshot.manifestPath.isEmpty());
            assertNotNull(shardSnapshot.ranges);
            assertFalse(shardSnapshot.ranges.isEmpty());
            assertTrue(db.retainSnapshot(shardSnapshot.snapshotId));

            try (ReadOnlyDb readOnlyDb =
                    ReadOnlyDb.open(configPath.toString(), shardSnapshot.snapshotId, dbId)) {
                for (int i = 0; i < 220; i++) {
                    assertArrayEquals(
                            valueBytes("snapshot-v", i),
                            readOnlyDb.get(0, keyBytes("snapshot", i), 0));
                }
            }

            try (DbCoordinator coordinator = DbCoordinator.open(configPath.toString())) {
                GlobalSnapshot materialized =
                        coordinator.materializeGlobalSnapshot(
                                1,
                                shardSnapshot.snapshotId,
                                Collections.singletonList(shardSnapshot));
                assertNotNull(materialized);
                assertEquals(shardSnapshot.snapshotId, materialized.id);
                GlobalSnapshot globalSnapshot =
                        coordinator.getGlobalSnapshot(shardSnapshot.snapshotId);
                assertNotNull(globalSnapshot);
                assertEquals(shardSnapshot.snapshotId, globalSnapshot.id);
                assertNotNull(globalSnapshot.shardSnapshots);
                assertEquals(1, globalSnapshot.shardSnapshots.size());
                assertFalse(coordinator.listGlobalSnapshots().isEmpty());
            }

            try (Reader reader = Reader.openCurrent(configPath.toString())) {
                assertEquals("current", reader.readMode());
                assertEquals(-1L, reader.configuredSnapshotId());
                for (int i = 0; i < 220; i++) {
                    assertArrayEquals(
                            valueBytes("snapshot-v", i), reader.get(0, keyBytes("snapshot", i), 0));
                }
                reader.refresh();
                assertFalse(reader.listGlobalSnapshots().isEmpty());
            }
        }
    }

    @Test
    void snapshotRestoreAndResumeFlow() throws IOException {
        Path dataDir = Files.createTempDirectory("cobble-java-restore-");
        Config config = new Config().addVolume(dataDir.toString()).numColumns(2).totalBuckets(1);
        Path configPath = writeConfigFile(dataDir, config);
        String dbId;
        try (Db db = Db.open(config)) {
            dbId = db.id();
            int count = 180;
            for (int i = 0; i < count; i++) {
                db.put(0, keyBytes("restore", i), 0, valueBytes("restore-v", i));
            }
            ShardSnapshot shardSnapshot = db.snapshot();
            assertTrue(db.retainSnapshot(shardSnapshot.snapshotId));
            try (Db restored = Db.restore(configPath.toString(), shardSnapshot.snapshotId, dbId)) {
                for (int i = 0; i < 180; i++) {
                    assertArrayEquals(
                            valueBytes("restore-v", i), restored.get(0, keyBytes("restore", i), 0));
                }
            }
            try (Db resumed = Db.resume(configPath.toString(), dbId)) {
                for (int i = 0; i < 180; i++) {
                    assertArrayEquals(
                            valueBytes("restore-v", i), resumed.get(0, keyBytes("restore", i), 0));
                }
            }
        }
    }

    @Test
    void dbScanWithBatchCursor() throws IOException {
        Path dataDir = Files.createTempDirectory("cobble-java-db-scan-");
        Config config = new Config().addVolume(dataDir.toString()).numColumns(2).totalBuckets(1);
        try (Db db = Db.open(config)) {
            int count = 1200;
            for (int i = 0; i < count; i++) {
                byte[] key = scanKeyBytes("scan-db", i);
                db.put(0, key, 0, valueBytes("scan-db-v0", i));
                db.put(0, key, 1, valueBytes("scan-db-v1", i));
            }
            try (ScanOptions options =
                            new ScanOptions()
                                    .readAheadBytes(256 * 1024)
                                    .batchSize(128)
                                    .columns(0, 1);
                    ScanCursor cursor =
                            db.scanWithOptions(
                                    0,
                                    scanKeyBytes("scan-db", 100),
                                    scanKeyBytes("scan-db", 900),
                                    options)) {
                List<String> keys = new ArrayList<String>();
                List<String> value0 = new ArrayList<String>();
                List<String> value1 = new ArrayList<String>();
                while (true) {
                    ScanBatch batch = cursor.nextBatch();
                    assertNotNull(batch);
                    assertEquals(batch.keys.length, batch.values.length);
                    for (int i = 0; i < batch.keys.length; i++) {
                        keys.add(new String(batch.keys[i], StandardCharsets.UTF_8));
                        assertEquals(2, batch.values[i].length);
                        value0.add(new String(batch.values[i][0], StandardCharsets.UTF_8));
                        value1.add(new String(batch.values[i][1], StandardCharsets.UTF_8));
                    }
                    if (!batch.hasMore) {
                        break;
                    }
                    assertNotNull(batch.nextStartAfterExclusive);
                }
                assertFalse(keys.isEmpty());
                for (int i = 0; i < keys.size(); i++) {
                    String key = keys.get(i);
                    int index = Integer.parseInt(key.substring(key.lastIndexOf('-') + 1));
                    assertTrue(index >= 100 && index < 900);
                    assertEquals("scan-db-v0-value-" + index + "-payload", value0.get(i));
                    assertEquals("scan-db-v1-value-" + index + "-payload", value1.get(i));
                }
            }
            try (ScanCursor cursor =
                    db.scan(0, scanKeyBytes("scan-db", 100), scanKeyBytes("scan-db", 900))) {
                int rows = 0;
                while (true) {
                    ScanBatch batch = cursor.nextBatch();
                    rows += batch.keys.length;
                    for (int i = 0; i < batch.values.length; i++) {
                        assertEquals(1, batch.values[i].length);
                    }
                    if (!batch.hasMore) {
                        break;
                    }
                }
                assertTrue(rows > 0);
            }
        }
    }

    @Test
    void dbScanReusesNativeScanOptionsHandle() throws IOException {
        Path dataDir = Files.createTempDirectory("cobble-java-db-scan-reuse-");
        Config config = new Config().addVolume(dataDir.toString()).numColumns(2).totalBuckets(1);
        try (Db db = Db.open(config)) {
            int count = 220;
            for (int i = 0; i < count; i++) {
                byte[] key = scanKeyBytes("scan-reuse", i);
                db.put(0, key, 0, valueBytes("scan-reuse-v0", i));
                db.put(0, key, 1, valueBytes("scan-reuse-v1", i));
            }
            try (ScanOptions options = new ScanOptions().batchSize(64).columns(0, 1)) {
                int firstRows = 0;
                try (ScanCursor first =
                        db.scanWithOptions(
                                0,
                                scanKeyBytes("scan-reuse", 0),
                                scanKeyBytes("scan-reuse", 220),
                                options)) {
                    while (true) {
                        ScanBatch batch = first.nextBatch();
                        for (int i = 0; i < batch.values.length; i++) {
                            assertEquals(2, batch.values[i].length);
                        }
                        firstRows += batch.keys.length;
                        if (!batch.hasMore) {
                            break;
                        }
                    }
                }
                assertTrue(firstRows > 0);

                options.readAheadBytes(64 * 1024).batchSize(32);
                int secondRows = 0;
                try (ScanCursor second =
                        db.scanWithOptions(
                                0,
                                scanKeyBytes("scan-reuse", 0),
                                scanKeyBytes("scan-reuse", 220),
                                options)) {
                    while (true) {
                        ScanBatch batch = second.nextBatch();
                        for (int i = 0; i < batch.values.length; i++) {
                            assertEquals(2, batch.values[i].length);
                        }
                        secondRows += batch.keys.length;
                        if (!batch.hasMore) {
                            break;
                        }
                    }
                }
                assertEquals(firstRows, secondRows);
            }
        }
    }

    @Test
    void dbScanLargeDatasetWithProjectedColumnsOverWideSchema() throws IOException {
        Path dataDir = Files.createTempDirectory("cobble-java-db-scan-large-");
        Config config = new Config().addVolume(dataDir.toString()).numColumns(4).totalBuckets(1);
        final int rowCount = 18_000;
        final int selectedColumnPayloadBytes = 4 * 1024;
        final long minimumScannedBytes = 128L * 1024L * 1024L;

        try (Db db = Db.open(config)) {
            for (int i = 0; i < rowCount; i++) {
                byte[] key = scanKeyBytes("scan-large", i);
                db.put(0, key, 0, largeValueBytes("scan-large-c0", i, selectedColumnPayloadBytes));
                db.put(0, key, 1, valueBytes("scan-large-c1", i));
                db.put(0, key, 2, largeValueBytes("scan-large-c2", i, selectedColumnPayloadBytes));
            }

            long scannedBytes = 0L;
            int scannedRows = 0;
            try (ScanOptions options = new ScanOptions().batchSize(256).columns(0, 2);
                    ScanCursor cursor =
                            db.scanWithOptions(
                                    0,
                                    scanKeyBytes("scan-large", 0),
                                    scanKeyBytes("scan-large", rowCount + 1),
                                    options)) {
                while (true) {
                    ScanBatch batch = cursor.nextBatch();
                    assertEquals(batch.keys.length, batch.values.length);
                    for (int i = 0; i < batch.values.length; i++) {
                        assertEquals(
                                2,
                                batch.values[i].length,
                                "only selected columns should be returned");
                        scannedBytes += batch.values[i][0].length;
                        scannedBytes += batch.values[i][1].length;
                        scannedRows++;
                    }
                    if (!batch.hasMore) {
                        break;
                    }
                }
            }

            assertTrue(scannedRows > 0);
            assertTrue(
                    scannedBytes >= minimumScannedBytes,
                    "expected scanned bytes >= 128MB, actual=" + scannedBytes);
        }
    }

    @Test
    void dbMergeAndWriteOptionsWithTtl() throws IOException {
        Path dataDir = Files.createTempDirectory("cobble-java-db-merge-ttl-");
        Config config = new Config().addVolume(dataDir.toString()).numColumns(2).totalBuckets(1);
        config.ttlEnabled = true;
        config.defaultTtlSeconds = null;
        config.timeProvider = Config.TimeProviderKind.MANUAL;

        try (Db db = Db.open(config)) {
            byte[] key = keyBytes("db-merge-ttl", 1);
            db.put(0, key, 0, "a".getBytes(StandardCharsets.UTF_8));
            db.merge(0, key, 0, "b".getBytes(StandardCharsets.UTF_8));
            db.merge(0, key, 0, "c".getBytes(StandardCharsets.UTF_8));
            assertEquals("abc", new String(db.get(0, key, 0), StandardCharsets.UTF_8));

            try (WriteOptions options = WriteOptions.withTtl(10)) {
                db.putWithOptions(0, key, 1, "ttl-put".getBytes(StandardCharsets.UTF_8), options);
            }

            assertArrayEquals("ttl-put".getBytes(StandardCharsets.UTF_8), db.get(0, key, 1));
            db.setTime(11);
            assertNull(db.get(0, key, 1), "ttl put should expire after manual time advances");

            byte[] mergeTtlKey = keyBytes("db-merge-ttl", 2);
            db.put(0, mergeTtlKey, 0, "x".getBytes(StandardCharsets.UTF_8));
            try (WriteOptions options = new WriteOptions().ttlSeconds(10)) {
                db.mergeWithOptions(
                        0, mergeTtlKey, 0, "y".getBytes(StandardCharsets.UTF_8), options);
            }
            assertEquals("xy", new String(db.get(0, mergeTtlKey, 0), StandardCharsets.UTF_8));
            db.setTime(22);
            assertEquals(
                    "x",
                    new String(db.get(0, mergeTtlKey, 0), StandardCharsets.UTF_8),
                    "expired merge operand should fall back to base put value");

            byte[] mergeOnlyTtlKey = keyBytes("db-merge-ttl", 3);
            try (WriteOptions options = new WriteOptions().ttlSeconds(5)) {
                db.mergeWithOptions(
                        0, mergeOnlyTtlKey, 0, "z".getBytes(StandardCharsets.UTF_8), options);
            }
            assertNotNull(db.get(0, mergeOnlyTtlKey, 0));
            db.setTime(28);
            assertNull(
                    db.get(0, mergeOnlyTtlKey, 0),
                    "ttl merge should expire merge-only value after manual time advances");
        }
    }

    @Test
    void singleDbMergeAndWriteOptionsWithTtl() throws IOException {
        Path dataDir = Files.createTempDirectory("cobble-java-single-merge-ttl-");
        Config config = new Config().addVolume(dataDir.toString()).numColumns(2).totalBuckets(1);
        config.ttlEnabled = true;
        config.defaultTtlSeconds = null;
        config.timeProvider = Config.TimeProviderKind.MANUAL;

        try (SingleDb db = SingleDb.open(config)) {
            byte[] key = keyBytes("single-merge-ttl", 1);
            db.put(0, key, 0, "m".getBytes(StandardCharsets.UTF_8));
            db.merge(0, key, 0, "n".getBytes(StandardCharsets.UTF_8));
            assertEquals("mn", new String(db.get(0, key, 0), StandardCharsets.UTF_8));

            try (WriteOptions options = WriteOptions.withTtl(5)) {
                db.putWithOptions(0, key, 1, "ttl".getBytes(StandardCharsets.UTF_8), options);
            }
            assertArrayEquals("ttl".getBytes(StandardCharsets.UTF_8), db.get(0, key, 1));
            db.setTime(6);
            assertNull(db.get(0, key, 1));

            byte[] mergeTtlKey = keyBytes("single-merge-ttl", 2);
            db.put(0, mergeTtlKey, 0, "p".getBytes(StandardCharsets.UTF_8));
            try (WriteOptions options = new WriteOptions().ttlSeconds(5)) {
                db.mergeWithOptions(
                        0, mergeTtlKey, 0, "q".getBytes(StandardCharsets.UTF_8), options);
            }
            assertEquals("pq", new String(db.get(0, mergeTtlKey, 0), StandardCharsets.UTF_8));
            db.setTime(12);
            assertEquals("p", new String(db.get(0, mergeTtlKey, 0), StandardCharsets.UTF_8));

            byte[] mergeOnlyTtlKey = keyBytes("single-merge-ttl", 3);
            try (WriteOptions options = new WriteOptions().ttlSeconds(3)) {
                db.mergeWithOptions(
                        0, mergeOnlyTtlKey, 0, "r".getBytes(StandardCharsets.UTF_8), options);
            }
            assertNotNull(db.get(0, mergeOnlyTtlKey, 0));
            db.setTime(16);
            assertNull(db.get(0, mergeOnlyTtlKey, 0));
        }
    }

    @Test
    void readOnlyAndReaderScanWithBatchCursor() throws IOException {
        Path dataDir = Files.createTempDirectory("cobble-java-ro-reader-scan-");
        Config config = new Config().addVolume(dataDir.toString()).numColumns(2).totalBuckets(1);
        Path configPath = writeConfigFile(dataDir, config);
        try (Db db = Db.open(config)) {
            String dbId = db.id();
            int count = 950;
            for (int i = 0; i < count; i++) {
                byte[] key = scanKeyBytes("scan-ro", i);
                db.put(0, key, 0, valueBytes("scan-ro-v0", i));
                db.put(0, key, 1, valueBytes("scan-ro-v1", i));
            }
            ShardSnapshot shardSnapshot = db.snapshot();
            assertTrue(db.retainSnapshot(shardSnapshot.snapshotId));
            try (ReadOnlyDb readOnlyDb =
                    ReadOnlyDb.open(configPath.toString(), shardSnapshot.snapshotId, dbId)) {
                assertArrayEquals(
                        valueBytes("scan-ro-v0", 10),
                        requiredSingleColumn(readOnlyDb.get(0, scanKeyBytes("scan-ro", 10))));
                try (ReadOptions readOptions = ReadOptions.forColumn(0)) {
                    assertArrayEquals(
                            valueBytes("scan-ro-v0", 10),
                            requiredSingleColumn(
                                    readOnlyDb.getWithOptions(
                                            0, scanKeyBytes("scan-ro", 10), readOptions)));
                }
                try (ReadOptions readOptions = ReadOptions.forColumns(0, 1)) {
                    byte[][] columns =
                            readOnlyDb.getWithOptions(0, scanKeyBytes("scan-ro", 10), readOptions);
                    assertNotNull(columns);
                    assertEquals(2, columns.length);
                    assertArrayEquals(valueBytes("scan-ro-v0", 10), columns[0]);
                    assertArrayEquals(valueBytes("scan-ro-v1", 10), columns[1]);
                }
                try (ScanOptions options = new ScanOptions().batchSize(96).columns(0, 1);
                        ScanCursor cursor =
                                readOnlyDb.scanWithOptions(
                                        0,
                                        scanKeyBytes("scan-ro", 10),
                                        scanKeyBytes("scan-ro", 510),
                                        options)) {
                    int rows = 0;
                    String first = null;
                    String last = null;
                    while (true) {
                        ScanBatch batch = cursor.nextBatch();
                        for (int i = 0; i < batch.keys.length; i++) {
                            String key = new String(batch.keys[i], StandardCharsets.UTF_8);
                            assertEquals(2, batch.values[i].length);
                            String value0 = new String(batch.values[i][0], StandardCharsets.UTF_8);
                            String value1 = new String(batch.values[i][1], StandardCharsets.UTF_8);
                            if (first == null) {
                                first = key + "|" + value0 + "|" + value1;
                            }
                            last = key + "|" + value0 + "|" + value1;
                            rows++;
                        }
                        if (!batch.hasMore) {
                            break;
                        }
                    }
                    assertEquals(500, rows);
                    assertEquals(
                            scanKeyString("scan-ro", 10)
                                    + "|scan-ro-v0-value-10-payload|scan-ro-v1-value-10-payload",
                            first);
                    assertEquals(
                            scanKeyString("scan-ro", 509)
                                    + "|scan-ro-v0-value-509-payload|scan-ro-v1-value-509-payload",
                            last);
                }
            }
            try (DbCoordinator coordinator = DbCoordinator.open(configPath.toString())) {
                coordinator.materializeGlobalSnapshot(
                        1, shardSnapshot.snapshotId, Collections.singletonList(shardSnapshot));
            }
            try (Reader reader = Reader.openCurrent(configPath.toString())) {
                assertArrayEquals(
                        valueBytes("scan-ro-v0", 400),
                        requiredSingleColumn(reader.get(0, scanKeyBytes("scan-ro", 400))));
                try (ReadOptions readOptions = ReadOptions.forColumn(0)) {
                    assertArrayEquals(
                            valueBytes("scan-ro-v0", 400),
                            requiredSingleColumn(
                                    reader.getWithOptions(
                                            0, scanKeyBytes("scan-ro", 400), readOptions)));
                }
                try (ReadOptions readOptions = ReadOptions.forColumns(0, 1)) {
                    byte[][] columns =
                            reader.getWithOptions(0, scanKeyBytes("scan-ro", 400), readOptions);
                    assertNotNull(columns);
                    assertEquals(2, columns.length);
                    assertArrayEquals(valueBytes("scan-ro-v0", 400), columns[0]);
                    assertArrayEquals(valueBytes("scan-ro-v1", 400), columns[1]);
                }
                try (ScanOptions options =
                                new ScanOptions()
                                        .batchSize(80)
                                        .readAheadBytes(128 * 1024)
                                        .columns(0, 1);
                        ScanCursor cursor =
                                reader.scanWithOptions(
                                        0,
                                        scanKeyBytes("scan-ro", 400),
                                        scanKeyBytes("scan-ro", 900),
                                        options)) {
                    int rows = 0;
                    String first = null;
                    String last = null;
                    while (true) {
                        ScanBatch batch = cursor.nextBatch();
                        for (int i = 0; i < batch.keys.length; i++) {
                            String key = new String(batch.keys[i], StandardCharsets.UTF_8);
                            assertEquals(2, batch.values[i].length);
                            String value0 = new String(batch.values[i][0], StandardCharsets.UTF_8);
                            String value1 = new String(batch.values[i][1], StandardCharsets.UTF_8);
                            if (first == null) {
                                first = key + "|" + value0 + "|" + value1;
                            }
                            last = key + "|" + value0 + "|" + value1;
                            rows++;
                        }
                        if (!batch.hasMore) {
                            break;
                        }
                    }
                    assertEquals(500, rows);
                    assertEquals(
                            scanKeyString("scan-ro", 400)
                                    + "|scan-ro-v0-value-400-payload|scan-ro-v1-value-400-payload",
                            first);
                    assertEquals(
                            scanKeyString("scan-ro", 899)
                                    + "|scan-ro-v0-value-899-payload|scan-ro-v1-value-899-payload",
                            last);
                }
            }
        }
    }

    private static ShardSnapshot awaitSnapshot(Future<ShardSnapshot> future) {
        try {
            return future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while waiting snapshot future", e);
        } catch (ExecutionException e) {
            throw new IllegalStateException("snapshot future failed", e);
        }
    }

    private static Path writeConfigFile(Path dataDir, Config config) throws IOException {
        Files.createDirectories(dataDir);
        Path configPath = Files.createTempFile("cobble-java-config-", ".json");
        Files.write(configPath, config.toJson().getBytes(StandardCharsets.UTF_8));
        return configPath;
    }

    private static byte[] requiredSingleColumn(byte[][] columns) {
        if (columns == null) {
            return null;
        }
        if (columns.length != 1) {
            throw new IllegalStateException("expected exactly one column, got " + columns.length);
        }
        return columns[0];
    }

    private static byte[] keyBytes(String prefix, int i) {
        return (prefix + "-k-" + i).getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] scanKeyBytes(String prefix, int i) {
        return scanKeyString(prefix, i).getBytes(StandardCharsets.UTF_8);
    }

    private static String scanKeyString(String prefix, int i) {
        return String.format("%s-k-%05d", prefix, i);
    }

    private static byte[] valueBytes(String prefix, int i) {
        return (prefix + "-value-" + i + "-payload").getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] largeValueBytes(String prefix, int i, int bytes) {
        if (bytes <= 0) {
            throw new IllegalArgumentException("bytes must be > 0");
        }
        byte[] value = new byte[bytes];
        byte[] marker = (prefix + "-" + i + "|").getBytes(StandardCharsets.UTF_8);
        int markerLength = Math.min(marker.length, value.length);
        System.arraycopy(marker, 0, value, 0, markerLength);
        byte fill = (byte) ('a' + (i % 26));
        for (int pos = markerLength; pos < value.length; pos++) {
            value[pos] = fill;
        }
        return value;
    }
}
