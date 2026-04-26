package io.cobble;

import io.cobble.structured.ColumnValue;
import io.cobble.structured.Row;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

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

            // Verify scan and scanWithOptions
            byte[] scanStart = new byte[] {0};
            byte[] scanEnd = new byte[] {(byte) 0xFF};
            int scannedRows = 0;
            try (ScanCursor cursor = db.scan(0, scanStart, scanEnd)) {
                while (true) {
                    ScanBatch batch = cursor.nextBatch();
                    scannedRows += batch.keys.length;
                    if (!batch.hasMore) {
                        break;
                    }
                }
            }
            int expectedAlive = count - ((count - 1) / 4 + (1 < count ? 0 : -1));
            // count=280, deleted every i%4==1 from i=1: 70 deleted, 210 alive
            assertEquals(210, scannedRows);

            try (ScanOptions scanOptions = new ScanOptions().columns(0)) {
                try (ScanCursor cursor = db.scanWithOptions(0, scanStart, scanEnd, scanOptions)) {
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
                    assertEquals(210, rows);
                }
            }
        }
    }

    @Test
    void structuredDbBulkReadWriteWithConfigJson() throws IOException {
        Path dataDir = Files.createTempDirectory("cobble-java-structured-json-");
        Config config = new Config().addVolume(dataDir.toString()).numColumns(2).totalBuckets(1);

        try (io.cobble.structured.Db db = io.cobble.structured.Db.open(config)) {
            int count = 300;
            for (int i = 0; i < count; i++) {
                byte[] key = keyBytes("structured-json", i);
                byte[] value = valueBytes("structured-json-v", i);
                db.put(0, key, 0, value);
            }

            byte[] mergeKey = keyBytes("structured-json-merge", 1);
            db.put(0, mergeKey, 0, "base".getBytes(StandardCharsets.UTF_8));
            db.merge(0, mergeKey, 0, "-m1".getBytes(StandardCharsets.UTF_8));
            Row mergeRow = db.get(0, mergeKey);
            assertEquals("base-m1", new String(mergeRow.getBytes(0), StandardCharsets.UTF_8));
            try (io.cobble.structured.WriteOptions options =
                    io.cobble.structured.WriteOptions.withTtl(60)) {
                db.putWithOptions(
                        0,
                        mergeKey,
                        1,
                        ColumnValue.ofBytes("ttl-structured".getBytes(StandardCharsets.UTF_8)),
                        options);
                db.mergeWithOptions(
                        0,
                        mergeKey,
                        0,
                        ColumnValue.ofBytes("-m2".getBytes(StandardCharsets.UTF_8)),
                        options);
            }
            assertEquals(
                    "base-m1-m2",
                    new String(db.get(0, mergeKey).getBytes(0), StandardCharsets.UTF_8));
            assertArrayEquals(
                    "ttl-structured".getBytes(StandardCharsets.UTF_8),
                    db.get(0, mergeKey).getBytes(1));

            for (int i = 0; i < count; i++) {
                byte[] key = keyBytes("structured-json", i);
                assertArrayEquals(valueBytes("structured-json-v", i), db.get(0, key).getBytes(0));
            }

            for (int i = 2; i < count; i += 5) {
                db.delete(0, keyBytes("structured-json", i), 0);
            }

            for (int i = 0; i < count; i++) {
                byte[] key = keyBytes("structured-json", i);
                Row row = db.get(0, key);
                byte[] value = row != null ? row.getBytes(0) : null;
                if (i % 5 == 2) {
                    assertNull(value);
                } else {
                    assertArrayEquals(valueBytes("structured-json-v", i), value);
                }
            }
        }
    }

    @Test
    void rawWithOptionsNullFallsBackToKernelNoOptions() throws IOException {
        Path dataDir = Files.createTempDirectory("cobble-java-raw-null-options-");
        Config config = new Config().addVolume(dataDir.toString()).numColumns(2).totalBuckets(1);

        try (Db db = Db.open(config)) {
            byte[] key = "null-opt-key".getBytes(StandardCharsets.UTF_8);
            byte[] value = "null-opt-value".getBytes(StandardCharsets.UTF_8);

            db.putWithOptions(0, key, 0, value, null);
            assertArrayEquals(value, db.get(0, key, 0));

            byte[][] row = db.getWithOptions(0, key, (ReadOptions) null);
            assertNotNull(row);
            assertArrayEquals(value, row[0]);

            try (ScanCursor cursor =
                    db.scanWithOptions(0, key, "zz".getBytes(StandardCharsets.UTF_8), null)) {
                ScanBatch batch = cursor.nextBatch();
                assertTrue(batch.keys.length >= 1);
            }

            db.deleteWithOptions(0, key, 0, null);
            assertNull(db.get(0, key, 0));
        }
    }

    @Test
    void structuredWithOptionsNullFallsBackToKernelNoOptions() throws IOException {
        Path dataDir = Files.createTempDirectory("cobble-java-structured-null-options-");
        Config config = new Config().addVolume(dataDir.toString()).numColumns(2).totalBuckets(1);

        try (io.cobble.structured.Db db = io.cobble.structured.Db.open(config)) {
            byte[] key = "st-null-opt-key".getBytes(StandardCharsets.UTF_8);
            byte[] value = "st-null-opt-value".getBytes(StandardCharsets.UTF_8);

            db.putWithOptions(0, key, 0, ColumnValue.ofBytes(value), null);
            Row row = db.getWithOptions(0, key, null);
            assertNotNull(row);
            assertArrayEquals(value, row.getBytes(0));

            try (io.cobble.structured.ScanCursor cursor =
                    db.scanWithOptions(0, key, "zz".getBytes(StandardCharsets.UTF_8), null)) {
                io.cobble.structured.ScanBatch batch = cursor.nextBatch();
                assertTrue(batch.size() >= 1);
            }

            db.deleteWithOptions(0, key, 0, null);
            Row deleted = db.get(0, key);
            assertTrue(deleted == null || deleted.getColumnValue(0) == null);
        }
    }

    @Test
    void structuredDbScanAndRawGet() throws IOException {
        Path dataDir = Files.createTempDirectory("cobble-java-structured-scan-");
        Config config = new Config().addVolume(dataDir.toString()).numColumns(2).totalBuckets(1);

        try (io.cobble.structured.Db db = io.cobble.structured.Db.open(config)) {
            int count = 500;
            for (int i = 0; i < count; i++) {
                byte[] key = scanKeyBytes("st-scan", i);
                db.put(0, key, 0, valueBytes("st-scan-v0", i));
            }

            // single-column get
            assertArrayEquals(
                    valueBytes("st-scan-v0", 42),
                    db.get(0, scanKeyBytes("st-scan", 42)).getBytes(0));

            // get with ReadOptions column projection
            try (io.cobble.structured.ReadOptions readOpts =
                    io.cobble.structured.ReadOptions.forColumns(0)) {
                Row projected = db.getWithOptions(0, scanKeyBytes("st-scan", 42), readOpts);
                assertNotNull(projected);
                assertArrayEquals(valueBytes("st-scan-v0", 42), projected.getBytes(0));
            }

            // scan with options
            try (io.cobble.structured.ScanOptions scanOpts =
                            new io.cobble.structured.ScanOptions().batchSize(64).columns(0);
                    io.cobble.structured.ScanCursor cursor =
                            db.scanWithOptions(
                                    0,
                                    scanKeyBytes("st-scan", 100),
                                    scanKeyBytes("st-scan", 300),
                                    scanOpts)) {
                int rows = 0;
                while (true) {
                    io.cobble.structured.ScanBatch batch = cursor.nextBatch();
                    assertNotNull(batch);
                    rows += batch.size();
                    if (!batch.hasMore) {
                        break;
                    }
                }
                assertEquals(200, rows);
            }

            // default scan (no options)
            try (io.cobble.structured.ScanCursor cursor =
                    db.scan(0, scanKeyBytes("st-scan", 0), scanKeyBytes("st-scan", count))) {
                int rows = 0;
                while (true) {
                    io.cobble.structured.ScanBatch batch = cursor.nextBatch();
                    rows += batch.size();
                    if (!batch.hasMore) {
                        break;
                    }
                }
                assertEquals(count, rows);
            }

            // id and time metadata
            assertNotNull(db.id());
            assertTrue(db.nowSeconds() >= 0);
        }
    }

    @Test
    void structuredDbSnapshotRestoreAndResume() throws IOException {
        Path dataDir = Files.createTempDirectory("cobble-java-structured-snap-");
        Config config = new Config().addVolume(dataDir.toString()).numColumns(2).totalBuckets(1);
        Path configPath = writeConfigFile(dataDir, config);
        String dbId;
        try (io.cobble.structured.Db db = io.cobble.structured.Db.open(config)) {
            dbId = db.id();
            int count = 180;
            for (int i = 0; i < count; i++) {
                db.put(0, keyBytes("st-snap", i), 0, valueBytes("st-snap-v", i));
            }

            ShardSnapshot shardSnapshot = db.snapshot();
            assertNotNull(shardSnapshot);
            assertTrue(shardSnapshot.snapshotId >= 0);
            assertTrue(db.retainSnapshot(shardSnapshot.snapshotId));

            // restore
            try (io.cobble.structured.Db restored =
                    io.cobble.structured.Db.restore(
                            configPath.toString(), shardSnapshot.snapshotId, dbId)) {
                for (int i = 0; i < count; i++) {
                    assertArrayEquals(
                            valueBytes("st-snap-v", i),
                            restored.get(0, keyBytes("st-snap", i)).getBytes(0));
                }
            }

            try (io.cobble.structured.Db restoredNew =
                    io.cobble.structured.Db.restore(
                            configPath.toString(), shardSnapshot.snapshotId, dbId, true)) {
                assertNotEquals(dbId, restoredNew.id());
                for (int i = 0; i < count; i++) {
                    assertArrayEquals(
                            valueBytes("st-snap-v", i),
                            restoredNew.get(0, keyBytes("st-snap", i)).getBytes(0));
                }
            }

            // resume
            try (io.cobble.structured.Db resumed =
                    io.cobble.structured.Db.resume(configPath.toString(), dbId)) {
                for (int i = 0; i < count; i++) {
                    assertArrayEquals(
                            valueBytes("st-snap-v", i),
                            resumed.get(0, keyBytes("st-snap", i)).getBytes(0));
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
            CompletableFuture<ShardSnapshot> shardSnapshotFuture = db.asyncSnapshot();
            ShardSnapshot shardSnapshot = awaitSnapshot(shardSnapshotFuture);
            assertNotNull(shardSnapshot);
            assertTrue(shardSnapshot.snapshotId >= 0);
            assertNotNull(shardSnapshot.manifestPath);
            assertFalse(shardSnapshot.manifestPath.isEmpty());
            assertNotNull(shardSnapshot.ranges);
            assertFalse(shardSnapshot.ranges.isEmpty());
            assertNotNull(shardSnapshot.columnFamilyIds);
            assertEquals(Integer.valueOf(0), shardSnapshot.columnFamilyIds.get("default"));
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
                assertNotNull(globalSnapshot.columnFamilyIds);
                assertEquals(Integer.valueOf(0), globalSnapshot.columnFamilyIds.get("default"));
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
    void coordinatorRetentionAndRetainExpireFlow() throws IOException {
        Path dataDir = Files.createTempDirectory("cobble-java-coordinator-retention-");
        Config config = new Config().addVolume(dataDir.toString()).numColumns(2).totalBuckets(1);
        config.snapshotRetention = 1;
        Path configPath = writeConfigFile(dataDir, config);

        try (Db db = Db.open(configPath.toString());
                DbCoordinator coordinator = DbCoordinator.open(configPath.toString())) {
            db.put(0, keyBytes("coord-ret", 1), 0, valueBytes("coord-ret-v", 1));
            ShardSnapshot s1 = db.snapshot();
            GlobalSnapshot g1 =
                    coordinator.materializeGlobalSnapshot(
                            1, s1.snapshotId, Collections.singletonList(s1));
            assertTrue(coordinator.retainSnapshot(g1.id));

            db.put(0, keyBytes("coord-ret", 2), 0, valueBytes("coord-ret-v", 2));
            ShardSnapshot s2 = db.snapshot();
            GlobalSnapshot g2 =
                    coordinator.materializeGlobalSnapshot(
                            1, s2.snapshotId, Collections.singletonList(s2));

            List<GlobalSnapshot> listed = coordinator.listGlobalSnapshots();
            assertEquals(2, listed.size());
            assertTrue(listed.stream().anyMatch(s -> s.id == g1.id));
            assertTrue(listed.stream().anyMatch(s -> s.id == g2.id));

            assertTrue(coordinator.expireSnapshot(g1.id));
            listed = coordinator.listGlobalSnapshots();
            assertEquals(1, listed.size());
            assertEquals(g2.id, listed.get(0).id);
        }
    }

    @Test
    void singleDbSnapshotRetainExpireAndListSnapshots() throws IOException {
        Path dataDir = Files.createTempDirectory("cobble-java-single-snapshot-lifecycle-");
        Config config = new Config().addVolume(dataDir.toString()).numColumns(2).totalBuckets(1);
        config.snapshotRetention = 1;

        try (SingleDb db = SingleDb.open(config)) {
            db.put(0, keyBytes("single-snap", 1), 0, valueBytes("single-snap-v", 1));
            GlobalSnapshot global1 = db.snapshot();
            assertTrue(db.retainSnapshot(global1.id));

            db.put(0, keyBytes("single-snap", 2), 0, valueBytes("single-snap-v", 2));
            GlobalSnapshot global2 = db.snapshot();

            List<GlobalSnapshot> listed = db.listSnapshots();
            assertEquals(2, listed.size());
            assertTrue(listed.stream().anyMatch(s -> s.id == global1.id));
            assertTrue(listed.stream().anyMatch(s -> s.id == global2.id));

            assertTrue(db.expireSnapshot(global1.id));
            listed = db.listSnapshots();
            assertEquals(1, listed.size());
            assertEquals(global2.id, listed.get(0).id);
        }
    }

    @Test
    void singleDbResumeFromGlobalSnapshot() throws IOException {
        Path dataDir = Files.createTempDirectory("cobble-java-single-resume-");
        Config config = new Config().addVolume(dataDir.toString()).numColumns(2).totalBuckets(1);
        Path configPath = writeConfigFile(dataDir, config);

        long globalSnapshotId;
        try (SingleDb db = SingleDb.open(configPath.toString())) {
            for (int i = 0; i < 120; i++) {
                db.put(0, keyBytes("single-resume", i), 0, valueBytes("single-resume-v", i));
            }
            GlobalSnapshot globalSnapshot = db.snapshot();
            assertNotNull(globalSnapshot);
            globalSnapshotId = globalSnapshot.id;
            assertTrue(db.retainSnapshot(globalSnapshotId));
        }

        try (SingleDb resumed = SingleDb.resume(configPath.toString(), globalSnapshotId)) {
            for (int i = 0; i < 120; i++) {
                assertArrayEquals(
                        valueBytes("single-resume-v", i),
                        resumed.get(0, keyBytes("single-resume", i), 0));
            }
            resumed.put(
                    0, keyBytes("single-resume-new", 1), 0, valueBytes("single-resume-new-v", 1));
            assertArrayEquals(
                    valueBytes("single-resume-new-v", 1),
                    resumed.get(0, keyBytes("single-resume-new", 1), 0));
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
            assertNotNull(shardSnapshot.manifestPath);
            assertFalse(shardSnapshot.manifestPath.isEmpty());
            try (Db restored = Db.restore(configPath.toString(), shardSnapshot.snapshotId, dbId)) {
                for (int i = 0; i < 180; i++) {
                    assertArrayEquals(
                            valueBytes("restore-v", i), restored.get(0, keyBytes("restore", i), 0));
                }
            }
            try (Db restoredNew =
                    Db.restore(configPath.toString(), shardSnapshot.snapshotId, dbId, true)) {
                assertNotEquals(dbId, restoredNew.id());
                for (int i = 0; i < 180; i++) {
                    assertArrayEquals(
                            valueBytes("restore-v", i),
                            restoredNew.get(0, keyBytes("restore", i), 0));
                }
            }
            try (Db restoredFromManifest =
                    Db.restoreWithManifest(configPath.toString(), shardSnapshot.manifestPath)) {
                assertNotEquals(dbId, restoredFromManifest.id());
                for (int i = 0; i < 180; i++) {
                    assertArrayEquals(
                            valueBytes("restore-v", i),
                            restoredFromManifest.get(0, keyBytes("restore", i), 0));
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
    void rangeOpenExpandAndShrinkFlow() throws IOException {
        Path dataDir = Files.createTempDirectory("cobble-java-rescale-");
        Config config = new Config().addVolume(dataDir.toString()).numColumns(1).totalBuckets(4);

        try (Db source = Db.open(config, 2, 3)) {
            source.put(2, keyBytes("rescale-source", 2), 0, valueBytes("rescale-source-v", 2));
            ShardSnapshot shardSnapshot = source.snapshot();
            assertEquals(1, shardSnapshot.ranges.size());
            assertEquals(2, shardSnapshot.ranges.get(0).start);
            assertEquals(3, shardSnapshot.ranges.get(0).end);
            assertTrue(source.retainSnapshot(shardSnapshot.snapshotId));

            try (Db target = Db.open(config, 0, 1)) {
                target.put(0, keyBytes("rescale-target", 0), 0, valueBytes("rescale-target-v", 0));

                assertTrue(
                        target.expandBucket(
                                        source.id(),
                                        shardSnapshot.snapshotId,
                                        new int[] {2},
                                        new int[] {3})
                                >= 0L);
                assertArrayEquals(
                        valueBytes("rescale-source-v", 2),
                        target.get(2, keyBytes("rescale-source", 2), 0));

                assertTrue(target.shrinkBucket(new int[] {2}, new int[] {3}) >= 0L);
                assertArrayEquals(
                        valueBytes("rescale-target-v", 0),
                        target.get(0, keyBytes("rescale-target", 0), 0));
                assertNull(target.get(2, keyBytes("rescale-source", 2), 0));
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
                        assertEquals(2, batch.values[i].length);
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

    @Test
    void rawColumnFamilyApisAcrossDbReadOnlyAndReader() throws IOException {
        Path dataDir = Files.createTempDirectory("cobble-java-raw-cf-");
        Config config = new Config().addVolume(dataDir.toString()).numColumns(1).totalBuckets(1);
        Path configPath = writeConfigFile(dataDir, config);

        try (Db db = Db.open(config)) {
            Schema schema = db.updateSchema().addColumn("metrics", 0, null, null).commit();
            assertEquals(Integer.valueOf(0), schema.columnFamilyIds.get("default"));
            assertEquals(Integer.valueOf(1), schema.columnFamilyIds.get("metrics"));
            assertEquals(
                    MergeOperatorType.BYTES.operatorId(), schema.mergeOperatorId("metrics", 0));
            assertNotNull(schema.columnFamily("metrics"));
            assertEquals(1, schema.columnFamily("metrics").numColumns);

            for (int i = 0; i < 7; i++) {
                byte[] key = scanKeyBytes("raw-cf", i);
                byte[] value = valueBytes("raw-cf-v", i);
                if (i % 2 == 0) {
                    db.put(0, key, "metrics", 0, value);
                } else {
                    try (WriteOptions options = WriteOptions.withColumnFamily("metrics")) {
                        db.putWithOptions(0, key, 0, value, options);
                    }
                }
            }

            assertArrayEquals(
                    valueBytes("raw-cf-v", 3), db.get(0, scanKeyBytes("raw-cf", 3), "metrics", 0));
            try (ReadOptions options = ReadOptions.forColumnInFamily("metrics", 0)) {
                assertArrayEquals(
                        valueBytes("raw-cf-v", 4),
                        requiredSingleColumn(
                                db.getWithOptions(0, scanKeyBytes("raw-cf", 4), options)));
            }
            try (ScanOptions options =
                            ScanOptions.forColumns(0).columnFamily("metrics").batchSize(3);
                    ScanCursor cursor =
                            db.scanWithOptions(
                                    0,
                                    scanKeyBytes("raw-cf", 0),
                                    scanKeyBytes("raw-cf", 7),
                                    options)) {
                int rows = 0;
                while (true) {
                    ScanBatch batch = cursor.nextBatch();
                    for (int i = 0; i < batch.keys.length; i++) {
                        assertEquals(1, batch.values[i].length);
                        assertArrayEquals(valueBytes("raw-cf-v", rows), batch.values[i][0]);
                        rows++;
                    }
                    if (!batch.hasMore) {
                        break;
                    }
                }
                assertEquals(7, rows);
            }

            try (WriteOptions options = WriteOptions.withColumnFamily("metrics")) {
                db.deleteWithOptions(0, scanKeyBytes("raw-cf", 0), 0, options);
            }
            db.delete(0, scanKeyBytes("raw-cf", 1), "metrics", 0);
            assertNull(db.get(0, scanKeyBytes("raw-cf", 0), "metrics", 0));
            assertNull(db.get(0, scanKeyBytes("raw-cf", 1), "metrics", 0));

            ShardSnapshot shardSnapshot = db.snapshot();
            assertTrue(db.retainSnapshot(shardSnapshot.snapshotId));
            try (ReadOnlyDb readOnlyDb =
                            ReadOnlyDb.open(
                                    configPath.toString(), shardSnapshot.snapshotId, db.id());
                    ScanCursor cursor =
                            readOnlyDb.scan(
                                    0,
                                    scanKeyBytes("raw-cf", 0),
                                    scanKeyBytes("raw-cf", 7),
                                    "metrics")) {
                assertArrayEquals(
                        valueBytes("raw-cf-v", 2),
                        readOnlyDb.get(0, scanKeyBytes("raw-cf", 2), "metrics", 0));
                int rows = 0;
                while (true) {
                    ScanBatch batch = cursor.nextBatch();
                    rows += batch.keys.length;
                    if (!batch.hasMore) {
                        break;
                    }
                }
                assertEquals(5, rows);
            }

            try (DbCoordinator coordinator = DbCoordinator.open(configPath.toString())) {
                coordinator.materializeGlobalSnapshot(
                        1, shardSnapshot.snapshotId, Collections.singletonList(shardSnapshot));
            }

            try (Reader reader = Reader.openCurrent(configPath.toString());
                    ScanCursor cursor =
                            reader.scan(
                                    0,
                                    scanKeyBytes("raw-cf", 0),
                                    scanKeyBytes("raw-cf", 7),
                                    "metrics")) {
                assertArrayEquals(
                        valueBytes("raw-cf-v", 6),
                        reader.get(0, scanKeyBytes("raw-cf", 6), "metrics", 0));
                int rows = 0;
                while (true) {
                    ScanBatch batch = cursor.nextBatch();
                    rows += batch.keys.length;
                    if (!batch.hasMore) {
                        break;
                    }
                }
                assertEquals(5, rows);
            }
        }
    }

    @Test
    void dbDirectBufferApisReturnExpectedBuffers() throws IOException {
        Path dataDir = Files.createTempDirectory("cobble-java-direct-raw-");
        Config config = new Config().addVolume(dataDir.toString()).numColumns(2).totalBuckets(1);

        byte[] key = "direct-key".getBytes(StandardCharsets.UTF_8);
        byte[] smallValue = "small-direct-value".getBytes(StandardCharsets.UTF_8);
        byte[] largeValue = largeValueBytes("large-direct-value", 1, 4096);

        try (Db db = Db.open(config)) {
            db.put(0, key, 0, smallValue);
            db.put(0, key, 1, largeValue);

            ByteBuffer ioBuffer = ByteBuffer.allocateDirect(2048);
            ((Buffer) ioBuffer).clear();
            ioBuffer.put(key);
            try (ReadOptions options = ReadOptions.forColumns(0)) {
                ByteBuffer encoded =
                        db.getEncodedDirectWithOptions(0, ioBuffer, key.length, options);
                assertNotNull(encoded);
                assertTrue(encoded.isDirect());
                assertTrue(encoded.limit() > 0);
            }

            ByteBuffer smallIoBuffer = ByteBuffer.allocateDirect(64);
            ((Buffer) smallIoBuffer).clear();
            smallIoBuffer.put(key);
            try (ReadOptions options = ReadOptions.forColumns(0, 1)) {
                ByteBuffer encoded =
                        db.getEncodedDirectWithOptions(0, smallIoBuffer, key.length, options);
                assertNotNull(encoded);
                assertNotSame(smallIoBuffer, encoded);
                assertTrue(encoded.isDirect());
                assertTrue(encoded.limit() > smallIoBuffer.capacity());
            }

            try (ReadOptions options = ReadOptions.forColumns(0, 1)) {
                ByteBuffer[] columns = db.getDirectWithOptions(0, key, options);
                assertNotNull(columns);
                assertEquals(2, columns.length);
                assertNotNull(columns[0]);
                assertNotNull(columns[1]);
                assertArrayEquals(smallValue, readDirectBytes(columns[0]));
                assertArrayEquals(largeValue, readDirectBytes(columns[1]));
            }

            ByteBuffer keyBuffer = ByteBuffer.allocateDirect(key.length);
            ((Buffer) keyBuffer).clear();
            keyBuffer.put(key);
            ((Buffer) keyBuffer).flip();
            try (ReadOptions options = ReadOptions.forColumns(0, 1)) {
                ByteBuffer[] columns = db.getDirectWithOptions(0, keyBuffer, options);
                assertNotNull(columns);
                assertEquals(2, columns.length);
                assertArrayEquals(smallValue, readDirectBytes(columns[0]));
                assertArrayEquals(largeValue, readDirectBytes(columns[1]));
            }

            ByteBuffer paddedKeyBuffer = ByteBuffer.allocateDirect(key.length + 32);
            ((Buffer) paddedKeyBuffer).clear();
            paddedKeyBuffer.put(key);
            paddedKeyBuffer.put((byte) 'x');
            ((Buffer) paddedKeyBuffer).position(paddedKeyBuffer.capacity());
            try (ReadOptions options = ReadOptions.forColumns(0, 1)) {
                ByteBuffer[] columns =
                        db.getDirectWithOptions(0, paddedKeyBuffer, key.length, options);
                assertNotNull(columns);
                assertEquals(2, columns.length);
                assertArrayEquals(smallValue, readDirectBytes(columns[0]));
                assertArrayEquals(largeValue, readDirectBytes(columns[1]));
            }
        }
    }

    private static ShardSnapshot awaitSnapshot(CompletableFuture<ShardSnapshot> future) {
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
        if (columns.length == 0) {
            throw new IllegalStateException("expected at least one column, got 0");
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

    private static byte[] readDirectBytes(ByteBuffer buffer) {
        ByteBuffer copy = buffer.duplicate();
        byte[] bytes = new byte[copy.remaining()];
        copy.get(bytes);
        return bytes;
    }

    @Test
    void singleDbSchemaUpdateAndCurrentSchema() throws IOException {
        Path dataDir = Files.createTempDirectory("cobble-java-schema-");
        Config config = new Config().addVolume(dataDir.toString()).numColumns(2).totalBuckets(1);

        try (SingleDb db = SingleDb.open(config)) {
            // Read initial schema
            Schema initial = db.currentSchema();
            assertEquals(0, initial.version);
            assertNotNull(initial.defaultColumnFamily());
            assertEquals(2, initial.defaultColumnFamily().numColumns);

            // Set column 0 to U32CounterMergeOperator via MergeOperatorType enum
            Schema updated =
                    db.updateSchema().setColumnOperator(0, MergeOperatorType.U32_COUNTER).commit();
            assertEquals(2, updated.defaultColumnFamily().numColumns);
            assertTrue(updated.version > initial.version);
            assertEquals(MergeOperatorType.U32_COUNTER.operatorId(), updated.mergeOperatorId(0));

            // Verify currentSchema reflects the update
            Schema current = db.currentSchema();
            assertEquals(updated.version, current.version);
            assertEquals(MergeOperatorType.U32_COUNTER.operatorId(), current.mergeOperatorId(0));

            Schema withMetrics = db.updateSchema().addColumn("metrics", 0, null, null).commit();
            assertEquals(Integer.valueOf(0), withMetrics.columnFamilyIds.get("default"));
            assertEquals(Integer.valueOf(1), withMetrics.columnFamilyIds.get("metrics"));
            assertNotNull(withMetrics.columnFamily("metrics"));
            assertEquals(1, withMetrics.columnFamily("metrics").numColumns);

            Schema metricsUpdated =
                    db.updateSchema()
                            .setColumnOperator("metrics", 0, MergeOperatorType.BYTES)
                            .commit();
            assertEquals(
                    MergeOperatorType.BYTES.operatorId(),
                    metricsUpdated.mergeOperatorId("metrics", 0));

            // Test merge with u32 counter operator
            byte[] key = "counter-key".getBytes(StandardCharsets.UTF_8);
            byte[] val5 = new byte[] {5, 0, 0, 0};
            byte[] val3 = new byte[] {3, 0, 0, 0};
            db.merge(0, key, 0, val5);
            db.merge(0, key, 0, val3);
            byte[] result = db.get(0, key, 0);
            assertNotNull(result);
            assertEquals(4, result.length);
            int sum =
                    (result[0] & 0xFF)
                            | ((result[1] & 0xFF) << 8)
                            | ((result[2] & 0xFF) << 16)
                            | ((result[3] & 0xFF) << 24);
            assertEquals(8, sum);

            // Add a column
            Schema added = db.updateSchema().addColumn(2, null, null).commit();
            assertEquals(3, added.defaultColumnFamily().numColumns);
            assertTrue(added.version > updated.version);

            // Delete a column
            Schema deleted = db.updateSchema().deleteColumn(2).commit();
            assertEquals(2, deleted.defaultColumnFamily().numColumns);
            assertTrue(deleted.version > added.version);
        }
    }

    @Test
    void dbSchemaUpdateAndCurrentSchema() throws IOException {
        Path dataDir = Files.createTempDirectory("cobble-java-db-schema-");
        Config config = new Config().addVolume(dataDir.toString()).numColumns(1).totalBuckets(1);

        try (Db db = Db.open(config)) {
            Schema initial = db.currentSchema();
            assertNotNull(initial.defaultColumnFamily());
            assertEquals(1, initial.defaultColumnFamily().numColumns);

            Schema updated =
                    db.updateSchema().setColumnOperator(0, MergeOperatorType.U64_COUNTER).commit();
            assertTrue(updated.version > initial.version);
            assertEquals(MergeOperatorType.U64_COUNTER.operatorId(), updated.mergeOperatorId(0));

            Schema current = db.currentSchema();
            assertEquals(updated.version, current.version);
        }
    }

    // ── structured SingleDb ───────────────────────────────────────────────────

    @Test
    void structuredSingleDbReadWriteAndSnapshot() throws IOException {
        Path dataDir = Files.createTempDirectory("cobble-java-structured-single-");
        Config config = new Config().addVolume(dataDir.toString()).numColumns(2).totalBuckets(1);

        try (io.cobble.structured.SingleDb db = io.cobble.structured.SingleDb.open(config)) {
            db.updateSchema().addBytesColumn(0).addBytesColumn(1).commit();
            // basic put / get
            int count = 100;
            for (int i = 0; i < count; i++) {
                byte[] key = scanKeyBytes("single-struct", i);
                byte[] value = valueBytes("single-struct-v", i);
                db.put(0, key, 0, value);
            }

            for (int i = 0; i < count; i++) {
                byte[] key = scanKeyBytes("single-struct", i);
                Row row = db.get(0, key);
                assertNotNull(row);
                assertArrayEquals(valueBytes("single-struct-v", i), row.getBytes(0));
            }

            // typed put with ColumnValue
            byte[] mergeKey = scanKeyBytes("single-struct-merge", 1);
            db.put(0, mergeKey, 0, ColumnValue.ofBytes("base".getBytes(StandardCharsets.UTF_8)));
            db.merge(
                    0,
                    mergeKey,
                    0,
                    ColumnValue.ofBytes("-appended".getBytes(StandardCharsets.UTF_8)));
            Row mergeRow = db.get(0, mergeKey);
            assertNotNull(mergeRow);
            assertEquals("base-appended", new String(mergeRow.getBytes(0), StandardCharsets.UTF_8));

            // delete
            db.delete(0, scanKeyBytes("single-struct", 0), 0);
            assertNull(db.get(0, scanKeyBytes("single-struct", 0)));

            // scan
            assertNotNull(
                    db.get(0, scanKeyBytes("single-struct", 1)), "key must exist before scan");
            byte[] scanStart = scanKeyBytes("single-struct", 1);
            byte[] scanEnd = scanKeyBytes("single-struct", 10);
            try (io.cobble.structured.ScanCursor cursor = db.scan(0, scanStart, scanEnd)) {
                int scanned = 0;
                io.cobble.structured.ScanBatch batch;
                do {
                    batch = cursor.nextBatch();
                    for (int b = 0; b < batch.size(); b++) {
                        Row r = batch.getRow(b);
                        assertNotNull(r.getKey());
                        scanned++;
                    }
                } while (batch.hasMore);
                assertEquals(9, scanned);
            }

            // snapshot lifecycle
            GlobalSnapshot snap = db.snapshot();
            assertNotNull(snap);
            assertTrue(snap.id >= 0);

            List<GlobalSnapshot> snapshots = db.listSnapshots();
            assertFalse(snapshots.isEmpty());

            db.retainSnapshot(snap.id);
            db.expireSnapshot(snap.id);

            // nowSeconds / setTime
            int now = db.nowSeconds();
            assertTrue(now >= 0);
        }
    }

    @Test
    void scanCursorForEachLoop() throws IOException {
        Path dataDir = Files.createTempDirectory("cobble-java-scan-foreach-");
        Config config = new Config().addVolume(dataDir.toString()).numColumns(2).totalBuckets(1);
        try (Db db = Db.open(config)) {
            int count = 50;
            for (int i = 0; i < count; i++) {
                byte[] key = scanKeyBytes("foreach", i);
                db.put(0, key, 0, valueBytes("c0", i));
                db.put(0, key, 1, valueBytes("c1", i));
            }
            try (ScanCursor cursor =
                    db.scan(0, scanKeyBytes("foreach", 10), scanKeyBytes("foreach", 40))) {
                List<String> keys = new ArrayList<>();
                for (ScanCursor.Entry entry : cursor) {
                    keys.add(new String(entry.key, StandardCharsets.UTF_8));
                    assertNotNull(entry.columns);
                }
                assertEquals(30, keys.size());
                for (int i = 0; i < keys.size(); i++) {
                    String key = keys.get(i);
                    int idx = Integer.parseInt(key.substring(key.lastIndexOf('-') + 1));
                    assertEquals(10 + i, idx);
                }
            }
        }
    }

    @Test
    void distributedScanPlanSplitCursorRoundTrip() throws IOException {
        Path dataDir = Files.createTempDirectory("cobble-java-dist-scan-");
        Config config = new Config().addVolume(dataDir.toString()).numColumns(2).totalBuckets(1);
        try (Db db = Db.open(config)) {
            int count = 60;
            for (int i = 0; i < count; i++) {
                byte[] key = scanKeyBytes("dscan", i);
                db.put(0, key, 0, valueBytes("dscan-v0", i));
                db.put(0, key, 1, valueBytes("dscan-v1", i));
            }
            ShardSnapshot shardSnapshot = db.snapshot();
            assertTrue(db.retainSnapshot(shardSnapshot.snapshotId));

            GlobalSnapshot globalSnapshot;
            try (DbCoordinator coordinator = DbCoordinator.open(config)) {
                globalSnapshot =
                        coordinator.materializeGlobalSnapshot(
                                1,
                                shardSnapshot.snapshotId,
                                Collections.singletonList(shardSnapshot));
            }
            assertNotNull(globalSnapshot);

            ScanPlan plan =
                    ScanPlan.fromGlobalSnapshot(globalSnapshot)
                            .withStart(scanKeyBytes("dscan", 10))
                            .withEnd(scanKeyBytes("dscan", 30));
            List<ScanSplit> splits = plan.splits();
            assertEquals(1, splits.size());

            ScanSplit split = ScanSplit.fromJson(splits.get(0).toJson());
            try (ScanOptions options = new ScanOptions().batchSize(9).columns(1);
                    ScanCursor cursor = split.openScannerWithOptions(config, options)) {
                List<ScanCursor.Entry> entries = new ArrayList<ScanCursor.Entry>();
                for (ScanCursor.Entry entry : cursor) {
                    entries.add(entry);
                }
                assertEquals(20, entries.size());
                for (int i = 0; i < entries.size(); i++) {
                    int expected = 10 + i;
                    ScanCursor.Entry entry = entries.get(i);
                    assertArrayEquals(scanKeyBytes("dscan", expected), entry.key);
                    assertEquals(1, entry.columns.length);
                    assertArrayEquals(valueBytes("dscan-v1", expected), entry.columns[0]);
                }
            }
        }
    }

    @Test
    void structuredScanCursorForEachLoop() throws IOException {
        Path dataDir = Files.createTempDirectory("cobble-java-structured-foreach-");
        Config config = new Config().addVolume(dataDir.toString()).numColumns(2).totalBuckets(1);
        try (io.cobble.structured.Db db = io.cobble.structured.Db.open(config)) {
            int count = 50;
            for (int i = 0; i < count; i++) {
                byte[] key = scanKeyBytes("sfore", i);
                db.put(0, key, 0, valueBytes("sc0", i));
                db.put(0, key, 1, valueBytes("sc1", i));
            }
            try (io.cobble.structured.ScanCursor cursor =
                    db.scan(0, scanKeyBytes("sfore", 5), scanKeyBytes("sfore", 25))) {
                List<Row> rows = new ArrayList<>();
                for (Row row : cursor) {
                    rows.add(row);
                    assertNotNull(row.getKey());
                }
                assertEquals(20, rows.size());
                for (int i = 0; i < rows.size(); i++) {
                    Row row = rows.get(i);
                    String key = new String(row.getKey(), StandardCharsets.UTF_8);
                    int idx = Integer.parseInt(key.substring(key.lastIndexOf('-') + 1));
                    assertEquals(5 + i, idx);
                    assertNotNull(row.getColumnValue(0));
                    assertTrue(row.getColumnValue(0).isBytes());
                }
            }
        }
    }
}
