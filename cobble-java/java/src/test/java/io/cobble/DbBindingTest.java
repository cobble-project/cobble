package io.cobble;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
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

    private static byte[] keyBytes(String prefix, int i) {
        return (prefix + "-k-" + i).getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] valueBytes(String prefix, int i) {
        return (prefix + "-value-" + i + "-payload").getBytes(StandardCharsets.UTF_8);
    }
}
