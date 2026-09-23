package io.cobble.table;

import io.cobble.Config;
import io.cobble.Db;
import io.cobble.DbCoordinator;
import io.cobble.DirectScanCursor;
import io.cobble.DirectScanEntry;
import io.cobble.GlobalSnapshot;
import io.cobble.ReadOptions;
import io.cobble.Reader;
import io.cobble.ScanOptions;
import io.cobble.ShardSnapshot;
import io.cobble.SnapshotTools;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ReaderSnapshotTest {
    @TempDir Path directory;

    @Test
    void opensRawSnapshotWithoutNativeTableMetadata() {
        Config config = new Config().addVolume(directory.toString()).numColumns(1).totalBuckets(1);
        byte[] key = "state-key".getBytes(StandardCharsets.UTF_8);
        byte[] value = "state-value".getBytes(StandardCharsets.UTF_8);
        byte[] wideKey = new byte[65];
        java.util.Arrays.fill(wideKey, (byte) 0xff);
        GlobalSnapshot global;
        try (Db db = Db.open(config);
                DbCoordinator coordinator = DbCoordinator.open(config)) {
            // Deliberately write the default raw column family without TableMetadata.
            db.put(0, key, 0, value);
            db.put(0, wideKey, 0, value);
            ShardSnapshot shard = db.snapshot();
            global = coordinator.materializeGlobalSnapshot(1, 7L, Collections.singletonList(shard));
        }

        try (Reader view = Reader.open(config, global);
                ReadOptions read = ReadOptions.defaults();
                ScanOptions scan = new ScanOptions()) {
            assertArrayEquals(value, view.getWithOptions(0, key, read)[0]);
            byte[][][] batch = view.multiGet(new int[] {0, 0}, new byte[][] {key, wideKey});
            assertArrayEquals(value, batch[0][0]);
            assertArrayEquals(value, batch[1][0]);
            try (DirectScanCursor cursor = view.scanDirectWithOptions(0, null, null, scan)) {
                DirectScanEntry entry = cursor.nextEntry();
                assertNotNull(entry);
                assertEquals(0, entry.getBucket());
                ByteBuffer scannedKey = entry.getKey();
                byte[] actualKey = new byte[scannedKey.remaining()];
                scannedKey.get(actualKey);
                assertArrayEquals(key, actualKey);
                ByteBuffer scannedValue = entry.columnsView().get(0);
                byte[] actualValue = new byte[scannedValue.remaining()];
                scannedValue.get(actualValue);
                assertArrayEquals(value, actualValue);
                DirectScanEntry wide = cursor.nextEntry();
                assertNotNull(wide);
                ByteBuffer wideBuffer = wide.getKey();
                byte[] scannedWide = new byte[wideBuffer.remaining()];
                wideBuffer.get(scannedWide);
                assertArrayEquals(wideKey, scannedWide);
                assertNull(cursor.nextEntry());
            }
            try (DirectScanCursor cursor = view.scanDirectWithOptions(0, wideKey, null, scan)) {
                assertNotNull(cursor.nextEntry());
                assertNull(cursor.nextEntry());
            }
        }
    }

    @Test
    void fixedManifestIgnoresNewerCurrentSnapshot() {
        Config config = new Config().addVolume(directory.toString()).numColumns(1).totalBuckets(1);
        byte[] key = new byte[] {1};
        byte[] oldValue = new byte[] {2};
        byte[] newValue = new byte[] {3};
        GlobalSnapshot old;
        try (Db db = Db.open(config);
                DbCoordinator coordinator = DbCoordinator.open(config)) {
            db.put(0, key, 0, oldValue);
            ShardSnapshot oldShard = db.snapshot();
            db.retainSnapshot(oldShard.snapshotId);
            old =
                    coordinator.materializeGlobalSnapshot(
                            1, 20L, Collections.singletonList(oldShard));
            db.put(0, key, 0, newValue);
            coordinator.materializeGlobalSnapshot(1, 21L, Collections.singletonList(db.snapshot()));
        }
        try (Reader fixed = Reader.open(config, old)) {
            assertEquals(20L, fixed.currentGlobalSnapshot().id);
            assertEquals("snapshot", fixed.readMode());
            assertArrayEquals(oldValue, fixed.get(0, key, 0));
        }
        try (Reader current = Reader.openCurrent(config)) {
            assertEquals(21L, current.currentGlobalSnapshot().id);
            assertArrayEquals(newValue, current.get(0, key, 0));
        }
    }

    @Test
    void buildsPhysicalManifestFromCompleteShardMetadata() {
        Config config = new Config().addVolume(directory.toString()).numColumns(1).totalBuckets(1);
        byte[] key = "checkpoint-key".getBytes(StandardCharsets.UTF_8);
        byte[] value = "checkpoint-value".getBytes(StandardCharsets.UTF_8);
        ShardSnapshot shard;
        try (Db db = Db.open(config)) {
            db.put(0, key, 0, value);
            shard = db.snapshot();
        }

        GlobalSnapshot global =
                SnapshotTools.buildGlobalSnapshot(1, 11L, Collections.singletonList(shard));
        assertEquals(Integer.valueOf(0), global.columnFamilyIds.get("default"));
        assertEquals(
                Integer.valueOf(0), global.shardSnapshots.get(0).columnFamilyIds.get("default"));

        try (Reader view = Reader.open(config, global);
                ReadOptions read = ReadOptions.defaults()) {
            assertArrayEquals(value, view.getWithOptions(0, key, read)[0]);
        }
    }

    @Test
    void loadsFixedGlobalMetadataWithoutOpeningAWriterCoordinator() {
        Path root = directory.resolve("snapshot root with spaces");
        Config config = new Config().addVolume(root.toString()).numColumns(1).totalBuckets(1);
        GlobalSnapshot materialized;
        try (Db db = Db.open(config);
                DbCoordinator coordinator = DbCoordinator.open(config)) {
            db.put(0, new byte[] {1}, 0, new byte[] {2});
            materialized =
                    coordinator.materializeGlobalSnapshot(
                            1, 9L, Collections.singletonList(db.snapshot()));
        }

        GlobalSnapshot loaded =
                SnapshotTools.loadGlobalSnapshot(
                        config, root.resolve("snapshot").resolve("SNAPSHOT-9").toString());
        assertEquals(materialized.id, loaded.id);
        assertEquals(materialized.totalBuckets, loaded.totalBuckets);
        assertEquals(1, loaded.shardSnapshots.size());
        assertEquals(
                materialized.shardSnapshots.get(0).manifestPath,
                loaded.shardSnapshots.get(0).manifestPath);

        Path manifest = root.resolve("snapshot").resolve("SNAPSHOT-9");
        assertEquals(
                materialized.id,
                SnapshotTools.loadGlobalSnapshot(
                                config,
                                "file:"
                                        + java.net
                                                .URI
                                                .create(manifest.toUri().toString())
                                                .getRawPath())
                        .id);
        assertEquals(
                materialized.id,
                SnapshotTools.loadGlobalSnapshot(config, manifest.toUri().toString()).id);
        assertThrows(
                IllegalArgumentException.class,
                () -> SnapshotTools.loadGlobalSnapshot(config, "snapshot/SNAPSHOT-9"));
    }

    @Test
    void reusesOpenedShardAcrossReadsWithinOnePhysicalView() throws Exception {
        Config config = new Config().addVolume(directory.toString()).numColumns(1).totalBuckets(1);
        byte[] key = "cached-key".getBytes(StandardCharsets.UTF_8);
        byte[] value = "cached-value".getBytes(StandardCharsets.UTF_8);
        GlobalSnapshot global;
        try (Db db = Db.open(config);
                DbCoordinator coordinator = DbCoordinator.open(config)) {
            db.put(0, key, 0, value);
            global =
                    coordinator.materializeGlobalSnapshot(
                            1, 10L, Collections.singletonList(db.snapshot()));
        }

        Path manifest = localPath(global.shardSnapshots.get(0).manifestPath);
        Path moved = manifest.resolveSibling(manifest.getFileName() + ".renamed-for-cache-test");
        try (Reader view = Reader.open(config, global);
                ReadOptions read = ReadOptions.defaults()) {
            assertArrayEquals(value, view.getWithOptions(0, key, read)[0]);
            Files.move(manifest, moved);
            try {
                // The first lookup opened this shard. The fixed reader retains that DB.
                assertArrayEquals(value, view.getWithOptions(0, key, read)[0]);
                try (Reader cold = Reader.open(config, global)) {
                    assertThrows(
                            IllegalStateException.class, () -> cold.getWithOptions(0, key, read));
                }
            } finally {
                Files.move(moved, manifest);
            }
        }
    }

    private static Path localPath(String path) {
        URI uri = URI.create(path);
        return uri.getScheme() == null ? Path.of(path) : Path.of(uri);
    }
}
