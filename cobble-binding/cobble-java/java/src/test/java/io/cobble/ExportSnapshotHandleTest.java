package io.cobble;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class ExportSnapshotHandleTest {
    @Test
    void exportedGlobalSnapshotPublishesIndependentlyAndSurvivesSourceDeletion(@TempDir Path root)
            throws Exception {
        Path source = root.resolve("source");
        Path destination = root.resolve("export");
        Config config = new Config().addVolume(source.toString()).numColumns(1).totalBuckets(4);
        config.memtableCapacity = 4096;
        config.valueSeparationThreshold = 32;
        config.snapshotOnFlush = false;
        config.snapshotRetention = 1;
        Config.VolumeDescriptor target =
                Config.VolumeDescriptor.singleVolume(destination.toString());
        try (Db first = Db.open(config, 0, 1);
                Db second = Db.open(config, 2, 3);
                DbCoordinator coordinator = DbCoordinator.open(config)) {
            List<ShardSnapshot> originals = new ArrayList<>();
            List<ShardSnapshot> exports = new ArrayList<>();
            for (int shard = 0; shard < 2; shard++) {
                Db db = shard == 0 ? first : second;
                for (int bucket = shard * 2; bucket < shard * 2 + 2; bucket++) {
                    byte[] value = new byte[256];
                    Arrays.fill(value, (byte) (bucket + 10));
                    for (int index = 0; index < 32; index++)
                        db.put(bucket, new byte[] {(byte) index}, 0, value);
                }
                ShardSnapshot snapshot = db.snapshot();
                originals.add(snapshot);
                try (ExportSnapshotHandle handle =
                        db.exportSnapshot(
                                snapshot.snapshotId,
                                Config.VolumeDescriptor.singleVolume(
                                        destination.resolve("shards").toString()))) {
                    exports.add(handle.future().get(30, TimeUnit.SECONDS));
                }
            }
            coordinator.materializeGlobalSnapshot(4, 7, originals);
            assertTrue(coordinator.retainSnapshot(7));
            GlobalSnapshot online = coordinator.materializeGlobalSnapshot(4, 8, originals);
            byte[] pointer = Files.readAllBytes(source.resolve("snapshot/CURRENT"));
            GlobalSnapshot exported = coordinator.takeExportedGlobalSnapshot(4, exports);
            assertEquals(0, exported.id);
            assertEquals(2, exported.shardSnapshots.size());
            assertEquals(online.columnFamilyIds, exported.columnFamilyIds);
            assertFalse(Files.exists(destination.resolve("snapshot/SNAPSHOT-0")));
            coordinator.materializeExportedGlobalSnapshot(exported, target);
            coordinator.materializeExportedGlobalSnapshot(exported.copy(), target);
            assertTrue(Files.exists(destination.resolve("snapshot/SNAPSHOT-0")));
            assertArrayEquals(pointer, Files.readAllBytes(source.resolve("snapshot/CURRENT")));
            assertEquals(online.toJson(), coordinator.loadCurrentGlobalSnapshot().toJson());
            assertEquals(2, coordinator.listGlobalSnapshots().size());
            coordinator.materializeGlobalSnapshot(4, 9, originals);
            List<GlobalSnapshot> retained = coordinator.listGlobalSnapshots();
            assertEquals(2, retained.size());
            assertEquals(7, retained.get(0).id);
            assertEquals(9, retained.get(1).id);

            GlobalSnapshot conflict = exported.copy();
            conflict.watermarkSeconds++;
            assertThrows(
                    IllegalStateException.class,
                    () -> coordinator.materializeExportedGlobalSnapshot(conflict, target));
            assertThrows(
                    IllegalStateException.class,
                    () -> coordinator.takeExportedGlobalSnapshot(5, exports));
            assertThrows(
                    IllegalStateException.class,
                    () -> coordinator.materializeExportedGlobalSnapshot(online, target));
            assertThrows(
                    IllegalArgumentException.class,
                    () -> coordinator.takeExportedGlobalSnapshot(-1, exports));
            assertThrows(
                    IllegalArgumentException.class,
                    () -> coordinator.takeExportedGlobalSnapshot(4, null));
            assertThrows(
                    IllegalArgumentException.class,
                    () -> coordinator.takeExportedGlobalSnapshot(4, Collections.emptyList()));
            assertThrows(
                    IllegalArgumentException.class,
                    () ->
                            coordinator.takeExportedGlobalSnapshot(
                                    4, Collections.singletonList(null)));
            assertThrows(
                    IllegalArgumentException.class,
                    () -> coordinator.materializeExportedGlobalSnapshot(null, target));
            assertThrows(
                    IllegalArgumentException.class,
                    () -> coordinator.materializeExportedGlobalSnapshot(exported, null));
            coordinator.close();
            assertThrows(
                    IllegalStateException.class,
                    () -> coordinator.takeExportedGlobalSnapshot(4, exports));
            assertThrows(
                    IllegalStateException.class,
                    () -> coordinator.materializeExportedGlobalSnapshot(exported, target));
        }
        deleteTree(source);
        Config readerConfig =
                new Config().addVolume(destination.toString()).numColumns(1).totalBuckets(4);
        for (boolean current : new boolean[] {true, false}) {
            try (Reader reader =
                    current ? Reader.openCurrent(readerConfig) : Reader.open(readerConfig, 0)) {
                for (int bucket = 0; bucket < 4; bucket++) {
                    byte[] value = new byte[256];
                    Arrays.fill(value, (byte) (bucket + 10));
                    for (int index = 0; index < 32; index++)
                        assertArrayEquals(value, reader.get(bucket, new byte[] {(byte) index}, 0));
                    try (ScanCursor cursor = reader.scan(bucket, null, null)) {
                        int count = 0;
                        for (ScanCursor.Entry row : cursor) {
                            assertEquals(bucket, row.bucket);
                            assertArrayEquals(new byte[] {(byte) count++}, row.key);
                            assertArrayEquals(value, row.columns[0]);
                        }
                        assertEquals(32, count);
                    }
                }
            }
        }
    }

    @Test
    void cancellationAndCloseBeforeCompletionKeepNativeWorkProtected(@TempDir Path root)
            throws Exception {
        Config config =
                new Config()
                        .addVolume(root.resolve("source").toString())
                        .numColumns(1)
                        .totalBuckets(1);
        try (Db db = Db.open(config)) {
            db.put(0, new byte[] {1}, 0, new byte[8192]);
            for (boolean cancel : new boolean[] {true, false}) {
                BlockingDestination destination = new BlockingDestination();
                ProcessFileSystems.registerCustomRegistry(
                        request ->
                                request.baseDir().startsWith("exporttest://") ? destination : null);
                try {
                    ShardSnapshot source = db.snapshot();
                    ExportSnapshotHandle handle =
                            db.exportSnapshot(
                                    source.snapshotId,
                                    Config.VolumeDescriptor.singleVolume("exporttest://target/"));
                    assertTrue(destination.started.await(30, TimeUnit.SECONDS));
                    if (cancel) assertTrue(handle.cancel());
                    handle.close();
                    assertFalse(
                            handle.future().isDone(),
                            "closing the handle must not detach or finish blocked work");
                    destination.proceed.countDown();
                    if (cancel) {
                        ExecutionException failure =
                                assertThrows(
                                        ExecutionException.class,
                                        () -> handle.future().get(30, TimeUnit.SECONDS));
                        assertInstanceOf(CancelledError.class, failure.getCause());
                        assertTrue(destination.paths.isEmpty(), "cancelled target must be cleaned");
                    } else {
                        assertNotNull(handle.future().get(30, TimeUnit.SECONDS));
                        assertFalse(destination.paths.isEmpty());
                    }
                    assertTrue(db.expireSnapshot(source.snapshotId));
                    ShardSnapshot next = db.snapshot();
                    assertTrue(db.expireSnapshot(next.snapshotId));
                    assertArrayEquals(new byte[8192], db.get(0, new byte[] {1}, 0));
                } finally {
                    destination.proceed.countDown();
                    ProcessFileSystems.clearCustomRegistry();
                }
            }
        }
    }

    private static final class BlockingDestination implements CustomFileSystem {
        final CountDownLatch started = new CountDownLatch(1);
        final CountDownLatch proceed = new CountDownLatch(1);
        final Set<String> paths = ConcurrentHashMap.newKeySet();
        final java.util.Map<String, Long> sizes = new ConcurrentHashMap<>();

        public void createDir(String path) {
            paths.add(path);
        }

        public boolean exists(String path) {
            return paths.contains(path);
        }

        public Long fileSize(String path) {
            return sizes.get(path);
        }

        public void delete(String path) {
            paths.removeIf(candidate -> candidate.equals(path) || candidate.startsWith(path + "/"));
            sizes.keySet()
                    .removeIf(
                            candidate ->
                                    candidate.equals(path) || candidate.startsWith(path + "/"));
        }

        public void deleteAsync(String path) {
            delete(path);
        }

        public void rename(String from, String to) {
            paths.remove(from);
            paths.add(to);
            Long size = sizes.remove(from);
            if (size != null) sizes.put(to, size);
        }

        public String[] list(String path) {
            return new String[0];
        }

        public CustomRandomAccessFile openRead(String path) {
            throw new UnsupportedOperationException();
        }

        public CustomSequentialWriteFile openWrite(String path) {
            paths.add(path);
            return new CustomSequentialWriteFile() {
                long size;

                public int write(byte[] bytes) {
                    started.countDown();
                    try {
                        if (!proceed.await(30, TimeUnit.SECONDS))
                            throw new IllegalStateException("copy gate timed out");
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException(e);
                    }
                    size += bytes.length;
                    sizes.put(path, size);
                    return bytes.length;
                }

                public long size() {
                    return size;
                }

                public void close() {}
            };
        }

        public Long lastModified(String path) {
            return null;
        }

        public void close() {}
    }

    @Test
    void exportedSnapshotRestoresAfterSourceDeletionAndContinuationCanCloseDb(@TempDir Path root)
            throws Exception {
        Path source = root.resolve("source");
        Path destination = root.resolve("export");
        Config config = new Config().addVolume(source.toString()).numColumns(1).totalBuckets(1);
        config.memtableCapacity = 4096;
        config.valueSeparationThreshold = 32;
        byte[] value = new byte[1024];
        Arrays.fill(value, (byte) 37);
        Db db = Db.open(config);
        for (int index = 0; index < 64; index++) db.put(0, new byte[] {(byte) index}, 0, value);
        ShardSnapshot sourceSnapshot = db.snapshot();
        ExportSnapshotHandle handle =
                db.exportSnapshot(
                        sourceSnapshot.snapshotId,
                        Config.VolumeDescriptor.singleVolume(destination.toString()));
        ShardSnapshot exported =
                handle.future()
                        .thenApply(
                                snapshot -> {
                                    db.close();
                                    return snapshot;
                                })
                        .get(30, TimeUnit.SECONDS);
        handle.close();
        handle.close();
        assertTrue(handle.isDisposed());
        assertFalse(handle.cancel());
        assertNotEquals(sourceSnapshot.dbId, exported.dbId);
        deleteTree(source);
        Config restoredConfig =
                new Config().addVolume(destination.toString()).numColumns(1).totalBuckets(1);
        for (int attempt = 0; attempt < 2; attempt++) {
            try (Db restored = Db.restoreWithManifest(restoredConfig, exported.manifestPath)) {
                for (int index = 0; index < 64; index++)
                    assertArrayEquals(value, restored.get(0, new byte[] {(byte) index}, 0));
                ShardSnapshot checkpoint = restored.snapshot();
                assertTrue(restored.expireSnapshot(checkpoint.snapshotId));
            }
        }
    }

    @Test
    void structuredDbExportsUsingTheSameNativeHandle(@TempDir Path root) throws Exception {
        Config config =
                new Config()
                        .addVolume(root.resolve("source").toString())
                        .numColumns(1)
                        .totalBuckets(1);
        ShardSnapshot exported;
        try (io.cobble.structured.Db db = io.cobble.structured.Db.open(config)) {
            db.put(0, new byte[] {1}, 0, new byte[] {2});
            ShardSnapshot snapshot = db.snapshot();
            try (ExportSnapshotHandle handle =
                    db.exportSnapshot(
                            snapshot.snapshotId,
                            Config.VolumeDescriptor.singleVolume(
                                    root.resolve("export").toString()))) {
                exported = handle.future().get(30, TimeUnit.SECONDS);
            }
            assertTrue(db.expireSnapshot(snapshot.snapshotId));
        }
        deleteTree(root.resolve("source"));
        Config restored =
                new Config()
                        .addVolume(root.resolve("export").toString())
                        .numColumns(1)
                        .totalBuckets(1);
        try (io.cobble.structured.Db db =
                io.cobble.structured.Db.restoreWithManifest(restored, exported.manifestPath)) {
            assertArrayEquals(new byte[] {2}, db.get(0, new byte[] {1}).getBytes(0));
        }
    }

    private static void deleteTree(Path root) throws Exception {
        Files.walkFileTree(
                root,
                new java.nio.file.SimpleFileVisitor<Path>() {
                    @Override
                    public java.nio.file.FileVisitResult visitFile(
                            Path path, java.nio.file.attribute.BasicFileAttributes attributes)
                            throws java.io.IOException {
                        Files.deleteIfExists(path);
                        return java.nio.file.FileVisitResult.CONTINUE;
                    }

                    @Override
                    public java.nio.file.FileVisitResult visitFileFailed(
                            Path path, java.io.IOException error) throws java.io.IOException {
                        if (!(error instanceof java.nio.file.NoSuchFileException)) throw error;
                        return java.nio.file.FileVisitResult.CONTINUE;
                    }

                    @Override
                    public java.nio.file.FileVisitResult postVisitDirectory(
                            Path path, java.io.IOException error) throws java.io.IOException {
                        if (error != null) throw error;
                        Files.deleteIfExists(path);
                        return java.nio.file.FileVisitResult.CONTINUE;
                    }
                });
    }
}
