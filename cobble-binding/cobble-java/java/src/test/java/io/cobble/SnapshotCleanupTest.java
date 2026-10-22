package io.cobble;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class SnapshotCleanupTest {
    @Test
    void shardDefaultsPreserveIncrementalBaseAndOptionsAllowBreakingIt(@TempDir Path root)
            throws Exception {
        Config config = config(root);
        ShardSnapshot base;
        ShardSnapshot next;
        try (Db db = Db.open(config)) {
            base = snapshot(db, 1);
            next = snapshot(db, 2);
        }
        byte[] manifestBytes = Files.readAllBytes(path(next.manifestPath));
        assertEquals(
                "mcs1",
                new String(manifestBytes, manifestBytes.length - 4, 4, StandardCharsets.US_ASCII));
        JsonObject nextManifest =
                JsonParser.parseString(
                                new String(
                                        manifestBytes,
                                        0,
                                        manifestBytes.length - 8,
                                        StandardCharsets.UTF_8))
                        .getAsJsonObject();
        assertEquals(base.snapshotId, nextManifest.get("base_snapshot_id").getAsLong());
        Config.VolumeDescriptor metadata = Config.VolumeDescriptor.singleVolume(root.toString());
        SnapshotCleanupResult preserved =
                SnapshotTools.cleanupShardSnapshot(
                        metadata, Collections.emptyList(), base.dbId, base.snapshotId);
        assertEquals(0, preserved.deletedFiles);
        assertEquals(0, preserved.deletedShardSnapshots);
        assertEquals(0, preserved.deletedGlobalSnapshots);
        assertEquals(1, preserved.preservedSnapshots.size());
        SnapshotCleanupResult.ShardSnapshotIdentity identity = preserved.preservedSnapshots.get(0);
        assertEquals(base.dbId, identity.dbId);
        assertEquals(base.snapshotId, identity.snapshotId);
        assertEquals(path(base.manifestPath), path(identity.manifestPath));
        assertTrue(Files.exists(path(base.manifestPath)));

        SnapshotCleanupOptions options = new SnapshotCleanupOptions();
        options.preserveDependencies = false;
        SnapshotCleanupResult removed =
                SnapshotTools.cleanupShardSnapshot(
                        metadata, Collections.emptyList(), base.dbId, base.snapshotId, options);
        assertEquals(1, removed.deletedShardSnapshots);
        assertEquals(0, removed.deletedGlobalSnapshots);
        assertTrue(removed.deletedFiles > 0);
        assertTrue(removed.preservedSnapshots.isEmpty());
        assertFalse(Files.exists(path(base.manifestPath)));
        assertTrue(Files.exists(path(next.manifestPath)));
    }

    @Test
    void globalOlderSelectionComparesGlobalRatherThanShardIds(@TempDir Path root) throws Exception {
        Path metadataRoot = root.resolve("metadata");
        Path shardRoot = root.resolve("shards");
        Config shardConfig = config(shardRoot);
        shardConfig.snapshotDisableIncrementalBaseLink = true;
        ShardSnapshot[] shards = new ShardSnapshot[3];
        try (Db db = Db.open(shardConfig);
                DbCoordinator coordinator = DbCoordinator.open(config(metadataRoot))) {
            for (int index = 0; index < shards.length; index++) {
                shards[index] = snapshot(db, index);
                coordinator.materializeGlobalSnapshot(
                        1, index + 7, Collections.singletonList(shards[index]));
            }
        }
        assertTrue(shards[2].snapshotId < 8);
        SnapshotCleanupOptions options = new SnapshotCleanupOptions();
        options.includeOlderSnapshots = true;
        SnapshotCleanupResult removed =
                SnapshotTools.cleanupGlobalSnapshot(
                        Config.VolumeDescriptor.singleVolume(metadataRoot.toString()),
                        Collections.singletonList(
                                Config.VolumeDescriptor.singleVolume(shardRoot.toString())),
                        8,
                        options);
        assertEquals(2, removed.deletedGlobalSnapshots);
        assertEquals(2, removed.deletedShardSnapshots);
        assertTrue(removed.preservedSnapshots.isEmpty());
        for (int id = 7; id <= 9; id++) {
            assertEquals(id == 9, Files.exists(metadataRoot.resolve("snapshot/SNAPSHOT-" + id)));
        }
        for (int index = 0; index < shards.length; index++) {
            assertEquals(index == 2, Files.exists(path(shards[index].manifestPath)));
        }
        try (Reader reader =
                Reader.openCurrent(config(metadataRoot).addVolume(shardRoot.toString()))) {
            assertArrayEquals(new byte[128], reader.get(0, new byte[] {2}, 0));
        }
    }

    @Test
    void exportedShardAndGlobalUseTheirRespectiveMetadataBases(@TempDir Path root)
            throws Exception {
        Path source = root.resolve("source");
        Path export = root.resolve("export");
        Path standalone = root.resolve("standalone");
        Config.VolumeDescriptor exportRoot =
                Config.VolumeDescriptor.singleVolume(export.toString());
        ShardSnapshot original;
        ShardSnapshot standaloneShard;
        ShardSnapshot globalShard;
        try (Db db = Db.open(config(source));
                DbCoordinator coordinator = DbCoordinator.open(config(source))) {
            original = snapshot(db, 1);
            try (ExportSnapshotHandle handle =
                    db.exportSnapshot(
                            original.snapshotId,
                            Config.VolumeDescriptor.singleVolume(
                                    standalone.resolve("shards").toString()))) {
                standaloneShard = handle.future().get(30, TimeUnit.SECONDS);
            }
            try (ExportSnapshotHandle handle =
                    db.exportSnapshot(
                            original.snapshotId,
                            Config.VolumeDescriptor.singleVolume(
                                    export.resolve("shards").toString()))) {
                globalShard = handle.future().get(30, TimeUnit.SECONDS);
            }
            GlobalSnapshot global =
                    coordinator.takeExportedGlobalSnapshot(
                            1, Collections.singletonList(globalShard));
            coordinator.materializeExportedGlobalSnapshot(global, exportRoot);
        }
        SnapshotCleanupResult shardResult =
                SnapshotTools.cleanupShardSnapshot(
                        Config.VolumeDescriptor.singleVolume(
                                standalone.resolve("shards").toString()),
                        Collections.emptyList(),
                        standaloneShard.dbId,
                        standaloneShard.snapshotId);
        assertEquals(1, shardResult.deletedShardSnapshots);
        assertEquals(0, shardResult.deletedGlobalSnapshots);
        assertTrue(shardResult.deletedFiles > 0);
        assertFalse(Files.exists(path(standaloneShard.manifestPath)));

        SnapshotCleanupResult globalResult =
                SnapshotTools.cleanupGlobalSnapshot(exportRoot, Collections.emptyList(), 0);
        assertEquals(1, globalResult.deletedGlobalSnapshots);
        assertEquals(1, globalResult.deletedShardSnapshots);
        assertTrue(globalResult.deletedFiles > 0);
        assertFalse(Files.exists(export.resolve("snapshot/SNAPSHOT-0")));
        assertFalse(Files.exists(path(globalShard.manifestPath)));
        assertTrue(Files.exists(path(original.manifestPath)));
    }

    @Test
    void descriptorsRetainStorageFieldMappingAndInvalidArgumentsAreRejected(@TempDir Path root) {
        Config.VolumeDescriptor metadata = Config.VolumeDescriptor.singleVolume(root.toString());
        metadata.accessId = "test-access";
        metadata.secretKey = "test-secret";
        metadata.sizeLimit = 123L;
        metadata.customOptions = Collections.singletonMap("endpoint", "test-endpoint");
        JsonObject single = JsonParser.parseString(metadata.toJson()).getAsJsonObject();
        JsonObject listed =
                JsonParser.parseString(
                                Config.VolumeDescriptor.listToJson(
                                        Collections.singletonList(metadata)))
                        .getAsJsonArray()
                        .get(0)
                        .getAsJsonObject();
        assertEquals(single, listed);
        assertEquals(root.toString(), listed.get("base_dir").getAsString());
        assertEquals("test-access", listed.get("access_id").getAsString());
        assertEquals("test-secret", listed.get("secret_key").getAsString());
        assertEquals(123L, listed.get("size_limit").getAsLong());
        assertEquals(
                "test-endpoint",
                listed.getAsJsonObject("custom_options").get("endpoint").getAsString());
        assertFalse(listed.has("accessId"));
        assertTrue(new SnapshotCleanupOptions().preserveDependencies);
        assertFalse(new SnapshotCleanupOptions().includeOlderSnapshots);
        assertThrows(
                IllegalArgumentException.class,
                () -> SnapshotTools.cleanupGlobalSnapshot(null, Collections.emptyList(), 0));
        assertThrows(
                IllegalArgumentException.class,
                () -> SnapshotTools.cleanupGlobalSnapshot(metadata, null, 0));
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        SnapshotTools.cleanupGlobalSnapshot(
                                metadata, Collections.singletonList(null), 0));
        assertThrows(
                IllegalArgumentException.class,
                () -> SnapshotTools.cleanupGlobalSnapshot(metadata, Collections.emptyList(), -1));
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        SnapshotTools.cleanupGlobalSnapshot(
                                metadata, Collections.emptyList(), 0, null));
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        SnapshotTools.cleanupShardSnapshot(
                                metadata, Collections.emptyList(), " ", 0));
        Config.VolumeDescriptor local = Config.VolumeDescriptor.singleVolume(root.toString());
        assertThrows(
                IllegalStateException.class,
                () ->
                        SnapshotTools.cleanupShardSnapshot(
                                local, Collections.emptyList(), "../escape", 0));
    }

    private static Config config(Path root) {
        Config config = new Config().addVolume(root.toString()).numColumns(1).totalBuckets(1);
        config.memtableCapacity = 4 * 1024 * 1024;
        config.l0FileLimit = 100;
        config.snapshotOnFlush = false;
        config.snapshotOnlyTrack = true;
        config.valueSeparationThreshold = 16;
        return config;
    }

    private static ShardSnapshot snapshot(Db db, int key) {
        db.put(0, new byte[] {(byte) key}, 0, new byte[128]);
        return db.snapshot();
    }

    private static Path path(String path) {
        return path.startsWith("file:") ? Paths.get(URI.create(path)) : Paths.get(path);
    }
}
