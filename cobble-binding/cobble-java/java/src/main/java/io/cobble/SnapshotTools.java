package io.cobble;

import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

/** Snapshot maintenance helper APIs. */
public final class SnapshotTools {
    static {
        NativeLoader.load();
    }

    private SnapshotTools() {}

    /** Clean one shard snapshot with dependency preservation and no older-snapshot selection. */
    public static SnapshotCleanupResult cleanupShardSnapshot(
            Config.VolumeDescriptor metadataVolume,
            List<Config.VolumeDescriptor> otherVolumes,
            String dbId,
            long snapshotId) {
        return cleanupShardSnapshot(
                metadataVolume, otherVolumes, dbId, snapshotId, new SnapshotCleanupOptions());
    }

    /**
     * Remove selected shard snapshots without opening a database.
     *
     * <p>This is offline maintenance: no reader, writer, compactor, or snapshot publication may
     * overlap it. The metadata base is the parent of {@code dbId/snapshot}; for exported shards use
     * the export's {@code shards} directory. Other volumes supply routes and credentials (an empty
     * list is allowed). Only this shard's snapshots are preservation roots: callers must account
     * for global references, or use {@link #cleanupGlobalSnapshot} instead.
     *
     * <p>Disabling dependency preservation can break surviving snapshots. Older-snapshot selection
     * includes all shard IDs at or below {@code snapshotId}.
     */
    public static SnapshotCleanupResult cleanupShardSnapshot(
            Config.VolumeDescriptor metadataVolume,
            List<Config.VolumeDescriptor> otherVolumes,
            String dbId,
            long snapshotId,
            SnapshotCleanupOptions options) {
        validateCleanup(metadataVolume, otherVolumes, snapshotId, options);
        if (dbId == null || dbId.trim().isEmpty()) {
            throw new IllegalArgumentException("dbId must not be empty");
        }
        return SnapshotCleanupResult.fromJson(
                cleanupShardSnapshotFromJson(
                        metadataVolume.toJson(),
                        Config.VolumeDescriptor.listToJson(otherVolumes),
                        dbId,
                        snapshotId,
                        options.preserveDependencies,
                        options.includeOlderSnapshots));
    }

    /** Clean one global snapshot with dependency preservation and no older-snapshot selection. */
    public static SnapshotCleanupResult cleanupGlobalSnapshot(
            Config.VolumeDescriptor metadataVolume,
            List<Config.VolumeDescriptor> otherVolumes,
            long snapshotId) {
        return cleanupGlobalSnapshot(
                metadataVolume, otherVolumes, snapshotId, new SnapshotCleanupOptions());
    }

    /**
     * Remove selected global snapshots and their actual shard snapshot references.
     *
     * <p>This is offline maintenance: no reader, writer, compactor, or snapshot publication may
     * overlap it. The metadata base contains {@code snapshot/SNAPSHOT-<global-id>}; for exports use
     * the export root, not its {@code shards} directory. Other volumes supply routes and
     * credentials (an empty list is allowed).
     *
     * <p>Disabling dependency preservation can break surviving snapshots. Older-snapshot selection
     * compares global IDs at or below {@code snapshotId}, never shard IDs.
     */
    public static SnapshotCleanupResult cleanupGlobalSnapshot(
            Config.VolumeDescriptor metadataVolume,
            List<Config.VolumeDescriptor> otherVolumes,
            long snapshotId,
            SnapshotCleanupOptions options) {
        validateCleanup(metadataVolume, otherVolumes, snapshotId, options);
        return SnapshotCleanupResult.fromJson(
                cleanupGlobalSnapshotFromJson(
                        metadataVolume.toJson(),
                        Config.VolumeDescriptor.listToJson(otherVolumes),
                        snapshotId,
                        options.preserveDependencies,
                        options.includeOlderSnapshots));
    }

    private static void validateCleanup(
            Config.VolumeDescriptor metadataVolume,
            List<Config.VolumeDescriptor> otherVolumes,
            long snapshotId,
            SnapshotCleanupOptions options) {
        if (metadataVolume == null || otherVolumes == null || options == null) {
            throw new IllegalArgumentException(
                    "metadataVolume, otherVolumes, and options must not be null");
        }
        for (Config.VolumeDescriptor volume : otherVolumes) {
            if (volume == null) {
                throw new IllegalArgumentException("otherVolumes must not contain null");
            }
        }
        if (snapshotId < 0) {
            throw new IllegalArgumentException("snapshotId must be >= 0");
        }
    }

    /**
     * Prune one shard snapshot by dbId and snapshotId.
     *
     * <p>This is an out-of-band maintenance API. It does not rely on an existing Db instance.
     */
    public static boolean pruneShardSnapshot(Config config, String dbId, long snapshotId) {
        if (config == null) {
            throw new IllegalArgumentException("config must not be null");
        }
        if (dbId == null || dbId.trim().isEmpty()) {
            throw new IllegalArgumentException("dbId must not be empty");
        }
        if (snapshotId < 0) {
            throw new IllegalArgumentException("snapshotId must be >= 0");
        }
        return pruneShardSnapshotFromJson(config.toJson(), dbId, snapshotId);
    }

    /**
     * Load complete shard snapshot metadata without opening a live writer DB.
     *
     * <p>The manifest path must be absolute and accessible through {@code config}'s metadata
     * volume. This call only reads the manifest and captured schema metadata.
     */
    public static ShardSnapshot loadShardSnapshot(Config config, String dbId, String manifestPath) {
        if (config == null) {
            throw new IllegalArgumentException("config must not be null");
        }
        if (dbId == null || dbId.trim().isEmpty()) {
            throw new IllegalArgumentException("dbId must not be empty");
        }
        if (manifestPath == null || manifestPath.trim().isEmpty()) {
            throw new IllegalArgumentException("manifestPath must not be empty");
        }
        ShardSnapshot snapshot =
                ShardSnapshot.fromJson(
                        loadShardSnapshotFromJson(
                                config.toJson(), dbId, nativeManifestPath(manifestPath)));
        snapshot.manifestPath = manifestPath;
        return snapshot;
    }

    /**
     * Load a fixed global manifest without opening shard data or a writable coordinator.
     *
     * <p>The manifest path must be absolute and accessible through {@code config}'s metadata
     * volume. Metadata framing and checksums are verified by the native reader.
     */
    public static GlobalSnapshot loadGlobalSnapshot(Config config, String manifestPath) {
        if (config == null) {
            throw new IllegalArgumentException("config must not be null");
        }
        if (manifestPath == null || manifestPath.trim().isEmpty()) {
            throw new IllegalArgumentException("manifestPath must not be empty");
        }
        return GlobalSnapshot.fromJson(
                loadGlobalSnapshotFromJson(config.toJson(), nativeManifestPath(manifestPath)));
    }

    /**
     * Builds a validated fixed global manifest from complete shard metadata without opening a
     * coordinator or writing metadata.
     */
    public static GlobalSnapshot buildGlobalSnapshot(
            int totalBuckets, long snapshotId, List<ShardSnapshot> shards) {
        if (totalBuckets <= 0) {
            throw new IllegalArgumentException("totalBuckets must be > 0");
        }
        if (snapshotId < 0) {
            throw new IllegalArgumentException("snapshotId must be >= 0");
        }
        if (shards == null || shards.isEmpty()) {
            throw new IllegalArgumentException("shards must not be empty");
        }
        return GlobalSnapshot.fromJson(
                buildGlobalSnapshotFromJson(
                        totalBuckets, snapshotId, ShardSnapshot.listToJson(shards)));
    }

    /**
     * Converts local Java paths and file URIs to the absolute path expected by the Rust binding.
     */
    private static String nativeManifestPath(String manifestPath) {
        Path local = Paths.get(manifestPath);
        if (local.isAbsolute()) {
            return local.normalize().toString();
        }
        final URI uri;
        try {
            uri = URI.create(manifestPath);
        } catch (IllegalArgumentException ignored) {
            throw new IllegalArgumentException(
                    "manifestPath is not a valid absolute path or URI", ignored);
        }
        if (uri.getScheme() == null) {
            throw new IllegalArgumentException("manifestPath must be absolute");
        }
        if ("file".equalsIgnoreCase(uri.getScheme())) {
            return Paths.get(uri).toAbsolutePath().normalize().toString();
        }
        return manifestPath;
    }

    private static native boolean pruneShardSnapshotFromJson(
            String configJson, String dbId, long snapshotId);

    private static native String cleanupShardSnapshotFromJson(
            String metadataVolumeJson,
            String otherVolumesJson,
            String dbId,
            long snapshotId,
            boolean preserveDependencies,
            boolean includeOlderSnapshots);

    private static native String cleanupGlobalSnapshotFromJson(
            String metadataVolumeJson,
            String otherVolumesJson,
            long snapshotId,
            boolean preserveDependencies,
            boolean includeOlderSnapshots);

    private static native String loadShardSnapshotFromJson(
            String configJson, String dbId, String manifestPath);

    private static native String loadGlobalSnapshotFromJson(String configJson, String manifestPath);

    private static native String buildGlobalSnapshotFromJson(
            int totalBuckets, long snapshotId, String shardsJson);
}
