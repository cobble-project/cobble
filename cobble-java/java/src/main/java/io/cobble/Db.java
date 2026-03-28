package io.cobble;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Java binding for writable Cobble database.
 *
 * <p>Use {@link #open(String)} or {@link #open(Config)} to create a DB instance, then call
 * put/get/delete operations. Remember to close with try-with-resources.
 */
public final class Db extends NativeObject {
    private Db(long nativeHandle) {
        super(nativeHandle);
    }

    /**
     * Open a writable DB from a config file path.
     *
     * @param configPath path to a JSON/YAML/TOML/INI config file
     * @return opened DB handle
     */
    public static Db open(String configPath) {
        NativeLoader.load();
        long nativeHandle = openHandle(configPath);
        if (nativeHandle == 0L) {
            throw new IllegalStateException("failed to open db");
        }
        return new Db(nativeHandle);
    }

    /**
     * Open a writable DB from Java {@link Config}.
     *
     * @param config Java-side config serialized to JSON and parsed by Rust
     * @return opened DB handle
     */
    public static Db open(Config config) {
        if (config == null) {
            throw new IllegalArgumentException("config must not be null");
        }
        NativeLoader.load();
        long nativeHandle = openHandleFromJson(config.toJson());
        if (nativeHandle == 0L) {
            throw new IllegalStateException("failed to open db from config json");
        }
        return new Db(nativeHandle);
    }

    /**
     * Restore a writable DB from one snapshot.
     *
     * @param configPath db config path
     * @param snapshotId snapshot id
     * @param dbId source db id
     * @return restored writable db
     */
    public static Db restore(String configPath, long snapshotId, String dbId) {
        NativeLoader.load();
        long nativeHandle = openFromSnapshotHandle(configPath, snapshotId, dbId);
        if (nativeHandle == 0L) {
            throw new IllegalStateException("failed to restore db from snapshot");
        }
        return new Db(nativeHandle);
    }

    /**
     * Restore a writable DB from one snapshot.
     *
     * @param config Java config
     * @param snapshotId snapshot id
     * @param dbId source db id
     * @return restored writable db
     */
    public static Db restore(Config config, long snapshotId, String dbId) {
        if (config == null) {
            throw new IllegalArgumentException("config must not be null");
        }
        NativeLoader.load();
        long nativeHandle = openFromSnapshotHandleFromJson(config.toJson(), snapshotId, dbId);
        if (nativeHandle == 0L) {
            throw new IllegalStateException("failed to restore db from snapshot config json");
        }
        return new Db(nativeHandle);
    }

    /**
     * Resume a writable DB from existing folder state.
     *
     * @param configPath db config path
     * @param dbId db id
     * @return resumed writable db
     */
    public static Db resume(String configPath, String dbId) {
        NativeLoader.load();
        long nativeHandle = resumeHandle(configPath, dbId);
        if (nativeHandle == 0L) {
            throw new IllegalStateException("failed to resume db");
        }
        return new Db(nativeHandle);
    }

    /**
     * Resume a writable DB from existing folder state.
     *
     * @param config Java config
     * @param dbId db id
     * @return resumed writable db
     */
    public static Db resume(Config config, String dbId) {
        if (config == null) {
            throw new IllegalArgumentException("config must not be null");
        }
        NativeLoader.load();
        long nativeHandle = resumeHandleFromJson(config.toJson(), dbId);
        if (nativeHandle == 0L) {
            throw new IllegalStateException("failed to resume db from config json");
        }
        return new Db(nativeHandle);
    }

    /**
     * Put one column value for a key.
     *
     * @param bucket bucket id
     * @param key logical user key
     * @param column column index
     * @param value raw bytes
     */
    public void put(int bucket, byte[] key, int column, byte[] value) {
        put(nativeHandle, bucket, key, column, value);
    }

    /**
     * Get one column value for a key.
     *
     * @return column bytes, or {@code null} when missing
     */
    public byte[] get(int bucket, byte[] key, int column) {
        return get(nativeHandle, bucket, key, column);
    }

    /** Open a high-throughput native scan cursor within [startKeyInclusive, endKeyExclusive). */
    public ScanCursor scan(
            int bucket, byte[] startKeyInclusive, byte[] endKeyExclusive, ScanOptions options) {
        if (options == null) {
            throw new IllegalArgumentException("options must not be null");
        }
        long handle =
                openScanCursor(
                        nativeHandle,
                        bucket,
                        startKeyInclusive,
                        endKeyExclusive,
                        options.nativeHandle);
        if (handle == 0L) {
            throw new IllegalStateException("failed to open scan cursor");
        }
        return new ScanCursor(handle);
    }

    /**
     * Delete one column value for a key.
     *
     * @param bucket bucket id
     * @param key logical user key
     * @param column column index
     */
    public void delete(int bucket, byte[] key, int column) {
        delete(nativeHandle, bucket, key, column);
    }

    /**
     * Return DB runtime id.
     *
     * @return unique db id string
     */
    public String id() {
        return id(nativeHandle);
    }

    /** Trigger snapshot creation asynchronously and return a future of shard snapshot payload. */
    public Future<ShardSnapshot> asyncSnapshot() {
        CompletableFuture<Long> snapshotIdFuture = new CompletableFuture<Long>();
        asyncSnapshot(nativeHandle, snapshotIdFuture);
        return snapshotIdFuture.thenApply(this::waitForShardSnapshotReady);
    }

    /**
     * Trigger snapshot creation and block until snapshot manifest is materialized.
     *
     * @return shard snapshot payload
     */
    public ShardSnapshot snapshot() {
        try {
            return asyncSnapshot().get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("snapshot interrupted", e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause() == null ? e : e.getCause();
            throw new IllegalStateException("snapshot failed: " + cause.getMessage(), cause);
        }
    }

    /**
     * Expire a snapshot and release related references.
     *
     * @param snapshotId target snapshot id
     * @return true if expired in this call
     */
    public boolean expireSnapshot(long snapshotId) {
        return expireSnapshot(nativeHandle, snapshotId);
    }

    /**
     * Retain a snapshot to avoid auto-expiration.
     *
     * @param snapshotId snapshot id
     * @return true if retain succeeded
     */
    public boolean retainSnapshot(long snapshotId) {
        return retainSnapshot(nativeHandle, snapshotId);
    }

    /**
     * Build one {@link ShardSnapshot} from a DB snapshot id.
     *
     * <p>The returned object is pure Java data (no native handle) and can be passed to {@link
     * DbCoordinator#materializeGlobalSnapshot(int, long, java.util.List)}.
     *
     * @param snapshotId local db snapshot id
     * @return shard snapshot input payload
     */
    public ShardSnapshot getShardSnapshot(long snapshotId) {
        return ShardSnapshot.fromJson(getShardSnapshotJson(nativeHandle, snapshotId));
    }

    private ShardSnapshot waitForShardSnapshotReady(long snapshotId) {
        IllegalStateException lastError = null;
        for (int i = 0; i < 120; i++) {
            try {
                ShardSnapshot snapshot = getShardSnapshot(snapshotId);
                if (snapshot != null
                        && snapshot.manifestPath != null
                        && !snapshot.manifestPath.isEmpty()
                        && manifestExists(snapshot.manifestPath)) {
                    return snapshot;
                }
            } catch (IllegalStateException e) {
                lastError = e;
            }
            try {
                Thread.sleep(50L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("interrupted while waiting snapshot ready", e);
            }
        }
        if (lastError != null) {
            throw lastError;
        }
        throw new IllegalStateException("snapshot manifest is unavailable: " + snapshotId);
    }

    private static boolean manifestExists(String manifestPath) {
        try {
            Path path;
            if (manifestPath.startsWith("file://")) {
                path = Paths.get(new URI(manifestPath));
            } else {
                path = Paths.get(manifestPath);
            }
            return Files.exists(path);
        } catch (URISyntaxException | IllegalArgumentException e) {
            return true;
        }
    }

    @Override
    protected native void disposeInternal(long nativeHandle);

    private static native long openHandle(String configPath);

    private static native long openHandleFromJson(String configJson);

    private static native long openFromSnapshotHandle(
            String configPath, long snapshotId, String dbId);

    private static native long openFromSnapshotHandleFromJson(
            String configJson, long snapshotId, String dbId);

    private static native long resumeHandle(String configPath, String dbId);

    private static native long resumeHandleFromJson(String configJson, String dbId);

    private static native void put(
            long nativeHandle, int bucket, byte[] key, int column, byte[] value);

    private static native byte[] get(long nativeHandle, int bucket, byte[] key, int column);

    private static native long openScanCursor(
            long nativeHandle,
            int bucket,
            byte[] startKeyInclusive,
            byte[] endKeyExclusive,
            long scanOptionsHandle);

    private static native void delete(long nativeHandle, int bucket, byte[] key, int column);

    private static native String id(long nativeHandle);

    private static native void asyncSnapshot(
            long nativeHandle, CompletableFuture<Long> snapshotIdFuture);

    private static native boolean expireSnapshot(long nativeHandle, long snapshotId);

    private static native boolean retainSnapshot(long nativeHandle, long snapshotId);

    private static native String getShardSnapshotJson(long nativeHandle, long snapshotId);
}
