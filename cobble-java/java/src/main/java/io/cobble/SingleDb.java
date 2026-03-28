package io.cobble;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/** Java binding for cobble SingleDb. */
public final class SingleDb extends NativeObject {
    private SingleDb(long nativeHandle) {
        super(nativeHandle);
    }

    public static SingleDb open(String configPath) {
        NativeLoader.load();
        long nativeHandle = openHandle(configPath);
        if (nativeHandle == 0L) {
            throw new IllegalStateException("failed to open single db");
        }
        return new SingleDb(nativeHandle);
    }

    public static SingleDb open(Config config) {
        if (config == null) {
            throw new IllegalArgumentException("config must not be null");
        }
        NativeLoader.load();
        long nativeHandle = openHandleFromJson(config.toJson());
        if (nativeHandle == 0L) {
            throw new IllegalStateException("failed to open single db from config json");
        }
        return new SingleDb(nativeHandle);
    }

    public void put(int bucket, byte[] key, int column, byte[] value) {
        put(nativeHandle, bucket, key, column, value);
    }

    public void putWithOptions(
            int bucket, byte[] key, int column, byte[] value, WriteOptions options) {
        long writeOptionsHandle = options == null ? 0L : options.nativeHandle;
        putWithOptions(nativeHandle, bucket, key, column, value, writeOptionsHandle);
    }

    public void merge(int bucket, byte[] key, int column, byte[] value) {
        merge(nativeHandle, bucket, key, column, value);
    }

    public void mergeWithOptions(
            int bucket, byte[] key, int column, byte[] value, WriteOptions options) {
        long writeOptionsHandle = options == null ? 0L : options.nativeHandle;
        mergeWithOptions(nativeHandle, bucket, key, column, value, writeOptionsHandle);
    }

    public byte[] get(int bucket, byte[] key, int column) {
        try (ReadOptions options = ReadOptions.forColumn(column)) {
            return singleColumnOrNull(get(nativeHandle, bucket, key, options.nativeHandle));
        }
    }

    public byte[][] get(int bucket, byte[] key) {
        return get(nativeHandle, bucket, key, 0L);
    }

    public byte[][] getWithOptions(int bucket, byte[] key, ReadOptions options) {
        long readOptionsHandle = options == null ? 0L : options.nativeHandle;
        return get(nativeHandle, bucket, key, readOptionsHandle);
    }

    public void delete(int bucket, byte[] key, int column) {
        delete(nativeHandle, bucket, key, column);
    }

    public void setTime(int nextSeconds) {
        if (nextSeconds < 0) {
            throw new IllegalArgumentException("nextSeconds must be >= 0");
        }
        setTime(nativeHandle, nextSeconds);
    }

    /** Trigger snapshot creation asynchronously and return future of global snapshot payload. */
    public Future<GlobalSnapshot> asyncSnapshot() {
        CompletableFuture<Long> snapshotIdFuture = new CompletableFuture<Long>();
        asyncSnapshot(nativeHandle, snapshotIdFuture);
        return snapshotIdFuture.thenApply(this::waitForGlobalSnapshotReady);
    }

    /** Trigger snapshot creation and block until global snapshot manifest is materialized. */
    public GlobalSnapshot snapshot() {
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

    /** Retain one global snapshot and related local shard snapshot(s). */
    public boolean retainSnapshot(long snapshotId) {
        return retainSnapshot(nativeHandle, snapshotId);
    }

    /** Expire one global snapshot and related local shard snapshot(s). */
    public boolean expireSnapshot(long snapshotId) {
        return expireSnapshot(nativeHandle, snapshotId);
    }

    /** List global snapshots materialized by this single-node coordinator. */
    public List<GlobalSnapshot> listSnapshots() {
        return GlobalSnapshot.listFromJson(listSnapshotsJson(nativeHandle));
    }

    @Override
    protected native void disposeInternal(long nativeHandle);

    private static native long openHandle(String configPath);

    private static native long openHandleFromJson(String configJson);

    private static native void put(
            long nativeHandle, int bucket, byte[] key, int column, byte[] value);

    private static native void putWithOptions(
            long nativeHandle,
            int bucket,
            byte[] key,
            int column,
            byte[] value,
            long writeOptionsHandle);

    private static native void merge(
            long nativeHandle, int bucket, byte[] key, int column, byte[] value);

    private static native void mergeWithOptions(
            long nativeHandle,
            int bucket,
            byte[] key,
            int column,
            byte[] value,
            long writeOptionsHandle);

    private static native byte[][] get(
            long nativeHandle, int bucket, byte[] key, long readOptionsHandle);

    private static native void delete(long nativeHandle, int bucket, byte[] key, int column);

    private static native void setTime(long nativeHandle, int nextSeconds);

    private static native void asyncSnapshot(
            long nativeHandle, CompletableFuture<Long> snapshotIdFuture);

    private static native boolean retainSnapshot(long nativeHandle, long snapshotId);

    private static native boolean expireSnapshot(long nativeHandle, long snapshotId);

    private static native String listSnapshotsJson(long nativeHandle);

    private GlobalSnapshot waitForGlobalSnapshotReady(long snapshotId) {
        IllegalStateException lastError = null;
        for (int i = 0; i < 120; i++) {
            try {
                GlobalSnapshot snapshot = getSnapshot(snapshotId);
                if (snapshot != null) {
                    return snapshot;
                }
            } catch (IllegalStateException e) {
                lastError = e;
            }
            try {
                Thread.sleep(50L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(
                        "interrupted while waiting global snapshot ready", e);
            }
        }
        if (lastError != null) {
            throw lastError;
        }
        throw new IllegalStateException("global snapshot is unavailable: " + snapshotId);
    }

    private GlobalSnapshot getSnapshot(long snapshotId) {
        return listSnapshots().stream()
                .filter(snapshot -> snapshot.id == snapshotId)
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("snapshot not found: " + snapshotId));
    }

    private static byte[] singleColumnOrNull(byte[][] columns) {
        if (columns == null) {
            return null;
        }
        if (columns.length != 1) {
            throw new IllegalStateException(
                    "expected exactly one selected column, got " + columns.length);
        }
        return columns[0];
    }
}
