package io.cobble.table;

import io.cobble.Config;
import io.cobble.GlobalSnapshot;
import io.cobble.NativeLoader;
import io.cobble.NativeObject;
import io.cobble.ShardSnapshot;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * In-process collector and committer for table shard snapshots.
 *
 * <p>Pending commits are kept only in memory and bounded by {@code maxPendingCommits}; callers must
 * replay incomplete commits after replacement or failure. Commit IDs order work within this
 * committer and are independent from the returned global snapshot IDs. This version assumes one
 * active committer across processes.
 *
 * <p>Closing this object must not race with {@link #submit(long, ShardSnapshot)} or {@link
 * #commitBatch(long, List)}.
 */
public final class TableSnapshotCommitter extends NativeObject {
    private static final Gson GSON = new GsonBuilder().create();

    private TableSnapshotCommitter(long nativeHandle) {
        super(nativeHandle);
    }

    /** Opens a committer with an independent coordinator using a config file. */
    public static TableSnapshotCommitter open(
            String configPath, int totalBuckets, int maxPendingCommits) {
        Objects.requireNonNull(configPath, "configPath");
        validateLimits(totalBuckets, maxPendingCommits);
        NativeLoader.load();
        long handle = openHandle(configPath, totalBuckets, maxPendingCommits);
        if (handle == 0L) {
            throw new IllegalStateException("failed to open table snapshot committer");
        }
        return new TableSnapshotCommitter(handle);
    }

    /** Opens a committer with an independent coordinator using a Java config. */
    public static TableSnapshotCommitter open(
            Config config, int totalBuckets, int maxPendingCommits) {
        if (config == null) {
            throw new IllegalArgumentException("config must not be null");
        }
        validateLimits(totalBuckets, maxPendingCommits);
        NativeLoader.load();
        long handle = openHandleFromJson(config.toJson(), totalBuckets, maxPendingCommits);
        if (handle == 0L) {
            throw new IllegalStateException("failed to open table snapshot committer");
        }
        return new TableSnapshotCommitter(handle);
    }

    /**
     * Submits one shard snapshot.
     *
     * @return the newly materialized global snapshot, or {@code null} while pending or superseded
     */
    public synchronized GlobalSnapshot submit(long commitId, ShardSnapshot snapshot) {
        ensureOpen();
        requireCommitId(commitId);
        Objects.requireNonNull(snapshot, "snapshot");
        String json =
                submitJson(nativeHandle, commitId, GSON.toJson(Collections.singleton(snapshot)));
        return json == null ? null : GlobalSnapshot.fromJson(json);
    }

    /**
     * Commits an already collected, complete checkpoint batch.
     *
     * @return the newly materialized global snapshot, or {@code null} when already superseded
     */
    public synchronized GlobalSnapshot commitBatch(long commitId, List<ShardSnapshot> snapshots) {
        ensureOpen();
        requireCommitId(commitId);
        if (snapshots == null || snapshots.isEmpty()) {
            throw new IllegalArgumentException("snapshots must not be empty");
        }
        for (ShardSnapshot snapshot : snapshots) {
            Objects.requireNonNull(snapshot, "snapshot");
        }
        String json = commitBatchJson(nativeHandle, commitId, GSON.toJson(snapshots));
        return json == null ? null : GlobalSnapshot.fromJson(json);
    }

    @Override
    public synchronized void close() {
        super.close();
    }

    @Override
    protected native void disposeInternal(long nativeHandle);

    private static void validateLimits(int totalBuckets, int maxPendingCommits) {
        if (totalBuckets <= 0 || totalBuckets > 65536) {
            throw new IllegalArgumentException("totalBuckets must be in range 1..=65536");
        }
        if (maxPendingCommits <= 0) {
            throw new IllegalArgumentException("maxPendingCommits must be positive");
        }
    }

    private static void requireCommitId(long commitId) {
        if (commitId < 0L) {
            throw new IllegalArgumentException("commitId must be non-negative");
        }
    }

    private void ensureOpen() {
        if (isDisposed()) {
            throw new IllegalStateException("table snapshot committer is closed");
        }
    }

    private static native long openHandle(
            String configPath, int totalBuckets, int maxPendingCommits);

    private static native long openHandleFromJson(
            String configJson, int totalBuckets, int maxPendingCommits);

    private static native String submitJson(
            long nativeHandle, long commitId, String shardSnapshotJson);

    private static native String commitBatchJson(
            long nativeHandle, long commitId, String shardSnapshotsJson);
}
