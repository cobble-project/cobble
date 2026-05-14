package io.cobble;

import java.util.concurrent.CompletableFuture;

/** Holds the allocated snapshot id together with its async completion future. */
public final class PendingSnapshot<T> {
    private final long snapshotId;
    private final CompletableFuture<T> future;

    public PendingSnapshot(long snapshotId, CompletableFuture<T> future) {
        this.snapshotId = snapshotId;
        this.future = future;
    }

    public long snapshotId() {
        return snapshotId;
    }

    public CompletableFuture<T> future() {
        return future;
    }
}
