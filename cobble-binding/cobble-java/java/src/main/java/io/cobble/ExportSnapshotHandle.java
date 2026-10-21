package io.cobble;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/** An independent snapshot export. Completion follows all copy cleanup and source-pin release. */
public final class ExportSnapshotHandle extends NativeObject {
    private final CompletableFuture<ShardSnapshot> future;

    private ExportSnapshotHandle(long handle, CompletableFuture<String> jsonFuture) {
        super(handle);
        future = jsonFuture.thenApply(ShardSnapshot::fromJson);
        jsonFuture.whenComplete((snapshot, error) -> close());
    }

    /** Internal cross-package binding entrypoint; prefer Db.exportSnapshot. */
    public static ExportSnapshotHandle start(
            long dbHandle,
            long snapshotId,
            Config.VolumeDescriptor destination,
            boolean structured) {
        return start(dbHandle, snapshotId, destination, structured, false);
    }

    /** Internal cross-package binding entrypoint; prefer Db.snapshotTo. */
    public static ExportSnapshotHandle startSnapshotTo(
            long dbHandle, Config.VolumeDescriptor destination, boolean structured) {
        return start(dbHandle, 0, destination, structured, true);
    }

    private static ExportSnapshotHandle start(
            long dbHandle,
            long snapshotId,
            Config.VolumeDescriptor destination,
            boolean structured,
            boolean captureCurrent) {
        Objects.requireNonNull(destination, "destination");
        CompletableFuture<String> jsonFuture = new CompletableFuture<>();
        return new ExportSnapshotHandle(
                startNative(
                        dbHandle,
                        snapshotId,
                        destination.toJson(),
                        structured,
                        captureCurrent,
                        jsonFuture),
                jsonFuture);
    }

    /** The result future. Use cancel() to request native cancellation, then await completion. */
    public CompletableFuture<ShardSnapshot> future() {
        return future;
    }

    /** Request cancellation before manifest publication starts; started I/O is drained first. */
    public synchronized boolean cancel() {
        return nativeHandle != 0L && cancelNative(nativeHandle);
    }

    /**
     * Release this native handle only. Pending work remains protected; cancellation is separate.
     */
    @Override
    public synchronized void close() {
        super.close();
    }

    @Override
    protected void disposeInternal(long handle) {
        disposeNative(handle);
    }

    private static native long startNative(
            long dbHandle,
            long snapshotId,
            String destinationJson,
            boolean structured,
            boolean captureCurrent,
            CompletableFuture<String> jsonFuture);

    private static native boolean cancelNative(long handle);

    private static native void disposeNative(long handle);
}
