package io.cobble;

/**
 * Native-backed scan cursor for one bucket/range.
 *
 * <p>Use {@link #nextBatch()} repeatedly until {@code hasMore=false}, then close.
 */
public final class ScanCursor extends NativeObject {
    ScanCursor(long nativeHandle) {
        super(nativeHandle);
    }

    /**
     * Fetch next batch of rows from native side.
     *
     * @return batched keys/values for configured columns
     */
    public ScanBatch nextBatch() {
        if (nativeHandle == 0L) {
            throw new IllegalStateException("scan cursor is disposed");
        }
        ScanBatch batch = nextBatchInternal(nativeHandle);
        if (batch == null) {
            return ScanBatch.empty();
        }
        return batch;
    }

    @Override
    protected native void disposeInternal(long nativeHandle);

    private static native ScanBatch nextBatchInternal(long nativeHandle);
}
