package io.cobble.structured;

import io.cobble.NativeObject;

/**
 * A native-backed scan cursor that yields typed rows from a structured database.
 *
 * <p>Use {@link #nextBatch()} repeatedly until {@link ScanBatch#hasMore} is false, then close.
 */
public final class ScanCursor extends NativeObject {

    ScanCursor(long nativeHandle) {
        super(nativeHandle);
    }

    /**
     * Fetches the next batch of typed rows from the native iterator.
     *
     * @return a batch of rows; empty batch when exhausted
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
