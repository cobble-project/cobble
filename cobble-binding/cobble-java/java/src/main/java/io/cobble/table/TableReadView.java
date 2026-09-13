package io.cobble.table;

import io.cobble.DirectScanCursor;
import io.cobble.NativeObject;

/** A private fixed raw read view retained by writable table projections and scans. */
final class TableReadView extends NativeObject {
    TableReadView(long nativeHandle) {
        super(nativeHandle);
    }

    byte[][] get(int bucket, byte[] key) {
        return get(nativeHandle, bucket, key);
    }

    byte[][][] multiGet(int[] buckets, byte[][] keys) {
        return multiGet(nativeHandle, buckets, keys);
    }

    DirectScanCursor scan(int bucket, byte[] startInclusive, byte[] endExclusive) {
        return openScanCursor(nativeHandle, bucket, startInclusive, endExclusive);
    }

    TableReadView copy() {
        return new TableReadView(cloneNative(nativeHandle));
    }

    @Override
    protected native void disposeInternal(long nativeHandle);

    private static native long cloneNative(long nativeHandle);

    private static native byte[][] get(long nativeHandle, int bucket, byte[] key);

    private static native byte[][][] multiGet(long nativeHandle, int[] buckets, byte[][] keys);

    private static native DirectScanCursor openScanCursor(
            long nativeHandle, int bucket, byte[] startInclusive, byte[] endExclusive);
}
