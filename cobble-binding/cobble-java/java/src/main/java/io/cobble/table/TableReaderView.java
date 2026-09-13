package io.cobble.table;

import io.cobble.DirectScanCursor;
import io.cobble.NativeObject;
import io.cobble.ReadOptions;
import io.cobble.ScanOptions;

final class TableReaderView extends NativeObject {
    TableReaderView(long nativeHandle) {
        super(nativeHandle);
    }

    byte[][] get(int bucket, byte[] key, ReadOptions options) {
        return get(nativeHandle, bucket, key, options.getNativeHandle());
    }

    byte[][][] multiGet(int[] buckets, byte[][] keys, ReadOptions options) {
        return multiGet(nativeHandle, buckets, keys, options.getNativeHandle());
    }

    DirectScanCursor scan(
            int bucket, byte[] startInclusive, byte[] endExclusive, ScanOptions options) {
        return openScanCursor(
                nativeHandle, bucket, startInclusive, endExclusive, options.getNativeHandle());
    }

    TableReaderView copy() {
        return new TableReaderView(TableReader.cloneViewNative(nativeHandle));
    }

    @Override
    protected native void disposeInternal(long nativeHandle);

    private static native byte[][] get(long nativeHandle, int bucket, byte[] key, long options);

    private static native byte[][][] multiGet(
            long nativeHandle, int[] buckets, byte[][] keys, long options);

    private static native DirectScanCursor openScanCursor(
            long nativeHandle,
            int bucket,
            byte[] startInclusive,
            byte[] endExclusive,
            long options);
}
