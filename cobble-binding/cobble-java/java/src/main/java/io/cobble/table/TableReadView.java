package io.cobble.table;

import io.cobble.DirectScanCursor;
import io.cobble.NativeObject;

import java.nio.ByteBuffer;

/** A private fixed raw read view retained by writable table projections and scans. */
final class TableReadView extends NativeObject {
    TableReadView(long nativeHandle) {
        super(nativeHandle);
    }

    int getEncodedDirect(int bucket, ByteBuffer buffer, int keyLength) {
        return getEncodedDirectNative(nativeHandle, bucket, buffer, keyLength);
    }

    int multiGetEncodedDirect(ByteBuffer buffer) {
        return multiGetEncodedDirectNative(nativeHandle, buffer);
    }

    ByteBuffer takeDirectOverflowBuffer() {
        return takeDirectOverflowNative();
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

    private static native int getEncodedDirectNative(
            long nativeHandle, int bucket, ByteBuffer buffer, int keyLength);

    private static native int multiGetEncodedDirectNative(long nativeHandle, ByteBuffer buffer);

    private static native ByteBuffer takeDirectOverflowNative();

    private static native DirectScanCursor openScanCursor(
            long nativeHandle, int bucket, byte[] startInclusive, byte[] endExclusive);
}
