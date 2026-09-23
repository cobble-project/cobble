package io.cobble.table;

import io.cobble.DirectScanCursor;
import io.cobble.NativeObject;
import io.cobble.ReadOptions;
import io.cobble.ScanOptions;

import java.nio.ByteBuffer;

final class TableReaderView extends NativeObject {
    TableReaderView(long nativeHandle) {
        super(nativeHandle);
    }

    int getEncodedDirect(int bucket, ByteBuffer buffer, int keyLength, ReadOptions options) {
        return getEncodedDirectNative(
                nativeHandle, bucket, buffer, keyLength, options.getNativeHandle());
    }

    int multiGetEncodedDirect(ByteBuffer buffer, ReadOptions options) {
        return multiGetEncodedDirectNative(nativeHandle, buffer, options.getNativeHandle());
    }

    ByteBuffer takeDirectOverflowBuffer() {
        return takeDirectOverflowNative();
    }

    DirectScanCursor scan(
            int bucket, byte[] startInclusive, byte[] endExclusive, ScanOptions options) {
        return openScanCursor(
                nativeHandle, bucket, startInclusive, endExclusive, options.getNativeHandle());
    }

    TableReaderView copy() {
        return new TableReaderView(NativeTableReader.cloneViewNative(nativeHandle));
    }

    TableScanPlan scanPlan() {
        return TableScanPlan.fromResponseJson(scanPlanNative(nativeHandle));
    }

    @Override
    protected native void disposeInternal(long nativeHandle);

    private static native int getEncodedDirectNative(
            long nativeHandle, int bucket, ByteBuffer buffer, int keyLength, long options);

    private static native int multiGetEncodedDirectNative(
            long nativeHandle, ByteBuffer buffer, long options);

    private static native ByteBuffer takeDirectOverflowNative();

    private static native DirectScanCursor openScanCursor(
            long nativeHandle,
            int bucket,
            byte[] startInclusive,
            byte[] endExclusive,
            long options);

    private static native String scanPlanNative(long nativeHandle);
}
