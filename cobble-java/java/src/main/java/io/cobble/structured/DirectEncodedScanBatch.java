package io.cobble.structured;

import io.cobble.DirectIoUtils;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * One encoded direct-buffer-backed structured scan batch.
 *
 * <p>The backing batch buffer remains valid until {@link #close()}.
 */
public final class DirectEncodedScanBatch implements AutoCloseable {
    private static final String PAYLOAD_NAME = "encoded structured direct scan batch payload";

    private static final DirectEncodedScanBatch EMPTY =
            new DirectEncodedScanBatch(
                    ByteBuffer.allocateDirect(1),
                    0,
                    0,
                    false,
                    new int[0],
                    new int[0],
                    new int[0],
                    new int[0],
                    () -> {});

    public final boolean hasMore;
    private final ByteBuffer encoded;
    private final int encodedLength;
    private final int size;
    private final int[] keyOffsets;
    private final int[] keyLengths;
    private final int[] rowOffsets;
    private final int[] rowLengths;
    private final Runnable onClose;
    private final AtomicBoolean closed;
    private int nextRowIndex;

    private DirectEncodedScanBatch(
            ByteBuffer encoded,
            int encodedLength,
            int size,
            boolean hasMore,
            int[] keyOffsets,
            int[] keyLengths,
            int[] rowOffsets,
            int[] rowLengths,
            Runnable onClose) {
        this.encoded = encoded;
        this.encodedLength = encodedLength;
        this.size = size;
        this.hasMore = hasMore;
        this.keyOffsets = keyOffsets;
        this.keyLengths = keyLengths;
        this.rowOffsets = rowOffsets;
        this.rowLengths = rowLengths;
        this.onClose = onClose;
        this.closed = new AtomicBoolean(false);
        this.nextRowIndex = 0;
    }

    static DirectEncodedScanBatch empty() {
        return EMPTY;
    }

    static DirectEncodedScanBatch decode(ByteBuffer encoded, int encodedLength, Runnable onClose) {
        long address = DirectIoUtils.directAddress(encoded);
        if (encodedLength < 5 || encodedLength > encoded.capacity()) {
            throw new IllegalStateException(
                    "invalid encoded direct scan batch length: " + encodedLength);
        }
        int offset = 0;

        int rowCount =
                DirectIoUtils.readLength(address, encodedLength, offset, "row count", PAYLOAD_NAME);
        offset += Integer.BYTES;
        byte hasMoreByte = DirectIoUtils.readByte(address, encodedLength, offset, PAYLOAD_NAME);
        offset += 1;
        if (hasMoreByte != 0 && hasMoreByte != 1) {
            throw new IllegalStateException(
                    "invalid direct scan batch hasMore flag: " + hasMoreByte);
        }

        int[] keyOffsets = new int[rowCount];
        int[] keyLengths = new int[rowCount];
        int[] rowOffsets = new int[rowCount];
        int[] rowLengths = new int[rowCount];
        for (int i = 0; i < rowCount; i++) {
            keyLengths[i] =
                    DirectIoUtils.readLength(
                            address, encodedLength, offset, "key length", PAYLOAD_NAME);
            offset += Integer.BYTES;
            keyOffsets[i] = offset;
            DirectIoUtils.ensureRemaining(encodedLength, offset, keyLengths[i], PAYLOAD_NAME);
            offset += keyLengths[i];

            rowLengths[i] =
                    DirectIoUtils.readLength(
                            address, encodedLength, offset, "row length", PAYLOAD_NAME);
            offset += Integer.BYTES;
            rowOffsets[i] = offset;
            DirectIoUtils.ensureRemaining(encodedLength, offset, rowLengths[i], PAYLOAD_NAME);
            offset += rowLengths[i];
        }
        if (offset != encodedLength) {
            throw new IllegalStateException("unexpected trailing bytes in direct scan batch");
        }
        return new DirectEncodedScanBatch(
                encoded,
                encodedLength,
                rowCount,
                hasMoreByte == 1,
                keyOffsets,
                keyLengths,
                rowOffsets,
                rowLengths,
                onClose == null ? () -> {} : onClose);
    }

    public int size() {
        return size;
    }

    public ByteBuffer getKey(int index) {
        checkIndex(index);
        return DirectIoUtils.slice(encoded, keyOffsets[index], keyLengths[index], PAYLOAD_NAME);
    }

    public DirectEncodedRow getEncodedRow(int index) {
        checkIndex(index);
        return new DirectEncodedRow(
                DirectIoUtils.directAddress(encoded) + rowOffsets[index], rowLengths[index], null);
    }

    /**
     * Returns the next encoded scan row in this batch, or {@code null} when consumed.
     *
     * <p>The returned row stays valid until this batch is closed.
     */
    public DirectEncodedScanRow nextRow() {
        if (nextRowIndex >= size) {
            return null;
        }
        int index = nextRowIndex++;
        ByteBuffer key =
                DirectIoUtils.slice(encoded, keyOffsets[index], keyLengths[index], PAYLOAD_NAME);
        return new DirectEncodedScanRow(
                key,
                new DirectEncodedRow(
                        DirectIoUtils.directAddress(encoded) + rowOffsets[index],
                        rowLengths[index],
                        null));
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            onClose.run();
        }
    }

    private void checkIndex(int index) {
        if (index < 0 || index >= size) {
            throw new IndexOutOfBoundsException("row index out of range: " + index);
        }
    }
}
