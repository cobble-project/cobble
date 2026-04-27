package io.cobble.structured;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;

/** One direct-buffer-backed scan batch. The underlying buffers remain valid until closed. */
public final class DirectScanBatch implements AutoCloseable {
    private static final DirectScanBatch EMPTY =
            new DirectScanBatch(
                    ByteBuffer.allocateDirect(1),
                    0,
                    false,
                    new int[0],
                    new int[0],
                    new int[0],
                    new int[0],
                    () -> {});

    final boolean hasMore;
    private final ByteBuffer encoded;
    private final int size;
    private final int[] keyOffsets;
    private final int[] keyLengths;
    private final int[] rowOffsets;
    private final int[] rowLengths;
    private final Runnable onClose;
    private final AtomicBoolean closed;

    private DirectScanBatch(
            ByteBuffer encoded,
            int size,
            boolean hasMore,
            int[] keyOffsets,
            int[] keyLengths,
            int[] rowOffsets,
            int[] rowLengths,
            Runnable onClose) {
        this.encoded = encoded;
        this.size = size;
        this.hasMore = hasMore;
        this.keyOffsets = keyOffsets;
        this.keyLengths = keyLengths;
        this.rowOffsets = rowOffsets;
        this.rowLengths = rowLengths;
        this.onClose = onClose;
        this.closed = new AtomicBoolean(false);
    }

    static DirectScanBatch empty() {
        return EMPTY;
    }

    static DirectScanBatch decode(ByteBuffer encoded, int encodedLength, Runnable onClose) {
        ByteBuffer view = encoded.duplicate();
        ((Buffer) view).clear();
        if (encodedLength < 5 || encodedLength > view.capacity()) {
            throw new IllegalStateException(
                    "invalid encoded direct scan batch length: " + encodedLength);
        }
        ((Buffer) view).limit(encodedLength);

        int rowCount = readLength(view, "row count");
        ensureRemaining(view, 1);
        byte hasMoreByte = view.get();
        if (hasMoreByte != 0 && hasMoreByte != 1) {
            throw new IllegalStateException(
                    "invalid direct scan batch hasMore flag: " + hasMoreByte);
        }

        int[] keyOffsets = new int[rowCount];
        int[] keyLengths = new int[rowCount];
        int[] rowOffsets = new int[rowCount];
        int[] rowLengths = new int[rowCount];
        for (int i = 0; i < rowCount; i++) {
            keyLengths[i] = readLength(view, "key length");
            keyOffsets[i] = view.position();
            ensureRemaining(view, keyLengths[i]);
            ((Buffer) view).position(view.position() + keyLengths[i]);

            rowLengths[i] = readLength(view, "row length");
            rowOffsets[i] = view.position();
            ensureRemaining(view, rowLengths[i]);
            ((Buffer) view).position(view.position() + rowLengths[i]);
        }
        if (view.hasRemaining()) {
            throw new IllegalStateException("unexpected trailing bytes in direct scan batch");
        }
        return new DirectScanBatch(
                encoded,
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

    public DirectScanRow getRow(int index) {
        if (index < 0 || index >= size) {
            throw new IndexOutOfBoundsException("row index out of range: " + index);
        }
        ByteBuffer key = slice(encoded, keyOffsets[index], keyLengths[index]);
        ByteBuffer row = slice(encoded, rowOffsets[index], rowLengths[index]);
        return new DirectScanRow(key, DirectRow.decode(row, rowLengths[index], null));
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            onClose.run();
        }
    }

    private static void ensureRemaining(ByteBuffer view, int size) {
        if (size < 0 || view.remaining() < size) {
            throw new IllegalStateException("malformed direct scan batch payload");
        }
    }

    private static int readLength(ByteBuffer view, String fieldName) {
        ensureRemaining(view, Integer.BYTES);
        int len = view.getInt();
        if (len < 0) {
            throw new IllegalStateException("invalid " + fieldName + ": " + len);
        }
        return len;
    }

    private static ByteBuffer slice(ByteBuffer source, int offset, int length) {
        ByteBuffer view = source.duplicate();
        ((Buffer) view).clear();
        ((Buffer) view).position(offset);
        ((Buffer) view).limit(offset + length);
        return view.slice();
    }
}
