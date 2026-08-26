package io.cobble;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;

/** Zero-copy direct-buffer-backed raw columns returned from direct JNI reads. */
public final class DirectColumns implements AutoCloseable {
    private final ByteBuffer[] columns;
    private final Runnable onClose;
    private final AtomicBoolean closed;

    private DirectColumns(ByteBuffer[] columns, Runnable onClose) {
        this.columns = columns;
        this.onClose = onClose;
        this.closed = new AtomicBoolean(false);
    }

    static DirectColumns decode(ByteBuffer encoded, int encodedLength, Runnable onClose) {
        ByteBuffer view = encoded.duplicate();
        ((Buffer) view).clear();
        if (encodedLength < Integer.BYTES || encodedLength > view.capacity()) {
            throw new IllegalStateException(
                    "invalid encoded direct columns length: " + encodedLength);
        }
        ((Buffer) view).limit(encodedLength);

        int columnCount = view.getInt();
        if (columnCount < 0) {
            throw new IllegalStateException("invalid column count: " + columnCount);
        }
        ByteBuffer[] columns = new ByteBuffer[columnCount];
        for (int i = 0; i < columnCount; i++) {
            ensureRemaining(view, 1);
            int present = view.get() & 0xFF;
            if (present == 0) {
                columns[i] = null;
                continue;
            }
            if (present != 1) {
                throw new IllegalStateException(
                        "invalid direct columns payload: bad presence flag "
                                + present
                                + " for column "
                                + i);
            }
            int length = readLength(view, "column length");
            columns[i] = sliceCurrent(view, length);
        }
        if (view.hasRemaining()) {
            throw new IllegalStateException("unexpected trailing bytes in direct columns payload");
        }
        return new DirectColumns(columns, onClose == null ? () -> {} : onClose);
    }

    public int size() {
        return columns.length;
    }

    public ByteBuffer get(int column) {
        if (column < 0 || column >= columns.length) {
            throw new IllegalArgumentException("column out of range: " + column);
        }
        ByteBuffer value = columns[column];
        return value == null ? null : value.duplicate();
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            onClose.run();
        }
    }

    private static void ensureRemaining(ByteBuffer view, int size) {
        if (size < 0 || view.remaining() < size) {
            throw new IllegalStateException("malformed direct columns payload");
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

    private static ByteBuffer sliceCurrent(ByteBuffer view, int len) {
        ensureRemaining(view, len);
        ByteBuffer slice = view.slice();
        ((Buffer) slice).limit(len);
        ((Buffer) view).position(view.position() + len);
        return slice;
    }
}
