package io.cobble;

import java.io.IOException;
import java.io.InputStream;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Encoded direct-buffer-backed raw row returned by {@link Db} direct read APIs.
 *
 * <p>The payload stores selected columns as presence flag + length + bytes. Callers can decode a
 * column with an {@link InputStream}-based mapper without materializing intermediate heap arrays.
 */
public final class DirectEncodedRow implements AutoCloseable {
    private static final Runnable NO_OP =
            new Runnable() {
                @Override
                public void run() {}
            };

    private final ByteBuffer buffer;
    private final int length;
    private final Runnable releaser;
    private final AtomicBoolean closed;

    DirectEncodedRow(ByteBuffer buffer, int length, Runnable releaser) {
        this.buffer = buffer;
        this.length = length;
        this.releaser = releaser == null ? NO_OP : releaser;
        this.closed = new AtomicBoolean(false);
    }

    /** Returns the underlying encoded direct buffer. */
    public ByteBuffer buffer() {
        return buffer;
    }

    /** Returns encoded payload length in bytes. */
    public int length() {
        return length;
    }

    /** Returns the selected column count in this encoded row payload. */
    public int size() {
        ByteBuffer view = rowView();
        return readLength(view, "column count");
    }

    /** Returns whether the target selected column is missing/null. */
    public boolean isNull(int column) {
        ByteBuffer view = rowView();
        int columnCount = readLength(view, "column count");
        ensureColumnInRange(column, columnCount);
        for (int i = 0; i < columnCount; i++) {
            ensureRemaining(view, 1);
            int present = view.get() & 0xFF;
            if (present == 0) {
                if (i == column) {
                    return true;
                }
                continue;
            }
            if (present != 1) {
                throw new IllegalStateException(
                        "invalid direct encoded row payload: bad presence flag " + present);
            }
            int valueLength = readLength(view, "column length");
            if (i == column) {
                return false;
            }
            skipBytes(view, valueLength);
        }
        throw new IllegalStateException("missing column in encoded direct row: " + column);
    }

    /**
     * Decodes one selected column through a caller-provided stream decoder.
     *
     * @return mapped value, or {@code null} when the selected column is missing/null
     */
    public <T> T decodeColumn(int column, ColumnDecoder<T> decoder) throws IOException {
        if (decoder == null) {
            throw new NullPointerException("decoder");
        }
        ByteBuffer view = rowView();
        int columnCount = readLength(view, "column count");
        ensureColumnInRange(column, columnCount);
        ReusableByteBufferInputStream inputStream = new ReusableByteBufferInputStream();
        for (int i = 0; i < columnCount; i++) {
            ensureRemaining(view, 1);
            int present = view.get() & 0xFF;
            if (present == 0) {
                if (i == column) {
                    return null;
                }
                continue;
            }
            if (present != 1) {
                throw new IllegalStateException(
                        "invalid direct encoded row payload: bad presence flag " + present);
            }
            int valueLength = readLength(view, "column length");
            if (i != column) {
                skipBytes(view, valueLength);
                continue;
            }
            ByteBuffer value = sliceCurrent(view, valueLength);
            inputStream.reset(value);
            return decoder.decode(inputStream);
        }
        throw new IllegalStateException("missing column in encoded direct row: " + column);
    }

    Runnable takeReleaser() {
        if (closed.compareAndSet(false, true)) {
            return releaser;
        }
        return NO_OP;
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            releaser.run();
        }
    }

    private ByteBuffer rowView() {
        ByteBuffer view = buffer.duplicate();
        ((Buffer) view).clear();
        if (length < Integer.BYTES || length > view.capacity()) {
            throw new IllegalStateException("invalid encoded direct row length: " + length);
        }
        ((Buffer) view).limit(length);
        return view;
    }

    private static void ensureColumnInRange(int column, int columnCount) {
        if (column < 0 || column >= columnCount) {
            throw new IllegalArgumentException("column out of range: " + column);
        }
    }

    private static int readLength(ByteBuffer view, String fieldName) {
        ensureRemaining(view, Integer.BYTES);
        int value = view.getInt();
        if (value < 0) {
            throw new IllegalStateException("invalid " + fieldName + ": " + value);
        }
        return value;
    }

    private static void skipBytes(ByteBuffer view, int bytesLength) {
        ensureRemaining(view, bytesLength);
        ((Buffer) view).position(view.position() + bytesLength);
    }

    private static ByteBuffer sliceCurrent(ByteBuffer view, int bytesLength) {
        ensureRemaining(view, bytesLength);
        ByteBuffer slice = view.slice();
        ((Buffer) slice).limit(bytesLength);
        ((Buffer) view).position(view.position() + bytesLength);
        return slice;
    }

    private static void ensureRemaining(ByteBuffer view, int size) {
        if (size < 0 || view.remaining() < size) {
            throw new IllegalStateException("malformed encoded direct row payload");
        }
    }

    /** Decoder function that maps one column payload stream to a typed value. */
    @FunctionalInterface
    public interface ColumnDecoder<T> {
        T decode(InputStream input) throws IOException;
    }

    private static final class ReusableByteBufferInputStream extends InputStream {
        private ByteBuffer current;

        private void reset(ByteBuffer source) {
            this.current = source.duplicate();
        }

        @Override
        public int read() {
            if (current == null || !current.hasRemaining()) {
                return -1;
            }
            return current.get() & 0xFF;
        }

        @Override
        public int read(byte[] bytes, int offset, int length) {
            if (current == null || !current.hasRemaining()) {
                return -1;
            }
            int readable = Math.min(length, current.remaining());
            current.get(bytes, offset, readable);
            return readable;
        }
    }
}
