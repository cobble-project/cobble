package io.cobble.structured;

import java.io.IOException;
import java.io.InputStream;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Encoded direct-buffer-backed structured row returned by {@link Db} direct read APIs.
 *
 * <p>The payload keeps Cobble's internal structured row encoding and lets callers decode a target
 * column through an {@link InputStream}-based mapper without materializing intermediate heap
 * arrays.
 */
public final class DirectEncodedRow implements AutoCloseable {
    private static final byte TAG_NULL = 0;
    private static final byte TAG_BYTES = 1;
    private static final byte TAG_LIST = 2;

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

    /** Returns the number of columns encoded in this row. */
    public int columnCount() {
        ByteBuffer view = rowView();
        return readLength(view, "column count");
    }

    /** Returns whether the target column is NULL. */
    public boolean isNull(int column) {
        ByteBuffer view = rowView();
        int columnCount = readLength(view, "column count");
        ensureColumnInRange(column, columnCount);
        for (int i = 0; i < columnCount; i++) {
            ensureRemaining(view, 1);
            byte tag = view.get();
            switch (tag) {
                case TAG_NULL:
                    if (i == column) {
                        return true;
                    }
                    break;
                case TAG_BYTES:
                    int bytesLength = readLength(view, "bytes length");
                    if (i == column) {
                        return false;
                    }
                    skipBytes(view, bytesLength);
                    break;
                case TAG_LIST:
                    int elementCount = readLength(view, "list element count");
                    if (i == column) {
                        return false;
                    }
                    skipList(view, elementCount);
                    break;
                default:
                    throw new IllegalStateException("unknown direct row column tag: " + tag);
            }
        }
        throw new IllegalStateException("missing column in encoded direct row: " + column);
    }

    /**
     * Decodes one BYTES column with a caller-provided {@link InputStream} decoder.
     *
     * @return mapped value, or {@code null} if the target column is NULL
     */
    public <T> T decodeBytesColumn(int column, ColumnDecoder<T> decoder) throws IOException {
        if (decoder == null) {
            throw new NullPointerException("decoder");
        }
        ByteBuffer view = rowView();
        int columnCount = readLength(view, "column count");
        ensureColumnInRange(column, columnCount);
        ReusableByteBufferInputStream inputStream = new ReusableByteBufferInputStream();
        for (int i = 0; i < columnCount; i++) {
            ensureRemaining(view, 1);
            byte tag = view.get();
            switch (tag) {
                case TAG_NULL:
                    if (i == column) {
                        return null;
                    }
                    break;
                case TAG_BYTES:
                    int bytesLength = readLength(view, "bytes length");
                    if (i == column) {
                        ByteBuffer bytes = sliceCurrent(view, bytesLength);
                        inputStream.reset(bytes);
                        return decoder.decode(inputStream);
                    }
                    skipBytes(view, bytesLength);
                    break;
                case TAG_LIST:
                    int elementCount = readLength(view, "list element count");
                    if (i == column) {
                        throw new IllegalStateException(
                                "expected BYTES column in direct row but found LIST");
                    }
                    skipList(view, elementCount);
                    break;
                default:
                    throw new IllegalStateException("unknown direct row column tag: " + tag);
            }
        }
        throw new IllegalStateException("missing column in encoded direct row: " + column);
    }

    /**
     * Decodes one LIST column with a caller-provided element decoder.
     *
     * <p>The decoder is invoked once for each list element and receives an {@link InputStream} view
     * of that element payload.
     *
     * @return decoded list, or {@code null} if the target column is NULL
     */
    public <T> List<T> decodeListColumn(int column, ColumnDecoder<T> decoder) throws IOException {
        if (decoder == null) {
            throw new NullPointerException("decoder");
        }
        ByteBuffer view = rowView();
        int columnCount = readLength(view, "column count");
        ensureColumnInRange(column, columnCount);
        ReusableByteBufferInputStream inputStream = new ReusableByteBufferInputStream();
        for (int i = 0; i < columnCount; i++) {
            ensureRemaining(view, 1);
            byte tag = view.get();
            switch (tag) {
                case TAG_NULL:
                    if (i == column) {
                        return null;
                    }
                    break;
                case TAG_BYTES:
                    int bytesLength = readLength(view, "bytes length");
                    if (i == column) {
                        throw new IllegalStateException(
                                "expected LIST column in direct row but found BYTES");
                    }
                    skipBytes(view, bytesLength);
                    break;
                case TAG_LIST:
                    int elementCount = readLength(view, "list element count");
                    if (i != column) {
                        skipList(view, elementCount);
                        break;
                    }
                    List<T> decoded = new ArrayList<>(elementCount);
                    for (int j = 0; j < elementCount; j++) {
                        int elementLength = readLength(view, "list element length");
                        ByteBuffer element = sliceCurrent(view, elementLength);
                        inputStream.reset(element);
                        decoded.add(decoder.decode(inputStream));
                    }
                    return decoded;
                default:
                    throw new IllegalStateException("unknown direct row column tag: " + tag);
            }
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

    private static void skipBytes(ByteBuffer view, int bytesLength) {
        ensureRemaining(view, bytesLength);
        ((Buffer) view).position(view.position() + bytesLength);
    }

    private static void skipList(ByteBuffer view, int elementCount) {
        for (int i = 0; i < elementCount; i++) {
            int elementLength = readLength(view, "list element length");
            ensureRemaining(view, elementLength);
            ((Buffer) view).position(view.position() + elementLength);
        }
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

    private static void ensureRemaining(ByteBuffer view, int size) {
        if (size < 0 || view.remaining() < size) {
            throw new IllegalStateException("malformed encoded direct row payload");
        }
    }

    private static ByteBuffer sliceCurrent(ByteBuffer view, int length) {
        ensureRemaining(view, length);
        ByteBuffer slice = view.slice();
        ((Buffer) slice).limit(length);
        ((Buffer) view).position(view.position() + length);
        return slice;
    }

    /** Decoder function that maps one column payload stream to a value. */
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
