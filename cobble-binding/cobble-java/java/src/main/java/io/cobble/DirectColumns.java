package io.cobble;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/** Zero-copy direct-buffer-backed raw columns returned from direct JNI reads. */
public final class DirectColumns implements AutoCloseable {
    /** Encoded direct-read backend used by typed facades without a Java Db handle. */
    public interface Reader {
        /** Writes the columns payload over the key prefix; a negative length selects overflow. */
        int read(int bucket, ByteBuffer ioBuffer, int keyLength);

        /** Transfers a stable Java-owned overflow buffer, never a reusable native slot. */
        ByteBuffer takeOverflowBuffer();
    }

    /** One native multi-get call over packed bucket/key pairs. */
    public interface BatchReader {
        int read(ByteBuffer ioBuffer);

        /** Transfers a stable Java-owned overflow buffer for the duration of row decoding. */
        ByteBuffer takeOverflowBuffer();
    }

    /** Decodes a transient batch row into an owned value. */
    public interface OwnedRowDecoder<T> {
        T decode(int index, DirectColumns columns);
    }

    /** Executes one native batch read and decodes each row before reusing direct storage. */
    public static <T> List<T> readBatch(
            BatchReader reader, int[] buckets, byte[][] keys, OwnedRowDecoder<T> decoder) {
        if (buckets == null || keys == null || buckets.length != keys.length)
            throw new IllegalArgumentException("buckets and keys must have the same length");
        long needed = Integer.BYTES;
        for (byte[] key : keys) {
            if (key == null) throw new IllegalArgumentException("key must not be null");
            needed += 2L * Integer.BYTES + key.length;
        }
        if (needed > Integer.MAX_VALUE) throw new IllegalArgumentException("packed keys too large");
        DirectBufferPool pool = Db.directBufferPool();
        ByteBuffer pooled = pool.acquire();
        try {
            ByteBuffer io =
                    needed <= pooled.capacity() ? pooled : ByteBuffer.allocateDirect((int) needed);
            ((Buffer) io).clear();
            io.putInt(keys.length);
            for (int i = 0; i < keys.length; i++) {
                io.putInt(buckets[i]);
                io.putInt(keys[i].length);
                io.put(keys[i]);
            }
            int length = reader.read(io);
            if (length == 0 || length == Integer.MIN_VALUE)
                throw new IllegalStateException("invalid direct multi-get length");
            ByteBuffer payload =
                    length > 0
                            ? io
                            : DirectIoUtils.resolveEncodedBuffer(
                                    io, length, reader::takeOverflowBuffer);
            ByteBuffer view = payload.duplicate();
            ((Buffer) view).clear();
            ((Buffer) view).limit(Math.abs(length));
            ensureRemaining(view, Integer.BYTES);
            int count = view.getInt();
            if (count != keys.length)
                throw new IllegalStateException("direct multi-get count mismatch");
            List<T> rows = new ArrayList<T>(count);
            for (int i = 0; i < count; i++) {
                int rowLength = readLength(view, "row length");
                if (rowLength == 0) {
                    rows.add(null);
                    continue;
                }
                ByteBuffer row = sliceCurrent(view, rowLength);
                rows.add(decoder.decode(i, decode(row, rowLength, null)));
            }
            if (view.hasRemaining())
                throw new IllegalStateException("trailing direct multi-get bytes");
            return Collections.unmodifiableList(rows);
        } finally {
            pool.release(pooled);
        }
    }

    /** Reads with an independent pooled buffer lease, retained until the columns are closed. */
    public static DirectColumns read(
            Reader reader, int bucket, ByteBuffer keyBuffer, int keyLength) {
        if (keyBuffer == null || !keyBuffer.isDirect())
            throw new IllegalArgumentException("keyBuffer must be a direct ByteBuffer");
        if (keyLength < 0 || keyLength > keyBuffer.capacity())
            throw new IllegalArgumentException("keyLength out of range: " + keyLength);
        DirectBufferPool pool = Db.directBufferPool();
        ByteBuffer pooled = pool.acquire();
        boolean retained = false;
        try {
            ByteBuffer io =
                    keyLength <= pooled.capacity() ? pooled : ByteBuffer.allocateDirect(keyLength);
            DirectIoUtils.copyKey(io, keyBuffer, keyLength);
            int length = reader.read(bucket, io, keyLength);
            if (length == 0) return null;
            ByteBuffer result =
                    length > 0
                            ? io
                            : DirectIoUtils.resolveEncodedBuffer(
                                    io, length, reader::takeOverflowBuffer);
            DirectColumns columns =
                    decode(
                            result,
                            Math.abs(length),
                            result == pooled ? () -> pool.release(pooled) : null);
            retained = result == pooled;
            return columns;
        } finally {
            if (!retained) pool.release(pooled);
        }
    }

    /** Reads from an encoded heap key while copying it only once into the pooled IO buffer. */
    public static DirectColumns read(Reader reader, int bucket, byte[] key) {
        if (key == null) throw new IllegalArgumentException("key must not be null");
        DirectBufferPool pool = Db.directBufferPool();
        ByteBuffer pooled = pool.acquire();
        boolean retained = false;
        try {
            ByteBuffer io =
                    key.length <= pooled.capacity()
                            ? pooled
                            : ByteBuffer.allocateDirect(key.length);
            ((Buffer) io).clear();
            io.put(key);
            int length = reader.read(bucket, io, key.length);
            if (length == 0) return null;
            ByteBuffer result =
                    length > 0
                            ? io
                            : DirectIoUtils.resolveEncodedBuffer(
                                    io, length, reader::takeOverflowBuffer);
            DirectColumns columns =
                    decode(
                            result,
                            Math.abs(length),
                            result == pooled ? () -> pool.release(pooled) : null);
            retained = result == pooled;
            return columns;
        } finally {
            if (!retained) pool.release(pooled);
        }
    }

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
