package io.cobble.structured;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Direct-buffer-backed row returned from structured direct JNI reads.
 *
 * <p>Each column is represented as:
 *
 * <ul>
 *   <li>null
 *   <li>one direct {@link ByteBuffer} for bytes columns
 *   <li>a list of direct {@link ByteBuffer} for list columns
 * </ul>
 */
public final class DirectRow implements AutoCloseable {
    private static final byte TAG_NULL = 0;
    private static final byte TAG_BYTES = 1;
    private static final byte TAG_LIST = 2;

    private final ColumnValue[] columns;
    private final Runnable onClose;
    private final AtomicBoolean closed;

    private DirectRow(ColumnValue[] columns, Runnable onClose) {
        this.columns = columns;
        this.onClose = onClose;
        this.closed = new AtomicBoolean(false);
    }

    static DirectRow decode(ByteBuffer encoded, int encodedLength, Runnable onClose) {
        ByteBuffer view = encoded.duplicate();
        ((Buffer) view).clear();
        if (encodedLength < 4 || encodedLength > view.capacity()) {
            throw new IllegalStateException("invalid encoded direct row length: " + encodedLength);
        }
        ((Buffer) view).limit(encodedLength);

        int columnCount = view.getInt();
        if (columnCount < 0) {
            throw new IllegalStateException("invalid column count: " + columnCount);
        }
        ColumnValue[] values = new ColumnValue[columnCount];
        for (int i = 0; i < columnCount; i++) {
            ensureRemaining(view, 1);
            byte tag = view.get();
            if (tag == TAG_NULL) {
                values[i] = null;
                continue;
            }
            if (tag == TAG_BYTES) {
                int len = readLength(view, "bytes length");
                values[i] = ColumnValue.bytes(sliceCurrent(view, len));
                continue;
            }
            if (tag == TAG_LIST) {
                int count = readLength(view, "list count");
                List<ByteBuffer> list = new ArrayList<>(count);
                for (int j = 0; j < count; j++) {
                    int len = readLength(view, "list element length");
                    list.add(sliceCurrent(view, len));
                }
                values[i] = ColumnValue.list(Collections.unmodifiableList(list));
                continue;
            }
            throw new IllegalStateException("unknown direct row column tag: " + tag);
        }
        return new DirectRow(values, onClose == null ? () -> {} : onClose);
    }

    public int size() {
        return columns.length;
    }

    public ByteBuffer getBytes(int column) {
        ColumnValue value = getColumnValue(column);
        if (value == null) {
            return null;
        }
        if (!value.isBytes()) {
            throw new IllegalStateException("column " + column + " is LIST");
        }
        return value.bytes();
    }

    public List<ByteBuffer> getList(int column) {
        ColumnValue value = getColumnValue(column);
        if (value == null) {
            return null;
        }
        if (value.isBytes()) {
            throw new IllegalStateException("column " + column + " is BYTES");
        }
        return value.list();
    }

    public boolean isNull(int column) {
        return getColumnValue(column) == null;
    }

    public int getListElementCount(int column) {
        ColumnValue value = getColumnValue(column);
        if (value == null) {
            throw new IllegalStateException("column " + column + " is NULL");
        }
        if (value.isBytes()) {
            throw new IllegalStateException("column " + column + " is BYTES");
        }
        return value.listSize();
    }

    public ByteBuffer getListElement(int column, int index) {
        ColumnValue value = getColumnValue(column);
        if (value == null) {
            throw new IllegalStateException("column " + column + " is NULL");
        }
        if (value.isBytes()) {
            throw new IllegalStateException("column " + column + " is BYTES");
        }
        return value.listElement(index);
    }

    private ColumnValue getColumnValue(int column) {
        if (column < 0 || column >= columns.length) {
            throw new IllegalArgumentException("column out of range: " + column);
        }
        return columns[column];
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            onClose.run();
        }
    }

    private static void ensureRemaining(ByteBuffer view, int size) {
        if (size < 0 || view.remaining() < size) {
            throw new IllegalStateException("malformed direct row payload");
        }
    }

    private static int readLength(ByteBuffer view, String fieldName) {
        ensureRemaining(view, 4);
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

    static final class ColumnValue {
        private final ByteBuffer bytes;
        private final List<ByteBuffer> list;

        private ColumnValue(ByteBuffer bytes, List<ByteBuffer> list) {
            this.bytes = bytes;
            this.list = list;
        }

        static ColumnValue bytes(ByteBuffer bytes) {
            return new ColumnValue(bytes, null);
        }

        static ColumnValue list(List<ByteBuffer> list) {
            return new ColumnValue(null, list);
        }

        boolean isBytes() {
            return bytes != null;
        }

        ByteBuffer bytes() {
            return bytes == null ? null : bytes.duplicate();
        }

        List<ByteBuffer> list() {
            if (list == null) {
                return null;
            }
            List<ByteBuffer> copied = new ArrayList<>(list.size());
            for (ByteBuffer element : list) {
                copied.add(element.duplicate());
            }
            return copied;
        }

        int listSize() {
            return list == null ? 0 : list.size();
        }

        ByteBuffer listElement(int index) {
            if (list == null) {
                throw new IllegalStateException("column is not LIST");
            }
            if (index < 0 || index >= list.size()) {
                throw new IndexOutOfBoundsException("list index out of range: " + index);
            }
            return list.get(index).duplicate();
        }
    }
}
