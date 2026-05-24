package io.cobble.structured;

import io.cobble.DirectIoUtils;
import io.cobble.NativeObject;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A native-backed scan cursor that yields typed rows from a structured database.
 *
 * <p>Supports Java enhanced for-each loops via {@link Iterable}. Each element is a {@link Row}.
 *
 * <p>A cursor is single-traversal: {@link #iterator()} may only be called once.
 */
public final class ScanCursor extends NativeObject implements Iterable<Row> {
    private static final DirectBufferPool DIRECT_BUFFER_POOL = DirectBufferPool.defaults();

    private final ByteBuffer ioBuffer;
    private final AtomicBoolean ioBufferReleased = new AtomicBoolean(false);
    private boolean iteratorCreated = false;

    ScanCursor(long nativeHandle) {
        super(nativeHandle);
        this.ioBuffer = DIRECT_BUFFER_POOL.acquire();
    }

    /** Returns the next structured row, or {@code null} when the cursor is exhausted. */
    public Row nextRow() {
        if (nativeHandle == 0L) {
            throw new IllegalStateException("scan cursor is disposed");
        }
        int encodedLength =
                nextRowDirectInternal(
                        nativeHandle, DirectIoUtils.directAddress(ioBuffer), ioBuffer.capacity());
        if (encodedLength == 0) {
            return null;
        }
        ByteBuffer resultBuffer =
                DirectIoUtils.resolveEncodedBuffer(
                        ioBuffer, encodedLength, Db::getLastDirectOverflowBuffer);
        DirectScanRow row = DirectScanRow.decode(resultBuffer, Math.abs(encodedLength));
        ColumnValue[] values = new ColumnValue[row.getColumnCount()];
        for (int i = 0; i < values.length; i++) {
            switch (row.columnTag(i)) {
                case 0:
                    values[i] = null;
                    break;
                case 1:
                    values[i] = ColumnValue.ofBytes(decodeBytes(row, i));
                    break;
                case 2:
                    values[i] = ColumnValue.ofList(decodeList(row, i));
                    break;
                default:
                    throw new IllegalStateException("unknown structured scan column tag");
            }
        }
        return new Row(row.getBucket(), copyBytes(row.getKey()), values);
    }

    @Override
    public Iterator<Row> iterator() {
        if (iteratorCreated) {
            throw new IllegalStateException("ScanCursor supports only a single traversal");
        }
        iteratorCreated = true;
        return new Iterator<Row>() {
            private Row nextRow;
            private boolean loaded;

            @Override
            public boolean hasNext() {
                if (!loaded) {
                    nextRow = ScanCursor.this.nextRow();
                    loaded = true;
                }
                return nextRow != null;
            }

            @Override
            public Row next() {
                if (!hasNext()) {
                    throw new NoSuchElementException("No more scan rows.");
                }
                Row result = nextRow;
                nextRow = null;
                loaded = false;
                return result;
            }
        };
    }

    @Override
    public void close() {
        try {
            super.close();
        } finally {
            if (ioBufferReleased.compareAndSet(false, true)) {
                DIRECT_BUFFER_POOL.release(ioBuffer);
            }
        }
    }

    @Override
    protected native void disposeInternal(long nativeHandle);

    private static byte[] copyBytes(ByteBuffer buffer) {
        ByteBuffer copy = buffer.duplicate();
        byte[] bytes = new byte[copy.remaining()];
        copy.get(bytes);
        return bytes;
    }

    private static byte[] decodeBytes(DirectScanRow row, int column) {
        try {
            return row.decodeBytesColumn(column, ScanCursor::readInputStreamBytes);
        } catch (IOException e) {
            throw new IllegalStateException(
                    "failed to decode structured scan bytes column " + column, e);
        }
    }

    private static byte[][] decodeList(DirectScanRow row, int column) {
        try {
            return row.decodeListColumn(column, ScanCursor::readInputStreamBytes)
                    .toArray(new byte[0][]);
        } catch (IOException e) {
            throw new IllegalStateException(
                    "failed to decode structured scan list column " + column, e);
        }
    }

    private static byte[] readInputStreamBytes(InputStream input) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] buffer = new byte[256];
        int read;
        while ((read = input.read(buffer)) != -1) {
            out.write(buffer, 0, read);
        }
        return out.toByteArray();
    }

    private static native int nextRowDirectInternal(
            long nativeHandle, long ioAddress, int ioCapacity);
}
