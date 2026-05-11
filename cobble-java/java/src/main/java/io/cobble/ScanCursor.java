package io.cobble;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Native-backed scan cursor for one bucket/range.
 *
 * <p>Supports Java enhanced for-each loops via {@link Iterable}. Each element is an {@link Entry}
 * containing the key and its column values.
 *
 * <p>A cursor is single-traversal: {@link #iterator()} may only be called once.
 */
public final class ScanCursor extends NativeObject implements Iterable<ScanCursor.Entry> {
    private static final DirectBufferPool DIRECT_BUFFER_POOL = DirectBufferPool.defaults();

    /** A single row from a scan: key + column values. */
    public static final class Entry {
        public final byte[] key;
        public final byte[][] columns;

        Entry(byte[] key, byte[][] columns) {
            this.key = key;
            this.columns = columns;
        }
    }

    private final ByteBuffer ioBuffer;
    private final AtomicBoolean ioBufferReleased = new AtomicBoolean(false);
    private boolean iteratorCreated = false;

    ScanCursor(long nativeHandle) {
        super(nativeHandle);
        this.ioBuffer = DIRECT_BUFFER_POOL.acquire();
    }

    /** Returns the next scan entry, or {@code null} when the cursor is exhausted. */
    public Entry nextEntry() {
        if (nativeHandle == 0L) {
            throw new IllegalStateException("scan cursor is disposed");
        }
        int encodedLength =
                nextEntryDirectInternal(
                        nativeHandle, DirectIoUtils.directAddress(ioBuffer), ioBuffer.capacity());
        if (encodedLength == 0) {
            return null;
        }
        ByteBuffer resultBuffer =
                DirectIoUtils.resolveEncodedBuffer(
                        ioBuffer, encodedLength, Db::getLastDirectOverflowBuffer);
        DirectScanEntry entry = DirectScanEntry.decode(resultBuffer, Math.abs(encodedLength));
        byte[] key = copyBytes(entry.getKey());
        byte[][] columns = new byte[entry.size()][];
        for (int i = 0; i < columns.length; i++) {
            if (!entry.isNull(i)) {
                try {
                    columns[i] = entry.decodeColumn(i, ScanCursor::readInputStreamBytes);
                } catch (IOException e) {
                    throw new IllegalStateException("failed to decode scan column " + i, e);
                }
            }
        }
        return new Entry(key, columns);
    }

    @Override
    public Iterator<Entry> iterator() {
        if (iteratorCreated) {
            throw new IllegalStateException("ScanCursor supports only a single traversal");
        }
        iteratorCreated = true;
        return new Iterator<Entry>() {
            private Entry nextEntry;
            private boolean loaded;

            @Override
            public boolean hasNext() {
                if (!loaded) {
                    nextEntry = ScanCursor.this.nextEntry();
                    loaded = true;
                }
                return nextEntry != null;
            }

            @Override
            public Entry next() {
                if (!hasNext()) {
                    throw new NoSuchElementException("No more scan entries.");
                }
                Entry result = nextEntry;
                nextEntry = null;
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
        ((Buffer) copy).clear();
        byte[] bytes = new byte[copy.remaining()];
        copy.get(bytes);
        return bytes;
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

    private static native int nextEntryDirectInternal(
            long nativeHandle, long ioAddress, int ioCapacity);
}
