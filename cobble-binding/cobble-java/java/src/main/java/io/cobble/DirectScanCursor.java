package io.cobble;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A native-backed raw scan cursor that reuses one direct io buffer for entry-at-a-time traversal.
 *
 * <p>Each returned entry remains valid only until the cursor advances again or closes.
 */
public final class DirectScanCursor extends NativeObject implements Iterable<DirectScanEntry> {
    private static final DirectBufferPool DEFAULT_POOL = DirectBufferPool.defaults();
    private final DirectBufferPool pool;
    private final ByteBuffer pooledBuffer;
    private ByteBuffer ioBuffer;
    private final AtomicBoolean ioBufferReleased = new AtomicBoolean(false);
    private boolean iteratorCreated = false;

    // Used by standalone snapshot scanners constructed through JNI.
    DirectScanCursor(long nativeHandle) {
        this(nativeHandle, DEFAULT_POOL);
    }

    DirectScanCursor(long nativeHandle, DirectBufferPool pool) {
        super(nativeHandle);
        this.pool = pool;
        this.pooledBuffer = pool.acquire();
        this.ioBuffer = pooledBuffer;
    }

    public DirectScanEntry nextEntry() {
        if (nativeHandle == 0L) {
            throw new IllegalStateException("direct scan cursor is disposed");
        }
        int encodedLength =
                nextEntryDirectInternal(
                        nativeHandle, DirectIoUtils.directAddress(ioBuffer), ioBuffer.capacity());
        if (encodedLength == 0) {
            return null;
        }
        ByteBuffer resultBuffer = ioBuffer;
        if (encodedLength < 0) {
            resultBuffer = takeOwnedOverflowNative();
            if (resultBuffer == null) {
                throw new IllegalStateException("missing owned overflow direct buffer from JNI");
            }
            ioBuffer = resultBuffer;
            releasePooledBuffer();
        }
        return DirectScanEntry.decode(resultBuffer, Math.abs(encodedLength));
    }

    @Override
    public Iterator<DirectScanEntry> iterator() {
        if (iteratorCreated) {
            throw new IllegalStateException("DirectScanCursor supports only a single traversal");
        }
        iteratorCreated = true;
        return new Iterator<DirectScanEntry>() {
            private DirectScanEntry nextEntry;
            private boolean loaded;

            @Override
            public boolean hasNext() {
                if (!loaded) {
                    nextEntry = DirectScanCursor.this.nextEntry();
                    loaded = true;
                }
                return nextEntry != null;
            }

            @Override
            public DirectScanEntry next() {
                if (!hasNext()) {
                    throw new NoSuchElementException("No more direct scan entries.");
                }
                DirectScanEntry result = nextEntry;
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
            releasePooledBuffer();
            ioBuffer = null;
        }
    }

    private void releasePooledBuffer() {
        if (ioBufferReleased.compareAndSet(false, true)) pool.release(pooledBuffer);
    }

    @Override
    protected native void disposeInternal(long nativeHandle);

    private static native int nextEntryDirectInternal(
            long nativeHandle, long ioAddress, int ioCapacity);

    private static native ByteBuffer takeOwnedOverflowNative();
}
