package io.cobble.structured;

import io.cobble.DirectIoUtils;
import io.cobble.NativeObject;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A native-backed structured scan cursor that reuses one direct io buffer for row-at-a-time
 * traversal.
 *
 * <p>Each returned row remains valid only until the cursor advances again or closes.
 */
public final class DirectScanCursor extends NativeObject implements Iterable<DirectScanRow> {
    private final DirectBufferPool pool;
    private final ByteBuffer ioBuffer;
    private final AtomicBoolean ioBufferReleased = new AtomicBoolean(false);
    private boolean iteratorCreated = false;

    DirectScanCursor(long nativeHandle, DirectBufferPool pool) {
        super(nativeHandle);
        this.pool = pool;
        this.ioBuffer = pool.acquire();
    }

    /**
     * Returns the next structured direct scan row, or {@code null} when the cursor is exhausted.
     *
     * <p>The returned row remains valid only until the next cursor advance or close.
     */
    public DirectScanRow nextRow() {
        if (nativeHandle == 0L) {
            throw new IllegalStateException("direct scan cursor is disposed");
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
        return DirectScanRow.decode(resultBuffer, Math.abs(encodedLength));
    }

    @Override
    public Iterator<DirectScanRow> iterator() {
        if (iteratorCreated) {
            throw new IllegalStateException("DirectScanCursor supports only a single traversal");
        }
        iteratorCreated = true;
        return new Iterator<DirectScanRow>() {
            private DirectScanRow nextRow;
            private boolean loaded;

            @Override
            public boolean hasNext() {
                if (!loaded) {
                    nextRow = DirectScanCursor.this.nextRow();
                    loaded = true;
                }
                return nextRow != null;
            }

            @Override
            public DirectScanRow next() {
                if (!hasNext()) {
                    throw new NoSuchElementException("No more direct scan rows.");
                }
                DirectScanRow result = nextRow;
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
                pool.release(ioBuffer);
            }
        }
    }

    @Override
    protected native void disposeInternal(long nativeHandle);

    private static native int nextRowDirectInternal(
            long nativeHandle, long ioAddress, int ioCapacity);
}
