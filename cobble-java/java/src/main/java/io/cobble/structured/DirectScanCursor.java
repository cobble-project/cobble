package io.cobble.structured;

import io.cobble.BatchIterator;
import io.cobble.NativeObject;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A native-backed structured scan cursor that delivers direct-buffer-backed scan rows.
 *
 * <p>Each batch remains valid until the cursor advances to the next batch or closes.
 */
public final class DirectScanCursor extends NativeObject implements Iterable<DirectScanRow> {
    private final DirectBufferPool pool;
    private boolean iteratorCreated = false;
    private DirectScanBatch currentBatch = DirectScanBatch.empty();

    DirectScanCursor(long nativeHandle, DirectBufferPool pool) {
        super(nativeHandle);
        this.pool = pool;
    }

    public DirectScanBatch nextBatch() {
        closeCurrentBatch();
        if (nativeHandle == 0L) {
            throw new IllegalStateException("direct scan cursor is disposed");
        }
        ByteBuffer pooled = pool.acquire();
        boolean pooledReleased = false;
        try {
            int encodedLength =
                    nextBatchDirectInternal(
                            nativeHandle, Db.directAddress(pooled), pooled.capacity());
            if (encodedLength == 0) {
                pool.release(pooled);
                pooledReleased = true;
                currentBatch = DirectScanBatch.empty();
                return currentBatch;
            }
            ByteBuffer resultBuffer = Db.resolveEncodedBuffer(pooled, encodedLength);
            int decodedLength = Math.abs(encodedLength);
            if (resultBuffer == pooled) {
                AtomicBoolean released = new AtomicBoolean(false);
                currentBatch =
                        DirectScanBatch.decode(
                                resultBuffer,
                                decodedLength,
                                () -> {
                                    if (released.compareAndSet(false, true)) {
                                        pool.release(pooled);
                                    }
                                });
                return currentBatch;
            }
            pool.release(pooled);
            pooledReleased = true;
            currentBatch = DirectScanBatch.decode(resultBuffer, decodedLength, null);
            return currentBatch;
        } catch (RuntimeException e) {
            if (!pooledReleased) {
                pool.release(pooled);
            }
            throw e;
        }
    }

    @Override
    public Iterator<DirectScanRow> iterator() {
        if (iteratorCreated) {
            throw new IllegalStateException("DirectScanCursor supports only a single traversal");
        }
        iteratorCreated = true;
        return new BatchIterator<>(
                () -> {
                    DirectScanBatch batch = nextBatch();
                    return new BatchIterator.Batch<DirectScanRow>() {
                        @Override
                        public int size() {
                            return batch.size();
                        }

                        @Override
                        public DirectScanRow get(int index) {
                            return batch.getRow(index);
                        }

                        @Override
                        public boolean hasMore() {
                            return batch.hasMore;
                        }
                    };
                });
    }

    @Override
    public void close() {
        closeCurrentBatch();
        super.close();
    }

    private void closeCurrentBatch() {
        DirectScanBatch batch = currentBatch;
        currentBatch = DirectScanBatch.empty();
        batch.close();
    }

    @Override
    protected native void disposeInternal(long nativeHandle);

    private static native int nextBatchDirectInternal(
            long nativeHandle, long ioAddress, int ioCapacity);
}
