package io.cobble.structured;

import io.cobble.BatchIterator;
import io.cobble.DirectIoUtils;
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
    private Runnable currentBatchCloser = () -> {};

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
                            nativeHandle, DirectIoUtils.directAddress(pooled), pooled.capacity());
            if (encodedLength == 0) {
                pool.release(pooled);
                pooledReleased = true;
                DirectScanBatch batch = DirectScanBatch.empty();
                currentBatchCloser = batch::close;
                return batch;
            }
            ByteBuffer resultBuffer =
                    DirectIoUtils.resolveEncodedBuffer(
                            pooled, encodedLength, Db.getLastDirectOverflowBuffer());
            int decodedLength = Math.abs(encodedLength);
            if (resultBuffer == pooled) {
                AtomicBoolean released = new AtomicBoolean(false);
                DirectScanBatch batch =
                        DirectScanBatch.decode(
                                resultBuffer,
                                decodedLength,
                                () -> {
                                    if (released.compareAndSet(false, true)) {
                                        pool.release(pooled);
                                    }
                                });
                currentBatchCloser = batch::close;
                return batch;
            }
            pool.release(pooled);
            pooledReleased = true;
            DirectScanBatch batch = DirectScanBatch.decode(resultBuffer, decodedLength, null);
            currentBatchCloser = batch::close;
            return batch;
        } catch (RuntimeException e) {
            if (!pooledReleased) {
                pool.release(pooled);
            }
            throw e;
        }
    }

    public DirectEncodedScanBatch nextEncodedBatch() {
        closeCurrentBatch();
        if (nativeHandle == 0L) {
            throw new IllegalStateException("direct scan cursor is disposed");
        }
        ByteBuffer pooled = pool.acquire();
        boolean pooledReleased = false;
        try {
            int encodedLength =
                    nextBatchDirectInternal(
                            nativeHandle, DirectIoUtils.directAddress(pooled), pooled.capacity());
            if (encodedLength == 0) {
                pool.release(pooled);
                pooledReleased = true;
                DirectEncodedScanBatch batch = DirectEncodedScanBatch.empty();
                currentBatchCloser = batch::close;
                return batch;
            }
            ByteBuffer resultBuffer =
                    DirectIoUtils.resolveEncodedBuffer(
                            pooled, encodedLength, Db.getLastDirectOverflowBuffer());
            int decodedLength = Math.abs(encodedLength);
            if (resultBuffer == pooled) {
                AtomicBoolean released = new AtomicBoolean(false);
                DirectEncodedScanBatch batch =
                        DirectEncodedScanBatch.decode(
                                resultBuffer,
                                decodedLength,
                                () -> {
                                    if (released.compareAndSet(false, true)) {
                                        pool.release(pooled);
                                    }
                                });
                currentBatchCloser = batch::close;
                return batch;
            }
            pool.release(pooled);
            pooledReleased = true;
            DirectEncodedScanBatch batch =
                    DirectEncodedScanBatch.decode(resultBuffer, decodedLength, null);
            currentBatchCloser = batch::close;
            return batch;
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
        Runnable closer = currentBatchCloser;
        currentBatchCloser = () -> {};
        closer.run();
    }

    @Override
    protected native void disposeInternal(long nativeHandle);

    private static native int nextBatchDirectInternal(
            long nativeHandle, long ioAddress, int ioCapacity);
}
