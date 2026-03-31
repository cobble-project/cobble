package io.cobble.structured;

import io.cobble.BatchIterator;
import io.cobble.NativeObject;

import java.util.Iterator;

/**
 * A native-backed scan cursor that yields typed rows from a structured database.
 *
 * <p>Supports Java enhanced for-each loops via {@link Iterable}. Each element is a {@link Row}.
 *
 * <p>A cursor is single-traversal: {@link #iterator()} may only be called once.
 */
public final class ScanCursor extends NativeObject implements Iterable<Row> {

    private boolean iteratorCreated = false;

    ScanCursor(long nativeHandle) {
        super(nativeHandle);
    }

    /**
     * Fetches the next batch of typed rows from the native iterator.
     *
     * @return a batch of rows; empty batch when exhausted
     */
    public ScanBatch nextBatch() {
        if (nativeHandle == 0L) {
            throw new IllegalStateException("scan cursor is disposed");
        }
        ScanBatch batch = nextBatchInternal(nativeHandle);
        if (batch == null) {
            return ScanBatch.empty();
        }
        return batch;
    }

    @Override
    public Iterator<Row> iterator() {
        if (iteratorCreated) {
            throw new IllegalStateException("ScanCursor supports only a single traversal");
        }
        iteratorCreated = true;
        return new BatchIterator<>(
                () -> {
                    ScanBatch b = nextBatch();
                    return new BatchIterator.Batch<Row>() {
                        @Override
                        public int size() {
                            return b.size();
                        }

                        @Override
                        public Row get(int i) {
                            return b.getRow(i);
                        }

                        @Override
                        public boolean hasMore() {
                            return b.hasMore;
                        }
                    };
                });
    }

    @Override
    protected native void disposeInternal(long nativeHandle);

    private static native ScanBatch nextBatchInternal(long nativeHandle);
}
