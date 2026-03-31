package io.cobble;

import java.util.Iterator;

/**
 * Native-backed scan cursor for one bucket/range.
 *
 * <p>Supports Java enhanced for-each loops via {@link Iterable}. Each element is a {@link Entry}
 * containing the key and its column values.
 *
 * <p>A cursor is single-traversal: {@link #iterator()} may only be called once.
 */
public final class ScanCursor extends NativeObject implements Iterable<ScanCursor.Entry> {

    /** A single row from a scan: key + column values. */
    public static final class Entry {
        public final byte[] key;
        public final byte[][] columns;

        Entry(byte[] key, byte[][] columns) {
            this.key = key;
            this.columns = columns;
        }
    }

    private boolean iteratorCreated = false;

    ScanCursor(long nativeHandle) {
        super(nativeHandle);
    }

    /**
     * Fetch next batch of rows from native side.
     *
     * @return batched keys/values for configured columns
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
    public Iterator<Entry> iterator() {
        if (iteratorCreated) {
            throw new IllegalStateException("ScanCursor supports only a single traversal");
        }
        iteratorCreated = true;
        return new BatchIterator<>(
                () -> {
                    ScanBatch b = nextBatch();
                    return new BatchIterator.Batch<Entry>() {
                        @Override
                        public int size() {
                            return b.keys.length;
                        }

                        @Override
                        public Entry get(int i) {
                            return new Entry(b.keys[i], b.values[i]);
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
