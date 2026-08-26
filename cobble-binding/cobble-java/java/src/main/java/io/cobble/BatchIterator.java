package io.cobble;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Supplier;

/**
 * An iterator that lazily fetches elements in batches.
 *
 * <p>Used by {@link ScanCursor} and {@link io.cobble.structured.ScanCursor} to implement {@link
 * Iterable}.
 *
 * @param <T> the element type
 */
public final class BatchIterator<T> implements Iterator<T> {

    /** Abstraction over a single batch of elements. */
    public interface Batch<T> {
        int size();

        T get(int index);

        boolean hasMore();
    }

    private final Supplier<Batch<T>> fetchBatch;
    private Batch<T> currentBatch;
    private int index;

    public BatchIterator(Supplier<Batch<T>> fetchBatch) {
        this.fetchBatch = fetchBatch;
    }

    @Override
    public boolean hasNext() {
        if (currentBatch != null && index < currentBatch.size()) {
            return true;
        }
        if (currentBatch != null && !currentBatch.hasMore()) {
            return false;
        }
        currentBatch = fetchBatch.get();
        index = 0;
        return currentBatch.size() > 0;
    }

    @Override
    public T next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        return currentBatch.get(index++);
    }
}
