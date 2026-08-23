package io.cobble.table;

import io.cobble.DirectScanCursor;
import io.cobble.DirectScanEntry;
import io.cobble.NativeObject;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/** Native-backed typed scan cursor. A cursor supports one traversal and must be closed. */
public final class TableScanCursor implements AutoCloseable, Iterable<List<Value>> {
    interface RowDecoder {
        List<Value> decode(DirectScanEntry entry);
    }

    private final NativeObject owner;
    private final DirectScanCursor inner;
    private final RowDecoder decoder;
    private boolean iteratorCreated;

    TableScanCursor(NativeObject owner, DirectScanCursor inner, RowDecoder decoder) {
        this.owner = owner;
        this.inner = inner;
        this.decoder = decoder;
    }

    /** Returns the next owned typed row, or {@code null} when exhausted. */
    public List<Value> nextRow() {
        if (owner.isDisposed()) throw new IllegalStateException("database is closed");
        DirectScanEntry entry = inner.nextEntry();
        return entry == null ? null : decoder.decode(entry);
    }

    @Override
    public Iterator<List<Value>> iterator() {
        if (iteratorCreated)
            throw new IllegalStateException("TableScanCursor supports only one traversal");
        iteratorCreated = true;
        return new Iterator<List<Value>>() {
            private List<Value> next;
            private boolean loaded;

            @Override
            public boolean hasNext() {
                if (!loaded) {
                    next = nextRow();
                    loaded = true;
                }
                return next != null;
            }

            @Override
            public List<Value> next() {
                if (!hasNext()) throw new NoSuchElementException("No more table rows.");
                List<Value> value = next;
                next = null;
                loaded = false;
                return value;
            }
        };
    }

    @Override
    public void close() {
        inner.close();
    }
}
