package io.cobble.table;

import io.cobble.DirectColumns;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A typed row whose binary values may borrow native direct-buffer storage.
 *
 * <p>The row is valid until {@link #close()}. Copy any binary values that must outlive this view.
 */
public final class DirectTableRow implements AutoCloseable {
    private final DirectColumns columns;
    private final List<Value> values;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    DirectTableRow(DirectColumns columns, List<Value> values) {
        this.columns = columns;
        this.values = Collections.unmodifiableList(values);
    }

    public List<Value> values() {
        if (closed.get()) throw new IllegalStateException("direct table row is closed");
        return values;
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) columns.close();
    }
}
