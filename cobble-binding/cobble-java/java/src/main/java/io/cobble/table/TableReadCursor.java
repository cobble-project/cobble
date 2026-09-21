package io.cobble.table;

/** Single-pass cursor over decoded logical rows. */
public interface TableReadCursor<R> extends AutoCloseable {
    /** Returns the next entry, or {@code null} once exhausted. */
    TableReadEntry<R> next() throws Exception;

    @Override
    void close();
}
