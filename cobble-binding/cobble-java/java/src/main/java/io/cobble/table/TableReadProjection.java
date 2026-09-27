package io.cobble.table;

import java.util.ArrayList;
import java.util.List;

/** Java-side typed row projection for format plugins without native pushdown. */
final class TableReadProjection {
    private TableReadProjection() {}

    static TableReadCursor<List<Value>> cursor(
            TableReadCursor<List<Value>> delegate, int[] projection) {
        return new Cursor(delegate, projection.clone());
    }

    private static final class Cursor implements TableReadCursor<List<Value>> {
        private final TableReadCursor<List<Value>> delegate;
        private final int[] projection;

        private Cursor(TableReadCursor<List<Value>> delegate, int[] projection) {
            this.delegate = delegate;
            this.projection = projection;
        }

        @Override
        public TableReadEntry<List<Value>> next() throws Exception {
            TableReadEntry<List<Value>> entry = delegate.next();
            return entry == null ? null : project(entry, projection);
        }

        @Override
        public void close() {
            delegate.close();
        }
    }

    private static TableReadEntry<List<Value>> project(
            TableReadEntry<List<Value>> entry, int[] projection) {
        ArrayList<Value> values = new ArrayList<Value>(projection.length);
        for (int index : projection) values.add(entry.value().get(index));
        return new TableReadEntry<List<Value>>(
                entry.position(), values, entry.physicalBytes(), entry.countsPhysicalEntry());
    }
}
