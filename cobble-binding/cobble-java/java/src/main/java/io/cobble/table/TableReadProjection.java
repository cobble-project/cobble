package io.cobble.table;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/** Java-side typed row projection shared by format providers until they add native pushdown. */
final class TableReadProjection {
    private TableReadProjection() {}

    static TableReadProvider<List<Value>, ?> apply(
            TableReadProvider<List<Value>, ?> provider, TableReadSchema schema, int[] projection) {
        return new Provider(provider, schema, projection.clone());
    }

    static TableReadCursor<List<Value>> cursor(
            TableReadCursor<List<Value>> delegate, int[] projection) {
        return new Cursor(delegate, projection.clone());
    }

    private static final class Provider implements TableReadProvider<List<Value>, Object> {
        private final TableReadProvider<List<Value>, ?> delegate;
        private final TableReadSchema schema;
        private final int[] projection;

        private Provider(
                TableReadProvider<List<Value>, ?> delegate,
                TableReadSchema schema,
                int[] projection) {
            this.delegate = delegate;
            this.schema = schema;
            this.projection = projection;
        }

        @Override
        public TableReadCapabilities capabilities() {
            return delegate.capabilities();
        }

        @Override
        public TableReadSession<List<Value>, Object> open() throws Exception {
            return new Session(delegate.open(), schema, projection);
        }

        @Override
        public void close() {
            delegate.close();
        }
    }

    private static final class Session implements TableReadSession<List<Value>, Object> {
        private final TableReadSession<List<Value>, ?> delegate;
        private final TableReadSchema schema;
        private final int[] projection;

        private Session(
                TableReadSession<List<Value>, ?> delegate,
                TableReadSchema schema,
                int[] projection) {
            this.delegate = delegate;
            this.schema = schema;
            this.projection = projection;
        }

        @Override
        public TableReadSchema schema() {
            return schema;
        }

        @Override
        public TableReadCapabilities capabilities() {
            return delegate.capabilities();
        }

        @Override
        public TableReadCursor<List<Value>> scan(TableReadRange range, TableReadPosition position)
                throws Exception {
            return new Cursor(delegate.scan(range, position), projection);
        }

        @Override
        @SuppressWarnings("unchecked")
        public Collection<TableReadEntry<List<Value>>> lookup(Object key) throws Exception {
            Collection<TableReadEntry<List<Value>>> entries =
                    ((TableReadSession<List<Value>, Object>) delegate).lookup(key);
            return projectEntries(entries, projection);
        }

        @Override
        public void close() {
            delegate.close();
        }
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

    private static Collection<TableReadEntry<List<Value>>> projectEntries(
            Collection<TableReadEntry<List<Value>>> entries, int[] projection) {
        if (entries.isEmpty()) return Collections.emptyList();
        ArrayList<TableReadEntry<List<Value>>> values =
                new ArrayList<TableReadEntry<List<Value>>>(entries.size());
        for (TableReadEntry<List<Value>> entry : entries) values.add(project(entry, projection));
        return values;
    }

    private static TableReadEntry<List<Value>> project(
            TableReadEntry<List<Value>> entry, int[] projection) {
        ArrayList<Value> values = new ArrayList<Value>(projection.length);
        for (int index : projection) values.add(entry.value().get(index));
        return new TableReadEntry<List<Value>>(
                entry.position(), values, entry.physicalBytes(), entry.countsPhysicalEntry());
    }
}
