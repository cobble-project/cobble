package io.cobble.table;

import io.cobble.Config;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** Adapts one fixed native {@link TableScanSplit} to the neutral row-by-row scan SPI. */
public final class NativeTableScanReadProvider implements TableReadProvider<List<Value>, Void> {
    private static final TableReadCapabilities CAPABILITIES =
            new TableReadCapabilities(true, false, false);

    private final Config config;
    private final TableScanSplit split;
    private boolean opened;

    public NativeTableScanReadProvider(Config config, TableScanSplit split) {
        this.config = Objects.requireNonNull(config, "config");
        this.split = Objects.requireNonNull(split, "split");
    }

    @Override
    public TableReadCapabilities capabilities() {
        return CAPABILITIES;
    }

    @Override
    public synchronized TableReadSession<List<Value>, Void> open() {
        if (opened)
            throw new IllegalStateException(
                    "native table scan provider already has an open session");
        opened = true;
        return new Session();
    }

    @Override
    public void close() {}

    private final class Session implements TableReadSession<List<Value>, Void> {
        private boolean closed;
        private final List<TableDirectScanCursor<List<Value>>> cursors =
                new ArrayList<TableDirectScanCursor<List<Value>>>();

        @Override
        public TableReadSchema schema() {
            return new TableReadSchema(split.schema().fields());
        }

        @Override
        public TableReadCapabilities capabilities() {
            return CAPABILITIES;
        }

        @Override
        public TableReadCursor<List<Value>> scan(TableReadRange range, TableReadPosition position) {
            ensureOpen();
            if (position != null) {
                throw new UnsupportedOperationException(
                        "native table scan split cannot seek by position");
            }
            if (range.firstBucket() != 0 || range.lastBucket() != Integer.MAX_VALUE) {
                throw new UnsupportedOperationException(
                        "native table scan range is fixed by its split");
            }
            List<String> names = new ArrayList<String>();
            for (DataField field : split.schema().fields()) names.add(field.name());
            Table.Compiled compiled = Table.Compiled.from(split.schema(), split.totalBuckets());
            TableDirectScanCursor<List<Value>> cursor =
                    TableDirectScanCursor.fixed(
                            split.openDirectScanner(config, names, 0),
                            (entry, ignoredKey) ->
                                    Collections.singletonList(
                                            Table.decodeDirectScannedRowOwned(compiled, entry)));
            cursors.add(cursor);
            return cursor;
        }

        @Override
        public Collection<TableReadEntry<List<Value>>> lookup(Void ignored) {
            throw new UnsupportedOperationException(
                    "native table scan split does not support lookup");
        }

        @Override
        public void close() {
            for (TableDirectScanCursor<List<Value>> cursor :
                    new ArrayList<TableDirectScanCursor<List<Value>>>(cursors)) cursor.close();
            cursors.clear();
            closed = true;
        }

        private void ensureOpen() {
            if (closed) throw new IllegalStateException("native table scan session is closed");
        }
    }
}
