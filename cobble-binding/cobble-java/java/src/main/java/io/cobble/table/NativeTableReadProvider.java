package io.cobble.table;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** Adapts a fixed native {@link TableReader} to the neutral table read SPI. */
public final class NativeTableReadProvider implements TableReadProvider<List<Value>, TableKey> {
    private static final TableReadCapabilities CAPABILITIES =
            new TableReadCapabilities(false, true, false);

    private final TableReader reader;
    private boolean opened;

    /** The supplied reader must be opened for one snapshot, not as a current-refreshing reader. */
    public NativeTableReadProvider(TableReader reader) {
        this.reader = Objects.requireNonNull(reader, "reader");
    }

    @Override
    public TableReadCapabilities capabilities() {
        return CAPABILITIES;
    }

    @Override
    public synchronized TableReadSession<List<Value>, TableKey> open() {
        if (opened) {
            throw new IllegalStateException("native table read provider already has an open view");
        }
        opened = true;
        return new View();
    }

    @Override
    public synchronized void close() {
        reader.close();
    }

    /** Builds a typed primary key for this provider's fixed reader schema. */
    public TableKeyBuilder keyBuilder() {
        return reader.keyBuilder();
    }

    private final class View implements TableReadSession<List<Value>, TableKey> {
        private boolean closed;

        @Override
        public TableReadCapabilities capabilities() {
            return CAPABILITIES;
        }

        @Override
        public TableReadSchema schema() {
            return new TableReadSchema(reader.schema().fields());
        }

        @Override
        public TableReadCursor<List<Value>> scan(TableReadRange range, TableReadPosition position) {
            throw new UnsupportedOperationException("native table reader provider does not scan");
        }

        @Override
        public Collection<TableReadEntry<List<Value>>> lookup(TableKey key) {
            ensureOpen();
            List<Value> row = reader.get(Objects.requireNonNull(key, "key"));
            return row == null
                    ? Collections.<TableReadEntry<List<Value>>>emptyList()
                    : Collections.singletonList(
                            new TableReadEntry<List<Value>>(
                                    new TableReadPosition(key.bucket(), null, 1), row));
        }

        @Override
        public synchronized void close() {
            closed = true;
        }

        private void ensureOpen() {
            if (closed) throw new IllegalStateException("native table read view is closed");
        }
    }
}
