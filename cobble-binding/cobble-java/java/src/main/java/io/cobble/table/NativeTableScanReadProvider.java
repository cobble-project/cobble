package io.cobble.table;

import io.cobble.Config;
import io.cobble.DirectColumns;
import io.cobble.DirectScanEntry;

import java.util.ArrayList;
import java.util.Arrays;
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
    private final TableReadSchema readSchema;
    private final List<String> fieldNames;
    private final Table.Compiled compiled;
    private final int[] keyIndexes;
    private final int[] valueIndexes;
    private final boolean hasKey;
    private final boolean hasValues;
    private boolean opened;

    public NativeTableScanReadProvider(Config config, TableScanSplit split) {
        this(config, split, identityProjection(split.schema()));
    }

    NativeTableScanReadProvider(Config config, TableScanSplit split, int[] projection) {
        this.config = Objects.requireNonNull(config, "config");
        this.split = Objects.requireNonNull(split, "split");
        Objects.requireNonNull(projection, "projection");
        TableSchema schema = split.schema();
        this.compiled = Table.Compiled.from(schema, split.totalBuckets());
        this.keyIndexes = new int[projection.length];
        this.valueIndexes = new int[projection.length];
        Arrays.fill(keyIndexes, -1);
        Arrays.fill(valueIndexes, -1);
        List<DataField> fields = new ArrayList<DataField>(projection.length);
        List<String> names = new ArrayList<String>(projection.length);
        int valueCount = 0;
        boolean keySelected = false;
        for (int i = 0; i < projection.length; i++) {
            int source = projection[i];
            if (source < 0 || source >= schema.fields().size()) {
                throw new IllegalArgumentException("projection index outside table schema");
            }
            DataField field = schema.fields().get(source);
            fields.add(field);
            names.add(field.name());
            int keyIndex = indexOf(compiled.keyPositions, source);
            if (keyIndex >= 0) {
                keyIndexes[i] = keyIndex;
                keySelected = true;
            } else {
                valueIndexes[i] = valueCount++;
            }
        }
        // Native scans require a field even when the logical projection is empty. A key
        // keeps the row-existence column available without decoding any values.
        if (names.isEmpty()) names.add(schema.fields().get(compiled.keyPositions[0]).name());
        this.fieldNames = Collections.unmodifiableList(names);
        this.readSchema = new TableReadSchema(fields);
        this.hasKey = keySelected;
        this.hasValues = valueCount > 0;
    }

    private static int[] identityProjection(TableSchema schema) {
        int[] indexes = new int[schema.fields().size()];
        for (int i = 0; i < indexes.length; i++) indexes[i] = i;
        return indexes;
    }

    private static int indexOf(int[] positions, int source) {
        for (int i = 0; i < positions.length; i++) if (positions[i] == source) return i;
        return -1;
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
            return readSchema;
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
            TableDirectScanCursor<List<Value>> cursor =
                    TableDirectScanCursor.fixed(
                            split.openDirectScanner(config, fieldNames, 0),
                            (entry, ignoredKey) -> Collections.singletonList(decode(entry)));
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

    private List<Value> decode(DirectScanEntry entry) {
        if (keyIndexes.length == 0) return Collections.emptyList();
        List<Value> keys = hasKey ? KeyCodec.decodeOwned(compiled.keyTypes, entry.getKey()) : null;
        DirectColumns columns = hasValues ? entry.columnsView() : null;
        ArrayList<Value> row = new ArrayList<Value>(keyIndexes.length);
        for (int i = 0; i < keyIndexes.length; i++) {
            if (keyIndexes[i] >= 0) {
                row.add(keys.get(keyIndexes[i]));
            } else {
                int columnIndex = valueIndexes[i];
                if (columnIndex >= columns.size()) {
                    throw new IllegalStateException("table row has an incompatible projection");
                }
                java.nio.ByteBuffer value = columns.get(columnIndex);
                LogicalType type = readSchema.fields().get(i).logicalType();
                if (value == null) {
                    if (!type.isNullable())
                        throw new IllegalStateException("table row is missing a value column");
                    row.add(Value.nullValue());
                } else {
                    row.add(ValueCodec.decodeOwned(type, value));
                }
            }
        }
        return Collections.unmodifiableList(row);
    }
}
