package io.cobble.table;

import io.cobble.Db;
import io.cobble.DirectColumns;
import io.cobble.DirectScanEntry;
import io.cobble.ReadOptions;
import io.cobble.ScanOptions;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * A reusable typed field projection over one table.
 *
 * <p>The projection owns its read and scan options. Close its cursors and the projection before
 * closing the table; {@link #close()} must not race another operation.
 */
public final class TableProjection implements AutoCloseable {
    private final Table table;
    private final Table.Compiled compiled;
    private final Source[] sources;
    private final boolean hasKeyFields;
    private final boolean hasValueFields;
    private final ReadOptions readOptions;
    private final ScanOptions scanOptions;
    private volatile boolean closed;

    TableProjection(Table table, List<String> fieldNames) {
        this.table = table;
        this.compiled = table.compiledInternal();
        Objects.requireNonNull(fieldNames, "fieldNames");
        if (fieldNames.isEmpty())
            throw new IllegalArgumentException("table projection must contain at least one field");
        List<Source> sourceList = new ArrayList<Source>(fieldNames.size());
        List<Integer> physicalColumns = new ArrayList<Integer>();
        Set<String> seen = new HashSet<String>();
        boolean selectsKey = false;
        for (String fieldNameValue : fieldNames) {
            String fieldName = Objects.requireNonNull(fieldNameValue, "fieldName");
            if (!seen.add(fieldName))
                throw new IllegalArgumentException("duplicate projection field: " + fieldName);
            int schemaPosition = schemaPosition(fieldName);
            int keyIndex = indexOf(compiled.keyPositions, schemaPosition);
            if (keyIndex >= 0) {
                sourceList.add(Source.key(keyIndex));
                selectsKey = true;
            } else {
                int physicalColumn = indexOf(compiled.valuePositions, schemaPosition);
                if (physicalColumn < 0)
                    throw new IllegalArgumentException(
                            "projection field does not map to table storage: " + fieldName);
                sourceList.add(Source.value(physicalColumns.size(), physicalColumn));
                physicalColumns.add(physicalColumn);
            }
        }
        this.hasValueFields = !physicalColumns.isEmpty();
        if (physicalColumns.isEmpty()) physicalColumns.add(0);
        int[] columns = toIntArray(physicalColumns);
        this.sources = sourceList.toArray(new Source[sourceList.size()]);
        this.hasKeyFields = selectsKey;
        this.readOptions = ReadOptions.forColumnsInFamily(table.nameInternal(), columns);
        this.scanOptions = new ScanOptions().columnFamily(table.nameInternal()).columns(columns);
    }

    /** Reads one projected row, or {@code null} when absent. */
    public List<Value> get(TableKey key) {
        ensureUsable();
        Objects.requireNonNull(key, "key");
        byte[][] columns =
                table.dbInternal().getWithOptions(key.bucket(), key.encodedInternal(), readOptions);
        return columns == null ? null : decodeRow(key.valuesInternal(), columns);
    }

    /** Reads projected rows in input order, preserving duplicates and misses. */
    public List<List<Value>> multiGet(List<TableKey> primaryKeys) {
        ensureUsable();
        Objects.requireNonNull(primaryKeys, "primaryKeys");
        int[] buckets = new int[primaryKeys.size()];
        byte[][] keys = new byte[primaryKeys.size()][];
        for (int i = 0; i < primaryKeys.size(); i++) {
            TableKey key = Objects.requireNonNull(primaryKeys.get(i), "primaryKey");
            buckets[i] = key.bucket();
            keys[i] = key.encodedInternal();
        }
        byte[][][] columns = table.dbInternal().multiGetWithOptions(buckets, keys, readOptions);
        List<List<Value>> rows = new ArrayList<List<Value>>(columns.length);
        for (int i = 0; i < columns.length; i++)
            rows.add(
                    columns[i] == null
                            ? null
                            : decodeRow(primaryKeys.get(i).valuesInternal(), columns[i]));
        return Collections.unmodifiableList(rows);
    }

    /** Opens a projected typed scan over one bucket. */
    public TableScanCursor scan(int bucket) {
        return scanBounds(bucket, null, null);
    }

    /** Opens a projected typed scan over cached primary-key bounds. */
    public TableScanCursor scanBounds(int bucket, TableKey startInclusive, TableKey endExclusive) {
        ensureUsable();
        Table.validateBound(bucket, startInclusive);
        Table.validateBound(bucket, endExclusive);
        byte[] start = startInclusive == null ? null : startInclusive.encodedInternal();
        byte[] end = endExclusive == null ? null : endExclusive.encodedInternal();
        ByteBuffer directStart = Table.directCopy(start);
        ByteBuffer directEnd = Table.directCopy(end);
        final TableProjection projection = this;
        Db db = table.dbInternal();
        return new TableScanCursor(
                db,
                db.scanDirectWithOptions(
                        bucket,
                        directStart,
                        start == null ? 0 : start.length,
                        directEnd,
                        end == null ? 0 : end.length,
                        scanOptions),
                new TableScanCursor.RowDecoder() {
                    @Override
                    public List<Value> decode(DirectScanEntry entry) {
                        return projection.decodeScannedRow(entry);
                    }
                });
    }

    @Override
    public synchronized void close() {
        if (closed) return;
        closed = true;
        scanOptions.close();
        readOptions.close();
    }

    private List<Value> decodeScannedRow(DirectScanEntry entry) {
        List<Value> keyValues =
                hasKeyFields ? KeyCodec.decodeOwned(compiled.keyTypes, entry.getKey()) : null;
        DirectColumns columns = hasValueFields ? entry.columnsView() : null;
        List<Value> row = new ArrayList<Value>(sources.length);
        for (Source source : sources) {
            if (source.keyIndex >= 0) {
                row.add(keyValues.get(source.keyIndex));
            } else {
                ByteBuffer value = columns.get(source.projectedColumn);
                if (value == null)
                    throw new IllegalStateException("table row is missing a value column");
                row.add(
                        ValueCodec.decodeOwned(
                                compiled.valueTypes.get(source.physicalColumn), value));
            }
        }
        return Collections.unmodifiableList(row);
    }

    private List<Value> decodeRow(List<Value> keyValues, byte[][] columns) {
        List<Value> row = new ArrayList<Value>(sources.length);
        for (Source source : sources) {
            if (source.keyIndex >= 0) {
                row.add(keyValues.get(source.keyIndex));
            } else {
                byte[] value = columns[source.projectedColumn];
                if (value == null)
                    throw new IllegalStateException("table row is missing a value column");
                ByteBuffer encoded = ByteBuffer.wrap(value);
                row.add(ValueCodec.decode(compiled.valueTypes.get(source.physicalColumn), encoded));
            }
        }
        return Collections.unmodifiableList(row);
    }

    private int schemaPosition(String fieldName) {
        for (int i = 0; i < compiled.schema.fields().size(); i++)
            if (compiled.schema.fields().get(i).name().equals(fieldName)) return i;
        throw new IllegalArgumentException("projection field does not exist: " + fieldName);
    }

    private void ensureUsable() {
        if (closed) throw new IllegalStateException("table projection is closed");
        table.ensureUsable();
    }

    private static int indexOf(int[] values, int target) {
        for (int i = 0; i < values.length; i++) if (values[i] == target) return i;
        return -1;
    }

    private static int[] toIntArray(List<Integer> values) {
        int[] result = new int[values.size()];
        for (int i = 0; i < values.size(); i++) result[i] = values.get(i);
        return result;
    }

    private static final class Source {
        final int keyIndex;
        final int projectedColumn;
        final int physicalColumn;

        private Source(int keyIndex, int projectedColumn, int physicalColumn) {
            this.keyIndex = keyIndex;
            this.projectedColumn = projectedColumn;
            this.physicalColumn = physicalColumn;
        }

        static Source key(int keyIndex) {
            return new Source(keyIndex, -1, -1);
        }

        static Source value(int projectedColumn, int physicalColumn) {
            return new Source(-1, projectedColumn, physicalColumn);
        }
    }
}
