package io.cobble.table;

import io.cobble.DirectScanEntry;
import io.cobble.ReadOnlyDb;
import io.cobble.ReadOptions;
import io.cobble.ScanOptions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Typed read-only access to one table in a fixed shard snapshot.
 *
 * <p>The table keeps its {@link ReadOnlyDb} reachable but does not own it. Close cursors,
 * projections, and this table before closing the database. Close must not race another operation.
 */
public final class ReadOnlyTable implements AutoCloseable {
    private final ReadOnlyDb db;
    private final TableReadBackend reads;
    private final String name;
    private final Table.Compiled compiled;
    private final ReadOptions readOptions;
    private final ScanOptions scanOptions;
    private volatile boolean closed;

    private ReadOnlyTable(ReadOnlyDb db, String name, Table.OpenInfo openInfo) {
        this.db = Objects.requireNonNull(db, "db");
        this.reads = TableReadBackend.readOnly(db);
        this.name = Objects.requireNonNull(name, "name");
        this.compiled = Table.Compiled.from(openInfo.schema, openInfo.totalBuckets);
        int[] columns = Table.physicalColumns(compiled.physicalColumns);
        this.readOptions = ReadOptions.forColumnsInFamily(name, columns);
        this.scanOptions = new ScanOptions().columnFamily(name).columns(columns);
    }

    /** Opens a table from metadata stored in the snapshot schema. */
    public static ReadOnlyTable open(ReadOnlyDb db, String name) {
        Objects.requireNonNull(db, "db");
        Objects.requireNonNull(name, "name");
        synchronized (db) {
            if (db.isDisposed()) throw new IllegalStateException("database is closed");
            String response = openNative(db.getNativeHandle(), name);
            return new ReadOnlyTable(db, name, TableJson.openInfoFromJson(response));
        }
    }

    public String name() {
        ensureUsable();
        return name;
    }

    public TableSchema schema() {
        ensureUsable();
        return compiled.schema;
    }

    /** Starts building one primary key in schema order. */
    public TableKeyBuilder keyBuilder() {
        ensureUsable();
        return new TableKeyBuilder(compiled);
    }

    /** Compiles a reusable typed projection from top-level field names. */
    public TableProjection projectByNames(List<String> fieldNames) {
        ensureUsable();
        return new TableProjection(reads, name, compiled, fieldNames);
    }

    /** Returns one owned typed row, or {@code null} when absent. */
    public List<Value> get(TableKey key) {
        ensureUsable();
        Objects.requireNonNull(key, "key");
        byte[][] columns = reads.get(key.bucket(), key.encodedInternal(), readOptions);
        return columns == null ? null : Table.assembleRow(compiled, key.valuesInternal(), columns);
    }

    /** Reads keys in one native multi-get while preserving input order and duplicates. */
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
        byte[][][] columns = reads.multiGet(buckets, keys, readOptions);
        List<List<Value>> rows = new ArrayList<List<Value>>(columns.length);
        for (int i = 0; i < columns.length; i++)
            rows.add(
                    columns[i] == null
                            ? null
                            : Table.assembleRow(
                                    compiled, primaryKeys.get(i).valuesInternal(), columns[i]));
        return Collections.unmodifiableList(rows);
    }

    /** Opens a typed scan over all rows in one bucket. */
    public TableScanCursor scan(int bucket) {
        return scanBounds(bucket, null, null);
    }

    /** Opens a typed scan over an inclusive/exclusive primary-key range in one bucket. */
    public TableScanCursor scanBounds(int bucket, TableKey startInclusive, TableKey endExclusive) {
        ensureUsable();
        Table.validateBound(bucket, startInclusive);
        Table.validateBound(bucket, endExclusive);
        byte[] start = startInclusive == null ? null : startInclusive.encodedInternal();
        byte[] end = endExclusive == null ? null : endExclusive.encodedInternal();
        return new TableScanCursor(
                db,
                reads.scan(bucket, start, end, scanOptions),
                new TableScanCursor.RowDecoder() {
                    @Override
                    public List<Value> decode(DirectScanEntry entry) {
                        return Table.decodeDirectScannedRowOwned(compiled, entry);
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

    private void ensureUsable() {
        if (closed) throw new IllegalStateException("read-only table is closed");
        reads.ensureOpen();
    }

    private static native String openNative(long dbHandle, String name);
}
