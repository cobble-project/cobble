package io.cobble.table;

import io.cobble.DirectColumns;
import io.cobble.DirectScanEntry;
import io.cobble.ReadOnlyDb;
import io.cobble.ReadOptions;
import io.cobble.ScanOptions;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;

/**
 * Typed read-only access to one table in a fixed shard snapshot.
 *
 * <p>{@link #open(ReadOnlyDb, String)} keeps its {@link ReadOnlyDb} reachable but does not own it.
 * Tables opened by {@link CatalogTable.ReadOnlyTableBuilder} own their snapshot database. Close
 * their cursors and projections before closing the table. Close must not race another operation.
 */
public final class ReadOnlyTable implements AutoCloseable {
    private final ReadOnlyDb db;
    private final TableReadBackend reads;
    private final String name;
    private final Table.Compiled compiled;
    private final String columnFamilyOptionsJson;
    private final int physicalColumns;
    private final ReadOptions readOptions;
    private final ScanOptions scanOptions;
    private final DirectColumns.Reader directReader;
    private final boolean ownsDb;
    private volatile boolean closed;

    private ReadOnlyTable(ReadOnlyDb db, String name, Table.OpenInfo openInfo, boolean ownsDb) {
        this.db = Objects.requireNonNull(db, "db");
        this.ownsDb = ownsDb;
        this.reads = TableReadBackend.readOnly(db);
        this.name = Objects.requireNonNull(name, "name");
        this.compiled = Table.Compiled.from(openInfo.schema, openInfo.totalBuckets);
        if (openInfo.physicalColumns != compiled.physicalColumns) {
            throw new IllegalStateException("captured table physical layout is inconsistent");
        }
        this.columnFamilyOptionsJson = openInfo.columnFamilyOptionsJson;
        this.physicalColumns = openInfo.physicalColumns;
        int[] columns = Table.physicalColumns(compiled.physicalColumns);
        ReadOptions readOptions = null;
        ScanOptions scanOptions = null;
        try {
            readOptions = ReadOptions.forColumnsInFamily(name, columns);
            scanOptions = new ScanOptions().columnFamily(name).columns(columns);
            Table.bindOptionsNative(
                    readOptions.getNativeHandle(),
                    scanOptions.getNativeHandle(),
                    columnFamilyOptionsJson,
                    physicalColumns);
            this.readOptions = readOptions;
            this.scanOptions = scanOptions;
            this.directReader =
                    new DirectColumns.Reader() {
                        @Override
                        public int read(int bucket, ByteBuffer ioBuffer, int keyLength) {
                            return getEncodedDirectNative(
                                    db.getNativeHandle(),
                                    bucket,
                                    ioBuffer,
                                    keyLength,
                                    ReadOnlyTable.this.readOptions.getNativeHandle());
                        }

                        @Override
                        public ByteBuffer takeOverflowBuffer() {
                            return takeDirectOverflowNative();
                        }
                    };
        } catch (RuntimeException error) {
            if (scanOptions != null) scanOptions.close();
            if (readOptions != null) readOptions.close();
            throw error;
        }
    }

    /** Opens a table from metadata stored in the snapshot schema. */
    public static ReadOnlyTable open(ReadOnlyDb db, String name) {
        return open(db, name, false);
    }

    static ReadOnlyTable openOwned(ReadOnlyDb db, String name) {
        return open(db, name, true);
    }

    private static ReadOnlyTable open(ReadOnlyDb db, String name, boolean ownsDb) {
        Objects.requireNonNull(db, "db");
        Objects.requireNonNull(name, "name");
        synchronized (db) {
            if (db.isDisposed()) throw new IllegalStateException("database is closed");
            String response = openNative(db.getNativeHandle(), name);
            return new ReadOnlyTable(db, name, TableJson.openInfoFromJson(response), ownsDb);
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
        return new TableProjection(
                reads, name, compiled, columnFamilyOptionsJson, physicalColumns, fieldNames);
    }

    /** Returns one owned typed row, or {@code null} when absent. */
    public List<Value> get(TableKey key) {
        ensureUsable();
        Objects.requireNonNull(key, "key");
        try (DirectColumns columns =
                DirectColumns.read(directReader, key.bucket(), key.encodedInternal())) {
            return columns == null
                    ? null
                    : Table.assembleDirectRowOwned(compiled, key.valuesInternal(), columns);
        }
    }

    /**
     * Reads one row through direct I/O and decodes a borrowed typed view.
     *
     * <p>Binary values, including nested binary values, remain valid only until the returned row is
     * closed. The key buffer is overwritten from position zero.
     */
    public DirectTableRow getDirect(TableKey key, ByteBuffer keyBuffer) {
        ensureUsable();
        return Table.readDirectRow(compiled, key, keyBuffer, directReader);
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
        return DirectColumns.readBatch(
                new DirectColumns.BatchReader() {
                    @Override
                    public int read(ByteBuffer io) {
                        return multiGetEncodedDirectNative(
                                db.getNativeHandle(), io, readOptions.getNativeHandle());
                    }

                    @Override
                    public ByteBuffer takeOverflowBuffer() {
                        return takeDirectOverflowNative();
                    }
                },
                buckets,
                keys,
                (index, columns) ->
                        Table.assembleDirectRowOwned(
                                compiled, primaryKeys.get(index).valuesInternal(), columns));
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
        try {
            scanOptions.close();
            readOptions.close();
        } finally {
            if (ownsDb) db.close();
        }
    }

    private void ensureUsable() {
        if (closed) throw new IllegalStateException("read-only table is closed");
        reads.ensureOpen();
    }

    private static native String openNative(long dbHandle, String name);

    static native int getEncodedDirectNative(
            long dbHandle, int bucket, ByteBuffer buffer, int keyLength, long options);

    static native int multiGetEncodedDirectNative(long dbHandle, ByteBuffer buffer, long options);

    static native ByteBuffer takeDirectOverflowNative();
}
