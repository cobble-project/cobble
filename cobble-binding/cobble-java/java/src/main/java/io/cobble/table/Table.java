package io.cobble.table;

import io.cobble.Db;
import io.cobble.DirectColumns;
import io.cobble.DirectScanEntry;
import io.cobble.MetricSample;
import io.cobble.NativeObject;
import io.cobble.PendingSnapshot;
import io.cobble.ReadOptions;
import io.cobble.ScanOptions;
import io.cobble.ShardSnapshot;
import io.cobble.WriteOptions;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * Typed access to one table-backed Cobble column family.
 *
 * <p>The native table keeps an internal Rust reference to its database; this Java facade does not
 * own a caller's {@link Db}. Closing a table releases only its own handle. Close cursors and tables
 * before closing the database; as with the core {@code Db} facade, {@link #close()} must not race
 * another operation.
 */
public final class Table extends NativeObject {
    private final String name;
    private volatile TableState state;
    private final DirectColumns.Reader directReader =
            new DirectColumns.Reader() {
                @Override
                public int read(int bucket, ByteBuffer ioBuffer, int keyLength) {
                    return getEncodedDirectNative(nativeHandle, bucket, ioBuffer, keyLength);
                }

                @Override
                public ByteBuffer takeOverflowBuffer() {
                    return takeDirectOverflowNative();
                }
            };

    private static void initializeDirectBufferPool(long handle) {
        int[] config = directBufferPoolConfigNative(handle);
        if (!io.cobble.Db.configureDirectBufferPool(config[0], config[1]))
            throw new IllegalStateException("direct buffer pool configuration can only grow");
    }

    private static native int[] directBufferPoolConfigNative(long handle);

    private static native int getEncodedDirectNative(
            long handle, int bucket, ByteBuffer ioBuffer, int keyLength);

    private static native int multiGetEncodedDirectNative(long handle, ByteBuffer ioBuffer);

    private static native ByteBuffer takeDirectOverflowNative();

    private Table(long nativeHandle, String name, OpenInfo openInfo) {
        super(nativeHandle);
        this.name = Objects.requireNonNull(name, "name");
        this.state = TableState.createCodec(openInfo);
        initializeDirectBufferPool(nativeHandle);
    }

    /** Starts a standalone writer for one isolated bucket. */
    public static TableWriterBuilder writerBuilder(io.cobble.Config runtime) {
        return TableWriterBuilder.fromStandalone(Objects.requireNonNull(runtime, "runtime"));
    }

    /** Creates a table, or opens an existing table when its semantic schema is identical. */
    public static Table create(io.cobble.Db db, String name, TableSchema schema) {
        Objects.requireNonNull(db, "db");
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(schema, "schema");
        synchronized (db) {
            ensureDbOpen(db);
            return createNative(db.getNativeHandle(), name, TableJson.toJson(schema));
        }
    }

    /** Opens a table from the metadata persisted in its named column family. */
    public static Table open(io.cobble.Db db, String name) {
        Objects.requireNonNull(db, "db");
        Objects.requireNonNull(name, "name");
        synchronized (db) {
            ensureDbOpen(db);
            return openNative(db.getNativeHandle(), name);
        }
    }

    static Table fromNativeHandle(long nativeHandle, String name, String openInfoJson) {
        OpenInfo openInfo = TableJson.openInfoFromJson(openInfoJson);
        return new Table(nativeHandle, name, openInfo);
    }

    public String name() {
        ensureUsable();
        return name;
    }

    /**
     * Returns an immutable snapshot of metrics for this table's backing database shard.
     *
     * <p>Metrics include all column families in that shard and are not filtered to this table.
     */
    public List<MetricSample> metrics() {
        ensureUsable();
        return parseMetricSamples(metricsJson(nativeHandle));
    }

    public TableSchema schema() {
        return state().compiled.schema;
    }

    /** Starts building one primary key in schema order. */
    public TableKeyBuilder keyBuilder() {
        return new TableKeyBuilder(state().compiled);
    }

    /** Compiles a reusable typed projection from top-level field names. */
    public TableProjection projectByNames(List<String> fieldNames) {
        TableState state = state();
        TableReadView view =
                new TableReadView(
                        createReadViewNative(
                                nativeHandle, fieldNames.toArray(new String[fieldNames.size()])));
        try {
            return new TableProjection(
                    TableReadBackend.tableView(view),
                    name,
                    state.compiled,
                    state.columnFamilyOptionsJson,
                    state.physicalColumns,
                    fieldNames,
                    view);
        } catch (RuntimeException error) {
            view.close();
            throw error;
        }
    }

    /**
     * Reload this table's local schema and layout.
     *
     * <p>Like {@link #close()}, this must not race another table operation. Existing projections
     * must be rebuilt after a successful refresh; cursors already opened retain their old view.
     * This does not consult a catalog.
     */
    public synchronized boolean refreshSchema() {
        ensureUsable();
        Table.OpenInfo openInfo = TableJson.openInfoFromJson(refreshNative(nativeHandle));
        return applyOpenInfo(openInfo);
    }

    boolean applyOpenInfo(OpenInfo openInfo) {
        TableState previous = state;
        if (previous.matches(openInfo)) return false;
        TableState candidate = TableState.createCodec(openInfo);
        state = candidate;
        previous.close();
        return true;
    }

    /** Writes one full row in schema field order. */
    public void put(List<Value> row) {
        putEncoded(row, 0L);
    }

    /** Writes one full row with caller TTL and WAL durability settings. */
    public void put(List<Value> row, WriteOptions options) {
        putEncoded(row, writeOptionsHandle(options));
    }

    private void putEncoded(List<Value> row, long writeOptionsHandle) {
        TableState state = state();
        requireRow(state.compiled, row);
        EncodedKey key = encodeRowKeyValidated(state.compiled, row);
        byte[] payload = encodeValuesValidated(state.compiled, row);
        putNative(nativeHandle, key.bucket, key.bytes, payload, writeOptionsHandle);
    }

    /**
     * Encodes and writes one row using caller-owned direct buffers.
     *
     * <p>The buffers are overwritten from position zero. JNI receives only encoded key/value bytes
     * and borrows them for the duration of this call.
     */
    public void putDirect(List<Value> row, ByteBuffer keyBuffer, ByteBuffer rowBuffer) {
        putDirectEncoded(row, keyBuffer, rowBuffer, 0L);
    }

    /** Writes one full row with caller-owned direct buffers and write options. */
    public void putDirect(
            List<Value> row, ByteBuffer keyBuffer, ByteBuffer rowBuffer, WriteOptions options) {
        putDirectEncoded(row, keyBuffer, rowBuffer, writeOptionsHandle(options));
    }

    private void putDirectEncoded(
            List<Value> row, ByteBuffer keyBuffer, ByteBuffer rowBuffer, long writeOptionsHandle) {
        TableState state = state();
        requireDirect(keyBuffer, "keyBuffer");
        requireDirect(rowBuffer, "rowBuffer");
        requireRow(state.compiled, row);
        ((Buffer) keyBuffer).clear();
        ((Buffer) rowBuffer).clear();
        int prefixEnd =
                KeyCodec.encodeFromPositionsToWithPrefix(
                        state.compiled.keyTypes,
                        row,
                        state.compiled.keyPositions,
                        state.compiled.bucketKeyFields,
                        keyBuffer);
        int keyLength = keyBuffer.position();
        int bucket = state.compiled.bucketHash.bucket(prefix(keyBuffer, prefixEnd));
        encodeValuesToValidated(state.compiled, row, rowBuffer);
        int rowLength = rowBuffer.position();
        putDirectNative(
                nativeHandle,
                bucket,
                keyBuffer,
                0,
                keyLength,
                rowBuffer,
                0,
                rowLength,
                writeOptionsHandle);
    }

    private static long writeOptionsHandle(WriteOptions options) {
        WriteOptions checked = Objects.requireNonNull(options, "options");
        long handle = checked.getNativeHandle();
        if (checked.isDisposed() || handle == 0L) {
            throw new IllegalStateException("write options is closed");
        }
        return handle;
    }

    /** Deletes one complete row. */
    public void delete(TableKey key) {
        state();
        Objects.requireNonNull(key, "key");
        deleteTableNative(nativeHandle, key.bucket(), key.encodedInternal());
    }

    /** Deletes complete rows in one native batch. Each row is atomic. */
    public void deleteBatch(List<TableKey> primaryKeys) {
        state();
        Objects.requireNonNull(primaryKeys, "primaryKeys");
        if (primaryKeys.isEmpty()) return;
        int[] buckets = new int[primaryKeys.size()];
        byte[][] keys = new byte[primaryKeys.size()][];
        for (int i = 0; i < primaryKeys.size(); i++) {
            TableKey key = Objects.requireNonNull(primaryKeys.get(i), "primaryKey");
            buckets[i] = key.bucket();
            keys[i] = key.encodedInternal();
        }
        deleteBatchTableNative(nativeHandle, buckets, keys);
    }

    /**
     * Creates a new shard snapshot asynchronously.
     *
     * <p>Keep this table and its DB open until the returned future completes.
     */
    public CompletableFuture<ShardSnapshot> asyncSnapshot() {
        return startAsyncSnapshot().future();
    }

    /**
     * Creates a new shard snapshot and returns its id with its completion future.
     *
     * <p>Keep this table and its DB open until the returned future completes.
     */
    public PendingSnapshot<ShardSnapshot> startAsyncSnapshot() {
        ensureUsable();
        CompletableFuture<String> snapshotJsonFuture = new CompletableFuture<>();
        long snapshotId = asyncSnapshotNative(nativeHandle, snapshotJsonFuture);
        return new PendingSnapshot<>(
                snapshotId, snapshotJsonFuture.thenApply(ShardSnapshot::fromJson));
    }

    /** Creates a new shard snapshot and waits for its manifest to be materialized. */
    public ShardSnapshot snapshot() {
        try {
            return asyncSnapshot().get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("snapshot interrupted", e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause() == null ? e : e.getCause();
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            }
            throw new IllegalStateException("snapshot failed: " + cause.getMessage(), cause);
        }
    }

    /** Returns the shard snapshot metadata for a completed local snapshot id. */
    public ShardSnapshot getShardSnapshot(long snapshotId) {
        ensureUsable();
        return ShardSnapshot.fromJson(getShardSnapshotJsonNative(nativeHandle, snapshotId));
    }

    /** Returns one owned typed row, or {@code null} when absent. */
    public List<Value> get(TableKey key) {
        TableState state = state();
        Objects.requireNonNull(key, "key");
        try (DirectColumns columns =
                DirectColumns.read(directReader, key.bucket(), key.encodedInternal())) {
            return columns == null
                    ? null
                    : assembleDirectRowOwned(state.compiled, key.valuesInternal(), columns);
        }
    }

    /** Reads keys in one native multi-get while preserving input order and duplicates. */
    public List<List<Value>> multiGet(List<TableKey> primaryKeys) {
        TableState state = state();
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
                        return multiGetEncodedDirectNative(nativeHandle, io);
                    }

                    @Override
                    public ByteBuffer takeOverflowBuffer() {
                        return takeDirectOverflowNative();
                    }
                },
                buckets,
                keys,
                (index, columns) ->
                        assembleDirectRowOwned(
                                state.compiled, primaryKeys.get(index).valuesInternal(), columns));
    }

    /**
     * Reads one row through direct I/O and decodes a borrowed typed view.
     *
     * <p>Binary values, including nested binary values, remain valid only until the returned row is
     * closed. The key buffer is overwritten from position zero.
     */
    public DirectTableRow getDirect(TableKey key, ByteBuffer keyBuffer) {
        TableState state = state();
        return readDirectRow(state.compiled, key, keyBuffer, directReader);
    }

    static DirectTableRow readDirectRow(
            Compiled compiled, TableKey key, ByteBuffer keyBuffer, DirectColumns.Reader reader) {
        Objects.requireNonNull(key, "key");
        requireDirect(keyBuffer, "keyBuffer");
        byte[] encodedKey = key.encodedInternal();
        ((Buffer) keyBuffer).clear();
        keyBuffer.put(encodedKey);
        DirectColumns columns =
                DirectColumns.read(reader, key.bucket(), keyBuffer, encodedKey.length);
        if (columns == null) return null;
        try {
            return new DirectTableRow(
                    columns, assembleDirectRow(compiled, key.valuesInternal(), columns));
        } catch (RuntimeException error) {
            columns.close();
            throw error;
        }
    }

    /** Opens a typed scan over all rows in one bucket. */
    public TableScanCursor scan(int bucket) {
        return scanBounds(bucket, null, null);
    }

    /** Opens a typed scan over an inclusive/exclusive primary-key range in one bucket. */
    public TableScanCursor scanBounds(int bucket, TableKey startInclusive, TableKey endExclusive) {
        TableState state = state();
        validateBound(bucket, startInclusive);
        validateBound(bucket, endExclusive);
        byte[] start = startInclusive == null ? null : startInclusive.encodedInternal();
        byte[] end = endExclusive == null ? null : endExclusive.encodedInternal();
        TableReadView view = new TableReadView(createReadViewNative(nativeHandle, null));
        try {
            return new TableScanCursor(
                    view,
                    view.scan(bucket, start, end),
                    new TableScanCursor.RowDecoder() {
                        @Override
                        public List<Value> decode(DirectScanEntry entry) {
                            return decodeDirectScannedRowOwned(state.compiled, entry);
                        }
                    },
                    view);
        } catch (RuntimeException error) {
            view.close();
            throw error;
        }
    }

    @Override
    public synchronized void close() {
        try {
            state.close();
        } finally {
            super.close();
        }
    }

    private static EncodedKey encodeRowKeyValidated(Compiled compiled, List<Value> row) {
        int size = KeyCodec.encodedSizeFromPositions(compiled.keyTypes, row, compiled.keyPositions);
        ByteBuffer output = ByteBuffer.allocate(size);
        int prefixEnd =
                KeyCodec.encodeFromPositionsToWithPrefix(
                        compiled.keyTypes,
                        row,
                        compiled.keyPositions,
                        compiled.bucketKeyFields,
                        output);
        return new EncodedKey(
                compiled.bucketHash.bucket(ByteBuffer.wrap(output.array(), 0, prefixEnd)),
                output.array());
    }

    private static byte[] encodeValuesValidated(Compiled compiled, List<Value> row) {
        ByteBuffer output = ByteBuffer.allocate(encodedValuesSizeValidated(compiled, row));
        encodeValuesToValidated(compiled, row, output);
        return output.array();
    }

    private static int encodedValuesSizeValidated(Compiled compiled, List<Value> row) {
        if (compiled.valuePositions.length == 0) return 10;
        int size = Integer.BYTES;
        for (int i = 0; i < compiled.valuePositions.length; i++) {
            int encoded =
                    ValueCodec.encodedSize(
                            compiled.valueTypes.get(i), row.get(compiled.valuePositions[i]));
            size = KeyCodec.checkedAdd(size, KeyCodec.checkedAdd(5, encoded));
        }
        return size;
    }

    private static void encodeValuesToValidated(
            Compiled compiled, List<Value> row, ByteBuffer output) {
        Objects.requireNonNull(output, "output");
        int start = output.position();
        try {
            if (compiled.valuePositions.length == 0) {
                output.putInt(1).put((byte) 1).putInt(1).put((byte) 1);
                return;
            }
            output.putInt(compiled.valuePositions.length);
            for (int i = 0; i < compiled.valuePositions.length; i++) {
                Value value = row.get(compiled.valuePositions[i]);
                LogicalType type = compiled.valueTypes.get(i);
                output.put((byte) 1);
                int lengthOffset = output.position();
                output.putInt(0);
                int valueStart = output.position();
                ValueCodec.encodeTo(type, value, output);
                output.putInt(lengthOffset, output.position() - valueStart);
            }
        } catch (RuntimeException | Error error) {
            ((Buffer) output).position(start);
            throw error;
        }
    }

    static List<Value> assembleRow(Compiled compiled, List<Value> primaryKey, byte[][] columns) {
        if (columns.length != compiled.physicalColumns)
            throw new IllegalStateException("table row has an incompatible physical layout");
        ArrayList<Value> row = emptyRow(compiled.schema.fields().size());
        for (int i = 0; i < compiled.keyPositions.length; i++)
            row.set(compiled.keyPositions[i], primaryKey.get(i));
        for (int i = 0; i < compiled.valuePositions.length; i++) {
            if (columns[i] == null)
                throw new IllegalStateException("table row is missing a value column");
            row.set(
                    compiled.valuePositions[i],
                    ValueCodec.decode(compiled.valueTypes.get(i), ByteBuffer.wrap(columns[i])));
        }
        return Collections.unmodifiableList(row);
    }

    private static List<Value> assembleDirectRow(
            Compiled compiled, List<Value> primaryKey, DirectColumns columns) {
        if (columns.size() != compiled.physicalColumns)
            throw new IllegalStateException("table row has an incompatible physical layout");
        ArrayList<Value> row = emptyRow(compiled.schema.fields().size());
        for (int i = 0; i < compiled.keyPositions.length; i++)
            row.set(compiled.keyPositions[i], primaryKey.get(i));
        for (int i = 0; i < compiled.valuePositions.length; i++) {
            ByteBuffer value = columns.get(i);
            if (value == null)
                throw new IllegalStateException("table row is missing a value column");
            row.set(
                    compiled.valuePositions[i],
                    ValueCodec.decode(compiled.valueTypes.get(i), value));
        }
        return row;
    }

    static List<Value> assembleDirectRowOwned(
            Compiled compiled, List<Value> primaryKey, DirectColumns columns) {
        if (columns.size() != compiled.physicalColumns)
            throw new IllegalStateException("table row has an incompatible physical layout");
        ArrayList<Value> row = emptyRow(compiled.schema.fields().size());
        for (int i = 0; i < compiled.keyPositions.length; i++)
            row.set(compiled.keyPositions[i], primaryKey.get(i));
        for (int i = 0; i < compiled.valuePositions.length; i++) {
            ByteBuffer value = columns.get(i);
            if (value == null)
                throw new IllegalStateException("table row is missing a value column");
            row.set(
                    compiled.valuePositions[i],
                    ValueCodec.decodeOwned(compiled.valueTypes.get(i), value));
        }
        return Collections.unmodifiableList(row);
    }

    static List<Value> decodeDirectScannedRowOwned(Compiled compiled, DirectScanEntry entry) {
        List<Value> keyValues = KeyCodec.decodeOwned(compiled.keyTypes, entry.getKey());
        return assembleDirectRowOwned(compiled, keyValues, entry.columnsView());
    }

    private static void requireRow(Compiled compiled, List<Value> row) {
        Objects.requireNonNull(row, "row");
        if (row.size() != compiled.schema.fields().size())
            throw new IllegalArgumentException("row field count does not match schema");
    }

    void ensureUsable() {
        if (isDisposed() || nativeHandle == 0L) throw new IllegalStateException("table is closed");
    }

    private TableState state() {
        ensureUsable();
        return state;
    }

    private static void ensureDbOpen(io.cobble.Db db) {
        if (db.isDisposed()) throw new IllegalStateException("database is closed");
    }

    private static void requireDirect(ByteBuffer buffer, String name) {
        Objects.requireNonNull(buffer, name);
        if (!buffer.isDirect()) throw new IllegalArgumentException(name + " must be direct");
    }

    static void validateBound(int bucket, TableKey key) {
        if (key != null && key.bucket() != bucket)
            throw new IllegalArgumentException("table scan bound belongs to a different bucket");
    }

    private static ByteBuffer prefix(ByteBuffer buffer, int end) {
        return range(buffer, 0, end);
    }

    private static ByteBuffer range(ByteBuffer buffer, int start, int end) {
        ByteBuffer range = buffer.duplicate();
        ((Buffer) range).clear();
        ((Buffer) range).position(start);
        ((Buffer) range).limit(end);
        return range;
    }

    private static ArrayList<Value> emptyRow(int size) {
        return new ArrayList<Value>(Collections.nCopies(size, (Value) null));
    }

    static int[] physicalColumns(int count) {
        int[] columns = new int[count];
        for (int i = 0; i < count; i++) columns[i] = i;
        return columns;
    }

    static final class OpenInfo {
        final TableSchema schema;
        final int totalBuckets;
        final String columnFamilyOptionsJson;
        final int physicalColumns;

        OpenInfo(
                TableSchema schema,
                int totalBuckets,
                String columnFamilyOptionsJson,
                int physicalColumns) {
            this.schema = schema;
            this.totalBuckets = totalBuckets;
            this.columnFamilyOptionsJson = columnFamilyOptionsJson;
            this.physicalColumns = physicalColumns;
        }
    }

    static final class TableState implements AutoCloseable {
        final Compiled compiled;
        final String columnFamilyOptionsJson;
        final int totalBuckets;
        final int physicalColumns;
        final ReadOptions readOptions;
        final ScanOptions scanOptions;

        private TableState(
                Compiled compiled,
                String columnFamilyOptionsJson,
                int totalBuckets,
                int physicalColumns,
                ReadOptions readOptions,
                ScanOptions scanOptions) {
            this.compiled = compiled;
            this.columnFamilyOptionsJson = columnFamilyOptionsJson;
            this.totalBuckets = totalBuckets;
            this.physicalColumns = physicalColumns;
            this.readOptions = readOptions;
            this.scanOptions = scanOptions;
        }

        static TableState createCodec(OpenInfo openInfo) {
            Compiled compiled = Compiled.from(openInfo.schema, openInfo.totalBuckets);
            if (openInfo.physicalColumns != compiled.physicalColumns) {
                throw new IllegalStateException("captured table physical layout is inconsistent");
            }
            return new TableState(
                    compiled,
                    openInfo.columnFamilyOptionsJson,
                    openInfo.totalBuckets,
                    openInfo.physicalColumns,
                    null,
                    null);
        }

        static TableState createReadOnly(String name, OpenInfo openInfo) {
            Compiled compiled = Compiled.from(openInfo.schema, openInfo.totalBuckets);
            if (openInfo.physicalColumns != compiled.physicalColumns) {
                throw new IllegalStateException("captured table physical layout is inconsistent");
            }
            int[] columns = Table.physicalColumns(compiled.physicalColumns);
            ReadOptions readOptions = null;
            ScanOptions scanOptions = null;
            try {
                readOptions = ReadOptions.forColumnsInFamily(name, columns);
                scanOptions = new ScanOptions().columnFamily(name).columns(columns);
                bindOptionsNative(
                        readOptions.getNativeHandle(),
                        scanOptions.getNativeHandle(),
                        openInfo.columnFamilyOptionsJson,
                        openInfo.physicalColumns);
                return new TableState(
                        compiled,
                        openInfo.columnFamilyOptionsJson,
                        openInfo.totalBuckets,
                        openInfo.physicalColumns,
                        readOptions,
                        scanOptions);
            } catch (RuntimeException error) {
                if (scanOptions != null) scanOptions.close();
                if (readOptions != null) readOptions.close();
                throw error;
            }
        }

        boolean matches(OpenInfo openInfo) {
            return totalBuckets == openInfo.totalBuckets
                    && physicalColumns == openInfo.physicalColumns
                    && columnFamilyOptionsJson.equals(openInfo.columnFamilyOptionsJson);
        }

        @Override
        public void close() {
            if (scanOptions != null) scanOptions.close();
            if (readOptions != null) readOptions.close();
        }
    }

    static final class Compiled {
        final TableSchema schema;
        final int[] keyPositions;
        final List<LogicalType> keyTypes;
        final int bucketKeyFields;
        final int[] valuePositions;
        final List<LogicalType> valueTypes;
        final int physicalColumns;
        final BucketHash bucketHash;

        private Compiled(
                TableSchema schema,
                int[] keyPositions,
                List<LogicalType> keyTypes,
                int bucketKeyFields,
                int[] valuePositions,
                List<LogicalType> valueTypes,
                BucketHash bucketHash) {
            this.schema = schema;
            this.keyPositions = keyPositions;
            this.keyTypes = keyTypes;
            this.bucketKeyFields = bucketKeyFields;
            this.valuePositions = valuePositions;
            this.valueTypes = valueTypes;
            this.physicalColumns = Math.max(1, valuePositions.length);
            this.bucketHash = bucketHash;
        }

        static Compiled from(TableSchema schema, int totalBuckets) {
            Map<Long, Integer> positions = new HashMap<Long, Integer>();
            for (int i = 0; i < schema.fields().size(); i++)
                positions.put(schema.fields().get(i).id(), i);
            int[] keyPositions = new int[schema.primaryKey().size()];
            List<LogicalType> keyTypes = new ArrayList<LogicalType>(keyPositions.length);
            Set<Long> keyIds = new HashSet<Long>(schema.primaryKey());
            for (int i = 0; i < keyPositions.length; i++) {
                keyPositions[i] = positions.get(schema.primaryKey().get(i));
                keyTypes.add(schema.fields().get(keyPositions[i]).logicalType());
            }
            List<Integer> valuePositionList = new ArrayList<Integer>();
            List<LogicalType> valueTypes = new ArrayList<LogicalType>();
            for (int i = 0; i < schema.fields().size(); i++) {
                DataField field = schema.fields().get(i);
                if (!keyIds.contains(field.id())) {
                    valuePositionList.add(i);
                    valueTypes.add(field.logicalType());
                }
            }
            int[] valuePositions = new int[valuePositionList.size()];
            for (int i = 0; i < valuePositions.length; i++)
                valuePositions[i] = valuePositionList.get(i);
            return new Compiled(
                    schema,
                    keyPositions,
                    Collections.unmodifiableList(keyTypes),
                    schema.bucketKey().size(),
                    valuePositions,
                    Collections.unmodifiableList(valueTypes),
                    new BucketHash(totalBuckets));
        }
    }

    private static final class EncodedKey {
        final int bucket;
        final byte[] bytes;

        private EncodedKey(int bucket, byte[] bytes) {
            this.bucket = bucket;
            this.bytes = bytes;
        }
    }

    private static native Table createNative(long dbHandle, String name, String schemaJson);

    static native Table writerCreateNative(
            String runtimeJson, String name, String schemaJson, int bucket);

    static native Table writerResumeNative(
            String runtimeJson, String name, int bucket, long snapshotId);

    private static native Table openNative(long dbHandle, String name);

    private static native void disposeNative(long nativeHandle);

    private static native String refreshNative(long nativeHandle);

    private static native String metricsJson(long nativeHandle);

    private static native long asyncSnapshotNative(
            long nativeHandle, CompletableFuture<String> snapshotJsonFuture);

    private static native String getShardSnapshotJsonNative(long nativeHandle, long snapshotId);

    private static native long createReadViewNative(long nativeHandle, String[] fieldNames);

    private static native void putNative(
            long nativeHandle, int bucket, byte[] key, byte[] rowPayload, long writeOptionsHandle);

    private static native void putDirectNative(
            long nativeHandle,
            int bucket,
            ByteBuffer key,
            int keyOffset,
            int keyLength,
            ByteBuffer rowPayload,
            int rowOffset,
            int rowLength,
            long writeOptionsHandle);

    private static native void deleteTableNative(long nativeHandle, int bucket, byte[] key);

    private static native void deleteBatchTableNative(
            long nativeHandle, int[] buckets, byte[][] keys);

    @Override
    protected void disposeInternal(long nativeHandle) {
        disposeNative(nativeHandle);
    }

    static native void bindOptionsNative(
            long readOptionsHandle,
            long scanOptionsHandle,
            String columnFamilyOptionsJson,
            int physicalColumns);
}
