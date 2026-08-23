package io.cobble.table;

import io.cobble.Db;
import io.cobble.DirectColumns;
import io.cobble.DirectScanEntry;
import io.cobble.ReadOptions;
import io.cobble.ScanOptions;
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

/**
 * Typed access to one table-backed Cobble column family.
 *
 * <p>The table does not own its {@link Db}. Close cursors and tables before closing the database;
 * as with the core {@code Db} facade, {@link #close()} must not race another operation.
 */
public final class Table implements AutoCloseable {
    private final Db db;
    private final String name;
    private final Compiled compiled;
    private final ReadOptions readOptions;
    private final WriteOptions writeOptions;
    private final ScanOptions scanOptions;
    private volatile boolean closed;

    private Table(Db db, String name, OpenInfo openInfo) {
        this.db = Objects.requireNonNull(db, "db");
        this.name = Objects.requireNonNull(name, "name");
        this.compiled = Compiled.from(openInfo.schema, openInfo.totalBuckets);
        int[] columns = physicalColumns(compiled.physicalColumns);
        this.readOptions = ReadOptions.forColumnsInFamily(name, columns);
        this.writeOptions = WriteOptions.withColumnFamily(name);
        this.scanOptions = new ScanOptions().columnFamily(name).columns(columns);
    }

    /** Creates a table, or opens an existing table when its semantic schema is identical. */
    public static Table create(Db db, String name, TableSchema schema) {
        Objects.requireNonNull(db, "db");
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(schema, "schema");
        synchronized (db) {
            ensureDbOpen(db);
            String response = createNative(db.getNativeHandle(), name, TableJson.toJson(schema));
            return new Table(db, name, TableJson.openInfoFromJson(response));
        }
    }

    /** Opens a table from the metadata persisted in its named column family. */
    public static Table open(Db db, String name) {
        Objects.requireNonNull(db, "db");
        Objects.requireNonNull(name, "name");
        synchronized (db) {
            ensureDbOpen(db);
            String response = openNative(db.getNativeHandle(), name);
            return new Table(db, name, TableJson.openInfoFromJson(response));
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
        return new TableProjection(this, fieldNames);
    }

    /** Writes one full row in schema field order. */
    public void put(List<Value> row) {
        ensureUsable();
        requireRow(row);
        EncodedKey key = encodeRowKeyValidated(row);
        putEncodedNative(
                db.getNativeHandle(),
                key.bucket,
                key.bytes,
                encodeValuesValidated(row),
                writeOptions.getNativeHandle());
    }

    /**
     * Encodes and writes one row using caller-owned direct buffers.
     *
     * <p>The buffers are overwritten from position zero. JNI receives only encoded key/value bytes
     * and borrows them for the duration of this call.
     */
    public void putDirect(List<Value> row, ByteBuffer keyBuffer, ByteBuffer rowBuffer) {
        ensureUsable();
        requireDirect(keyBuffer, "keyBuffer");
        requireDirect(rowBuffer, "rowBuffer");
        requireRow(row);
        ((Buffer) keyBuffer).clear();
        ((Buffer) rowBuffer).clear();
        int prefixEnd =
                KeyCodec.encodeFromPositionsToWithPrefix(
                        compiled.keyTypes,
                        row,
                        compiled.keyPositions,
                        compiled.bucketKeyFields,
                        keyBuffer);
        int keyLength = keyBuffer.position();
        int bucket = compiled.bucketHash.bucket(prefix(keyBuffer, prefixEnd));
        encodeValuesToValidated(row, rowBuffer);
        int rowLength = rowBuffer.position();
        putEncodedDirectNative(
                db.getNativeHandle(),
                bucket,
                keyBuffer,
                0,
                keyLength,
                rowBuffer,
                0,
                rowLength,
                writeOptions.getNativeHandle());
    }

    /** Deletes one complete row. */
    public void delete(TableKey key) {
        ensureUsable();
        Objects.requireNonNull(key, "key");
        deleteNative(
                db.getNativeHandle(),
                key.bucket(),
                key.encodedInternal(),
                writeOptions.getNativeHandle());
    }

    /** Deletes complete rows in one native batch. Each row is atomic. */
    public void deleteBatch(List<TableKey> primaryKeys) {
        ensureUsable();
        Objects.requireNonNull(primaryKeys, "primaryKeys");
        if (primaryKeys.isEmpty()) return;
        int[] buckets = new int[primaryKeys.size()];
        byte[][] keys = new byte[primaryKeys.size()][];
        for (int i = 0; i < primaryKeys.size(); i++) {
            TableKey key = Objects.requireNonNull(primaryKeys.get(i), "primaryKey");
            buckets[i] = key.bucket();
            keys[i] = key.encodedInternal();
        }
        deleteBatchNative(db.getNativeHandle(), buckets, keys, writeOptions.getNativeHandle());
    }

    /** Returns one owned typed row, or {@code null} when absent. */
    public List<Value> get(TableKey key) {
        ensureUsable();
        Objects.requireNonNull(key, "key");
        byte[][] columns = db.getWithOptions(key.bucket(), key.encodedInternal(), readOptions);
        return columns == null ? null : assembleRow(key.valuesInternal(), columns);
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
        byte[][][] columns = db.multiGetWithOptions(buckets, keys, readOptions);
        List<List<Value>> rows = new ArrayList<List<Value>>(columns.length);
        for (int i = 0; i < columns.length; i++)
            rows.add(
                    columns[i] == null
                            ? null
                            : assembleRow(primaryKeys.get(i).valuesInternal(), columns[i]));
        return Collections.unmodifiableList(rows);
    }

    /**
     * Reads one row through direct I/O and decodes a borrowed typed view.
     *
     * <p>Binary values, including nested binary values, remain valid only until the returned row is
     * closed. The key buffer is overwritten from position zero.
     */
    public DirectTableRow getDirect(TableKey key, ByteBuffer keyBuffer) {
        ensureUsable();
        Objects.requireNonNull(key, "key");
        requireDirect(keyBuffer, "keyBuffer");
        ((Buffer) keyBuffer).clear();
        keyBuffer.put(key.encodedInternal());
        DirectColumns columns =
                db.getDirectColumnsWithOptions(
                        key.bucket(), keyBuffer, key.encodedInternal().length, readOptions);
        if (columns == null) return null;
        try {
            return new DirectTableRow(columns, assembleDirectRow(key.valuesInternal(), columns));
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
        ensureUsable();
        validateBound(bucket, startInclusive);
        validateBound(bucket, endExclusive);
        byte[] start = startInclusive == null ? null : startInclusive.encodedInternal();
        byte[] end = endExclusive == null ? null : endExclusive.encodedInternal();
        ByteBuffer directStart = directCopy(start);
        ByteBuffer directEnd = directCopy(end);
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
                        return decodeDirectScannedRowOwned(compiled, entry);
                    }
                });
    }

    @Override
    public synchronized void close() {
        if (closed) return;
        closed = true;
        scanOptions.close();
        readOptions.close();
        writeOptions.close();
    }

    private EncodedKey encodeRowKeyValidated(List<Value> row) {
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

    private byte[] encodeValuesValidated(List<Value> row) {
        ByteBuffer output = ByteBuffer.allocate(encodedValuesSizeValidated(row));
        encodeValuesToValidated(row, output);
        return output.array();
    }

    private int encodedValuesSizeValidated(List<Value> row) {
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

    private void encodeValuesToValidated(List<Value> row, ByteBuffer output) {
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

    private List<Value> assembleRow(List<Value> primaryKey, byte[][] columns) {
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

    private List<Value> assembleDirectRow(List<Value> primaryKey, DirectColumns columns) {
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

    static List<Value> decodeDirectScannedRowOwned(Compiled compiled, DirectScanEntry entry) {
        List<Value> keyValues = KeyCodec.decodeOwned(compiled.keyTypes, entry.getKey());
        DirectColumns columns = entry.columnsView();
        if (columns.size() != compiled.physicalColumns)
            throw new IllegalStateException("table row has an incompatible physical layout");
        ArrayList<Value> row = emptyRow(compiled.schema.fields().size());
        for (int i = 0; i < compiled.keyPositions.length; i++)
            row.set(compiled.keyPositions[i], keyValues.get(i));
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

    private void requireRow(List<Value> row) {
        Objects.requireNonNull(row, "row");
        if (row.size() != compiled.schema.fields().size())
            throw new IllegalArgumentException("row field count does not match schema");
    }

    void ensureUsable() {
        if (closed) throw new IllegalStateException("table is closed");
        ensureDbOpen(db);
    }

    private static void ensureDbOpen(Db db) {
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

    static ByteBuffer directCopy(byte[] bytes) {
        if (bytes == null) return null;
        ByteBuffer direct = ByteBuffer.allocateDirect(bytes.length);
        direct.put(bytes);
        return direct;
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

    private static int[] physicalColumns(int count) {
        int[] columns = new int[count];
        for (int i = 0; i < count; i++) columns[i] = i;
        return columns;
    }

    static final class OpenInfo {
        final TableSchema schema;
        final int totalBuckets;

        OpenInfo(TableSchema schema, int totalBuckets) {
            this.schema = schema;
            this.totalBuckets = totalBuckets;
        }
    }

    Db dbInternal() {
        return db;
    }

    String nameInternal() {
        return name;
    }

    Compiled compiledInternal() {
        return compiled;
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

    private static native String createNative(long dbHandle, String name, String schemaJson);

    private static native String openNative(long dbHandle, String name);

    private static native void putEncodedNative(
            long dbHandle, int bucket, byte[] key, byte[] rowPayload, long writeOptionsHandle);

    private static native void putEncodedDirectNative(
            long dbHandle,
            int bucket,
            ByteBuffer key,
            int keyOffset,
            int keyLength,
            ByteBuffer rowPayload,
            int rowOffset,
            int rowLength,
            long writeOptionsHandle);

    private static native void deleteNative(
            long dbHandle, int bucket, byte[] key, long writeOptionsHandle);

    private static native void deleteBatchNative(
            long dbHandle, int[] buckets, byte[][] keys, long writeOptionsHandle);
}
