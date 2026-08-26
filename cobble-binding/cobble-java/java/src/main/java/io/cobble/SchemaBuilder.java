package io.cobble;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * Builder for schema updates. Each operation is validated immediately by the native engine. Call
 * {@link #commit()} to atomically apply all changes.
 *
 * <p>Obtain via {@link Db#updateSchema()} or {@link SingleDb#updateSchema()}.
 */
public final class SchemaBuilder extends NativeObject {
    private static final Gson GSON = new GsonBuilder().create();

    SchemaBuilder(long nativeHandle) {
        super(nativeHandle);
    }

    /** Set the merge operator for an existing column. */
    public SchemaBuilder setColumnOperator(int columnIdx, MergeOperatorType operator) {
        return setColumnOperator(null, columnIdx, operator);
    }

    /** Set the merge operator for an existing column in one column family. */
    public SchemaBuilder setColumnOperator(
            String columnFamily, int columnIdx, MergeOperatorType operator) {
        if (operator == null) {
            throw new IllegalArgumentException("operator must not be null");
        }
        nativeSetColumnOperator(
                nativeHandle,
                columnFamily,
                columnIdx,
                operator.operatorId(),
                operator.metadataJson());
        return this;
    }

    /**
     * Add a new column at the specified index, shifting existing columns right.
     *
     * @param columnIdx insertion index
     * @param operator merge operator for the new column, or null for default (BYTES)
     * @param defaultValue default value for existing rows, or null
     */
    public SchemaBuilder addColumn(int columnIdx, MergeOperatorType operator, byte[] defaultValue) {
        return addColumn(null, columnIdx, operator, defaultValue);
    }

    public SchemaBuilder addColumn(
            String columnFamily, int columnIdx, MergeOperatorType operator, byte[] defaultValue) {
        String opId = operator == null ? null : operator.operatorId();
        String meta = operator == null ? null : operator.metadataJson();
        nativeAddColumn(nativeHandle, columnFamily, columnIdx, opId, meta, defaultValue);
        return this;
    }

    /** Delete the column at the specified index, shifting remaining columns left. */
    public SchemaBuilder deleteColumn(int columnIdx) {
        return deleteColumn(null, columnIdx);
    }

    /** Delete the column at the specified index inside one column family. */
    public SchemaBuilder deleteColumn(String columnFamily, int columnIdx) {
        nativeDeleteColumn(nativeHandle, columnFamily, columnIdx);
        return this;
    }

    /**
     * Applies column-family options on top of the existing schema family.
     *
     * <p>For TTL behavior: if {@code options.valueHasTtl == false}, write-time TTL values are
     * ignored for that column family.
     */
    public SchemaBuilder setColumnFamilyOptions(String columnFamily, ColumnFamilyOptions options) {
        if (options == null) {
            throw new IllegalArgumentException("options must not be null");
        }
        nativeSetColumnFamilyOptions(nativeHandle, columnFamily, GSON.toJson(options));
        return this;
    }

    /** Commit all accumulated operations atomically and return the new schema. */
    public Schema commit() {
        long handle = nativeHandle;
        nativeHandle = 0L;
        String resultJson = nativeCommit(handle);
        return Schema.fromJson(resultJson);
    }

    @Override
    protected native void disposeInternal(long nativeHandle);

    private static native void nativeSetColumnOperator(
            long nativeHandle,
            String columnFamily,
            int columnIdx,
            String operatorId,
            String metadataJson);

    private static native void nativeAddColumn(
            long nativeHandle,
            String columnFamily,
            int columnIdx,
            String operatorId,
            String metadataJson,
            byte[] defaultValue);

    private static native void nativeDeleteColumn(
            long nativeHandle, String columnFamily, int columnIdx);

    private static native void nativeSetColumnFamilyOptions(
            long nativeHandle, String columnFamily, String optionsJson);

    private static native String nativeCommit(long nativeHandle);
}
