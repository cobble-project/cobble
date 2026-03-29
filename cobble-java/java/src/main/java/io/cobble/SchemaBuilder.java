package io.cobble;

/**
 * Builder for schema updates. Each operation is validated immediately by the native engine. Call
 * {@link #commit()} to atomically apply all changes.
 *
 * <p>Obtain via {@link Db#updateSchema()} or {@link SingleDb#updateSchema()}.
 */
public final class SchemaBuilder extends NativeObject {

    SchemaBuilder(long nativeHandle) {
        super(nativeHandle);
    }

    /** Set the merge operator for an existing column. */
    public SchemaBuilder setColumnOperator(int columnIdx, MergeOperatorType operator) {
        if (operator == null) {
            throw new IllegalArgumentException("operator must not be null");
        }
        nativeSetColumnOperator(
                nativeHandle, columnIdx, operator.operatorId(), operator.metadataJson());
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
        String opId = operator == null ? null : operator.operatorId();
        String meta = operator == null ? null : operator.metadataJson();
        nativeAddColumn(nativeHandle, columnIdx, opId, meta, defaultValue);
        return this;
    }

    /** Delete the column at the specified index, shifting remaining columns left. */
    public SchemaBuilder deleteColumn(int columnIdx) {
        nativeDeleteColumn(nativeHandle, columnIdx);
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
            long nativeHandle, int columnIdx, String operatorId, String metadataJson);

    private static native void nativeAddColumn(
            long nativeHandle,
            int columnIdx,
            String operatorId,
            String metadataJson,
            byte[] defaultValue);

    private static native void nativeDeleteColumn(long nativeHandle, int columnIdx);

    private static native String nativeCommit(long nativeHandle);
}
