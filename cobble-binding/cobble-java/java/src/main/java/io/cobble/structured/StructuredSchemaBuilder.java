package io.cobble.structured;

import io.cobble.ColumnFamilyOptions;
import io.cobble.NativeObject;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * Builder for structured schema updates.
 *
 * <p>Operations are applied to a native builder handle, and {@link #commit()} atomically commits
 * all accumulated changes.
 */
public final class StructuredSchemaBuilder extends NativeObject {
    private static final Gson GSON = new GsonBuilder().create();

    StructuredSchemaBuilder(long nativeHandle) {
        super(nativeHandle);
    }

    public StructuredSchemaBuilder addBytesColumn(int columnIdx) {
        nativeAddBytesColumn(nativeHandle, null, columnIdx);
        return this;
    }

    public StructuredSchemaBuilder addBytesColumn(String columnFamily, int columnIdx) {
        nativeAddBytesColumn(nativeHandle, columnFamily, columnIdx);
        return this;
    }

    public StructuredSchemaBuilder addListColumn(int columnIdx, ListConfig config) {
        return addListColumn(null, columnIdx, config);
    }

    public StructuredSchemaBuilder addListColumn(
            String columnFamily, int columnIdx, ListConfig config) {
        if (config == null) {
            throw new IllegalArgumentException("config must not be null");
        }
        Integer maxElements = config.getMaxElements();
        nativeAddListColumn(
                nativeHandle,
                columnFamily,
                columnIdx,
                maxElements == null ? -1 : maxElements,
                config.getRetainMode().getId(),
                config.isPreserveElementTtl());
        return this;
    }

    public StructuredSchemaBuilder deleteColumn(int columnIdx) {
        nativeDeleteColumn(nativeHandle, null, columnIdx);
        return this;
    }

    public StructuredSchemaBuilder deleteColumn(String columnFamily, int columnIdx) {
        nativeDeleteColumn(nativeHandle, columnFamily, columnIdx);
        return this;
    }

    /**
     * Applies column-family options for structured schemas.
     *
     * <p>For TTL behavior: if {@code options.valueHasTtl == false}, write-time TTL values are
     * ignored for that column family.
     */
    public StructuredSchemaBuilder setColumnFamilyOptions(
            String columnFamily, ColumnFamilyOptions options) {
        if (options == null) {
            throw new IllegalArgumentException("options must not be null");
        }
        nativeSetColumnFamilyOptions(nativeHandle, columnFamily, GSON.toJson(options));
        return this;
    }

    public Schema commit() {
        long handle = nativeHandle;
        nativeHandle = 0L;
        String resultJson = nativeCommit(handle);
        return Schema.fromJson(resultJson);
    }

    @Override
    protected native void disposeInternal(long nativeHandle);

    private static native void nativeAddBytesColumn(
            long nativeHandle, String columnFamily, int columnIdx);

    private static native void nativeAddListColumn(
            long nativeHandle,
            String columnFamily,
            int columnIdx,
            int maxElements,
            String retainMode,
            boolean preserveElementTtl);

    private static native void nativeDeleteColumn(
            long nativeHandle, String columnFamily, int columnIdx);

    private static native void nativeSetColumnFamilyOptions(
            long nativeHandle, String columnFamily, String optionsJson);

    private static native String nativeCommit(long nativeHandle);
}
