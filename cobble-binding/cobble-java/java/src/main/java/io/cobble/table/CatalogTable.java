package io.cobble.table;

import io.cobble.Db;
import io.cobble.NativeObject;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.util.Objects;

/** A fixed catalog table schema version that can materialize into one writable shard. */
public final class CatalogTable extends NativeObject {
    private final TableIdentifier identifier;
    private final long tableId;
    private final long catalogSchemaId;
    private final TableSchema schema;
    private final String physicalName;

    private CatalogTable(long nativeHandle, String descriptorJson) {
        super(nativeHandle);
        JsonObject descriptor = JsonParser.parseString(descriptorJson).getAsJsonObject();
        identifier = TableIdentifier.fromJson(descriptor.getAsJsonObject("identifier"));
        tableId = descriptor.get("table_id").getAsLong();
        catalogSchemaId = descriptor.get("catalog_schema_id").getAsLong();
        schema = TableSchema.fromJson(descriptor.getAsJsonObject("schema").toString());
        physicalName = descriptor.get("physical_name").getAsString();
    }

    static CatalogTable fromNativeHandle(long nativeHandle) {
        if (nativeHandle == 0L) throw new IllegalStateException("failed to load catalog table");
        try {
            return new CatalogTable(nativeHandle, descriptorNative(nativeHandle));
        } catch (RuntimeException e) {
            disposeNative(nativeHandle);
            throw e;
        }
    }

    public TableIdentifier identifier() {
        ensureOpen();
        return identifier;
    }

    public long tableId() {
        ensureOpen();
        return tableId;
    }

    public long catalogSchemaId() {
        ensureOpen();
        return catalogSchemaId;
    }

    public TableSchema schema() {
        ensureOpen();
        return schema;
    }

    /** Materializes this captured catalog version and returns a bound writable table. */
    public Table materializeTable(Db db) {
        Objects.requireNonNull(db, "db");
        synchronized (this) {
            ensureOpen();
            synchronized (db) {
                ensureDbOpen(db);
                return Table.fromCatalogMaterialization(
                        db, physicalName, materializeNative(nativeHandle, db.getNativeHandle()));
            }
        }
    }

    /**
     * Materializes this captured version and refreshes a matching writable table.
     *
     * <p>Like {@link Table#refreshSchema()}, this must not race an operation on {@code table}.
     */
    public boolean refreshWriter(Table table) {
        Objects.requireNonNull(table, "table");
        synchronized (this) {
            ensureOpen();
            synchronized (table) {
                Db db = table.catalogDb();
                synchronized (db) {
                    ensureDbOpen(db);
                    String openInfo =
                            refreshWriterNative(nativeHandle, db.getNativeHandle(), table.name());
                    return table.refreshFromCatalog(openInfo);
                }
            }
        }
    }

    @Override
    public synchronized void close() {
        super.close();
    }

    @Override
    protected native void disposeInternal(long nativeHandle);

    private static native String descriptorNative(long nativeHandle);

    private static native void disposeNative(long nativeHandle);

    private static native String materializeNative(long nativeHandle, long dbHandle);

    private static native String refreshWriterNative(
            long nativeHandle, long dbHandle, String tableName);

    private void ensureOpen() {
        if (isDisposed() || nativeHandle == 0L)
            throw new IllegalStateException("catalog table is closed");
    }

    private static void ensureDbOpen(Db db) {
        if (db.isDisposed() || db.getNativeHandle() == 0L)
            throw new IllegalStateException("db is closed");
    }
}
