package io.cobble.table;

import io.cobble.Config;
import io.cobble.Db;
import io.cobble.DbCoordinator;
import io.cobble.NativeLoader;
import io.cobble.NativeObject;
import io.cobble.ReadOnlyDb;

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
                return materializeNative(nativeHandle, db.getNativeHandle());
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
                return table.applyOpenInfo(
                        TableJson.openInfoFromJson(
                                refreshWriterNative(nativeHandle, table.getNativeHandle())));
            }
        }
    }

    /** Starts a builder for one catalog-scoped writable shard. */
    public TableWriterBuilder writerBuilder(Config runtime) {
        Objects.requireNonNull(runtime, "runtime");
        synchronized (this) {
            ensureOpen();
        }
        return TableWriterBuilder.fromCatalog(this, runtime);
    }

    /** Starts a builder for a portable writer initialization plan. */
    public TableWriteBuilder newWriteBuilder() {
        synchronized (this) {
            ensureOpen();
        }
        return new TableWriteBuilder(this);
    }

    /** Starts a builder for one catalog-scoped global snapshot reader. */
    public ReaderBuilder readerBuilder(Config runtime) {
        Objects.requireNonNull(runtime, "runtime");
        synchronized (this) {
            ensureOpen();
        }
        return new ReaderBuilder(this, runtime);
    }

    /** Starts a builder for one catalog-scoped table over a fixed shard snapshot. */
    public ReadOnlyTableBuilder readonlyTableBuilder(Config runtime) {
        Objects.requireNonNull(runtime, "runtime");
        synchronized (this) {
            ensureOpen();
        }
        return new ReadOnlyTableBuilder(this, runtime);
    }

    /** Opens the catalog-scoped in-process snapshot committer. */
    public synchronized TableSnapshotCommitter snapshotCommitter(
            Config runtime, int maxPendingCommits) {
        Objects.requireNonNull(runtime, "runtime");
        if (maxPendingCommits <= 0)
            throw new IllegalArgumentException("maxPendingCommits must be positive");
        ensureOpen();
        return TableSnapshotCommitter.fromNativeHandle(
                snapshotCommitterNative(nativeHandle, runtime.toJson(), maxPendingCommits));
    }

    /** Opens an independently owned coordinator in this table's snapshot namespace. */
    public synchronized DbCoordinator coordinator(Config runtime) {
        Objects.requireNonNull(runtime, "runtime");
        ensureOpen();
        return coordinatorNative(nativeHandle, runtime.toJson());
    }

    @Override
    public synchronized void close() {
        super.close();
    }

    @Override
    protected native void disposeInternal(long nativeHandle);

    private static native String descriptorNative(long nativeHandle);

    private static native void disposeNative(long nativeHandle);

    private static native Table materializeNative(long nativeHandle, long dbHandle);

    private static native String refreshWriterNative(long nativeHandle, long tableHandle);

    static native Table writerOpenNative(
            long nativeHandle, String runtimeJson, int mode, long snapshotId, int bucket);

    private static native long readerOpenNative(
            long nativeHandle, String runtimeJson, long snapshotId);

    private static native ReadOnlyDb readonlyTableOpenNative(
            long nativeHandle, String runtimeJson, String dbId, long snapshotId);

    private static native long snapshotCommitterNative(
            long nativeHandle, String runtimeJson, int maxPendingCommits);

    private static native DbCoordinator coordinatorNative(long nativeHandle, String runtimeJson);

    static native String buildWritePlanNative(long nativeHandle, int totalBuckets);

    void ensureOpen() {
        if (isDisposed() || nativeHandle == 0L)
            throw new IllegalStateException("catalog table is closed");
    }

    long nativeHandleForBuilder() {
        ensureOpen();
        return nativeHandle;
    }

    private static void ensureDbOpen(Db db) {
        if (db.isDisposed() || db.getNativeHandle() == 0L)
            throw new IllegalStateException("db is closed");
    }

    /** Configures and terminally opens one catalog-scoped global snapshot reader. */
    public static final class ReaderBuilder {
        private final CatalogTable table;
        private final Config runtime;
        private long snapshotId = Long.MIN_VALUE;

        private ReaderBuilder(CatalogTable table, Config runtime) {
            this.table = table;
            this.runtime = runtime;
        }

        public ReaderBuilder currentGlobalSnapshot() {
            snapshotId = -1L;
            return this;
        }

        public ReaderBuilder globalSnapshot(long value) {
            if (value < 0L) throw new IllegalArgumentException("snapshotId must be >= 0");
            snapshotId = value;
            return this;
        }

        public TableReader open() {
            if (snapshotId == Long.MIN_VALUE)
                throw new IllegalStateException(
                        "CatalogTable.ReaderBuilder requires a snapshot selection");
            synchronized (table) {
                table.ensureOpen();
                return TableReader.fromNativeHandle(
                        readerOpenNative(table.nativeHandle, runtime.toJson(), snapshotId),
                        table.physicalName);
            }
        }
    }

    /** Configures and terminally opens one catalog-scoped table over a fixed shard snapshot. */
    public static final class ReadOnlyTableBuilder {
        private final CatalogTable table;
        private final Config runtime;
        private String dbId;
        private long snapshotId = Long.MIN_VALUE;

        private ReadOnlyTableBuilder(CatalogTable table, Config runtime) {
            this.table = table;
            this.runtime = runtime;
        }

        /** Selects the source shard and its durable snapshot. */
        public ReadOnlyTableBuilder shardSnapshot(String value, long snapshot) {
            if (snapshot < 0L) throw new IllegalArgumentException("snapshotId must be >= 0");
            dbId = Objects.requireNonNull(value, "dbId");
            snapshotId = snapshot;
            return this;
        }

        /** Opens the selected snapshot table. The returned table owns its snapshot database. */
        public ReadOnlyTable open() {
            if (snapshotId == Long.MIN_VALUE)
                throw new IllegalStateException(
                        "CatalogTable.ReadOnlyTableBuilder requires a shard snapshot selection");
            NativeLoader.load();
            ReadOnlyDb db;
            synchronized (table) {
                table.ensureOpen();
                db =
                        readonlyTableOpenNative(
                                table.nativeHandle, runtime.toJson(), dbId, snapshotId);
            }
            if (db == null) throw new IllegalStateException("failed to open read-only table");
            try {
                return ReadOnlyTable.openOwned(db, table.physicalName);
            } catch (RuntimeException error) {
                db.close();
                throw error;
            }
        }
    }
}
