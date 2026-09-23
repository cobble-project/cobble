package io.cobble.table;

import io.cobble.Config;
import io.cobble.GlobalSnapshot;
import io.cobble.Reader;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;

/**
 * Public reader for a current or fixed Cobble table snapshot.
 *
 * <p>The native implementation remains an internal {@link NativeTableReader}; external formats will
 * use this same facade through their fixed read sessions. Native direct reads, projections,
 * refresh, and multi-get retain their existing implementation and lifetime semantics.
 */
public final class TableReader implements AutoCloseable {
    private final NativeTableReader nativeReader;
    private final TableReadSnapshot snapshot;
    private final TableReadSession<List<Value>, List<Value>> session;
    private boolean closed;

    private TableReader(NativeTableReader nativeReader) {
        this.nativeReader = Objects.requireNonNull(nativeReader, "nativeReader");
        this.snapshot = null;
        this.session = null;
    }

    private TableReader(
            TableReadSnapshot snapshot, TableReadSession<List<Value>, List<Value>> session) {
        this.nativeReader = null;
        this.snapshot = Objects.requireNonNull(snapshot, "snapshot");
        this.session = Objects.requireNonNull(session, "session");
    }

    public static TableReader openCurrent(Config config, String tableName) {
        Objects.requireNonNull(config, "config");
        try (Reader reader = Reader.openCurrent(config)) {
            GlobalSnapshot snapshot = reader.currentGlobalSnapshot();
            if (snapshot == null) {
                throw new IllegalArgumentException("no committed snapshot for '" + tableName + "'");
            }
            TableReadSnapshot description =
                    TableReadSnapshot.forGlobal(config, snapshot, tableName);
            if (TableMetadata.FORMAT.equals(description.formatId())) {
                return new TableReader(NativeTableReader.openCurrent(config, tableName));
            }
            return openFixed(config, description);
        }
    }

    public static TableReader open(Config config, String tableName, long snapshotId) {
        Objects.requireNonNull(config, "config");
        if (snapshotId < 0L) throw new IllegalArgumentException("snapshotId must be >= 0");
        try (Reader reader = Reader.open(config, snapshotId)) {
            GlobalSnapshot snapshot = reader.currentGlobalSnapshot();
            if (snapshot == null) {
                throw new IllegalArgumentException("no committed snapshot for '" + tableName + "'");
            }
            TableReadSnapshot description =
                    TableReadSnapshot.forGlobal(config, snapshot, tableName);
            if (TableMetadata.FORMAT.equals(description.formatId())) {
                return new TableReader(NativeTableReader.open(config, tableName, snapshotId));
            }
            return openFixed(config, description);
        }
    }

    /** Opens a fixed external read through the installed table-format plugin. */
    public static TableReader open(Config config, TablePathRequest request) throws Exception {
        TableReadSnapshot snapshot = TableFormatPluginRegistry.resolvePath(config, request);
        return openFixed(config, snapshot);
    }

    /** Opens an already-resolved, fixed snapshot description without reselecting a path. */
    public static TableReader open(Config config, TableReadSnapshot snapshot) {
        Objects.requireNonNull(config, "config");
        return openFixed(config, Objects.requireNonNull(snapshot, "snapshot"));
    }

    /**
     * Opens an already-resolved fixed description with its known format implementation.
     *
     * <p>This is for integrations that package a format implementation without registering a second
     * service provider. The implementation must exactly match the persisted format ID.
     */
    public static TableReader open(
            Config config, TableReadSnapshot snapshot, TableFormatPlugin plugin) {
        Objects.requireNonNull(config, "config");
        Objects.requireNonNull(snapshot, "snapshot");
        Objects.requireNonNull(plugin, "plugin");
        if (!snapshot.formatId().equals(plugin.formatId())) {
            throw new IllegalArgumentException(
                    "format plugin '"
                            + plugin.formatId()
                            + "' does not match fixed snapshot format '"
                            + snapshot.formatId()
                            + "'");
        }
        return openFixed(config, snapshot, plugin);
    }

    private static TableReader openFixed(Config config, TableReadSnapshot snapshot) {
        if (TableMetadata.FORMAT.equals(snapshot.formatId())) {
            GlobalSnapshot global = snapshot.globalSnapshot();
            if (global == null) {
                throw new IllegalArgumentException(
                        "native table reads require a fixed global snapshot");
            }
            return new TableReader(
                    NativeTableReader.open(config, snapshot.columnFamily(), global.id));
        }
        TableFormatPlugin plugin = TableFormatPluginRegistry.resolve(snapshot.formatId());
        return openFixed(config, snapshot, plugin);
    }

    private static TableReader openFixed(
            Config config, TableReadSnapshot snapshot, TableFormatPlugin plugin) {
        try {
            if (TableMetadata.FORMAT.equals(snapshot.formatId())) {
                // Keep the native typed controller: it owns schema-transform registration,
                // retained projections, and current-view refresh. NativeTableReader exposes the
                // same NativeBinding for the facade's schema and key contract.
                plugin.bind(snapshot);
                io.cobble.GlobalSnapshot global = snapshot.globalSnapshot();
                if (global == null) {
                    throw new IllegalArgumentException(
                            "native table reads require a fixed global snapshot");
                }
                return new TableReader(
                        NativeTableReader.open(config, snapshot.columnFamily(), global.id));
            }
            TableFormatBinding binding = plugin.bind(snapshot);
            return new TableReader(
                    snapshot, new PhysicalTableReadSession(config, snapshot, binding));
        } catch (RuntimeException error) {
            throw error;
        } catch (Exception error) {
            throw new IllegalStateException("failed to open fixed table reader", error);
        }
    }

    static TableReader fromNativeHandle(long nativeHandle, String name) {
        return new TableReader(NativeTableReader.fromNativeHandle(nativeHandle, name));
    }

    /** Logical read schema. Unlike native write schemas, this does not require a primary key. */
    public TableReadSchema schema() {
        ensureOpen();
        return nativeReader == null ? session.schema() : nativeReader.binding().schema();
    }

    /** Native table schema, including primary-key metadata required by write-aware callers. */
    public TableSchema tableSchema() {
        ensureOpen();
        requireNative();
        return nativeReader.tableSchema();
    }

    public TableReadCapabilities capabilities() {
        ensureOpen();
        return nativeReader == null
                ? session.capabilities()
                : nativeReader.binding().capabilities();
    }

    /** Full-key fields in the exact order accepted by {@link #lookup(List)}. */
    public List<DataField> keyFields() {
        ensureOpen();
        return nativeReader == null ? session.keyFields() : nativeReader.binding().keyFields();
    }

    /** Opens a logical scan on this reader's fixed snapshot. */
    public TableReadCursor<List<Value>> scan(TableReadRange range, TableReadPosition position)
            throws Exception {
        ensureOpen();
        Objects.requireNonNull(range, "range");
        if (nativeReader == null) return session.scan(range, position);
        return scanPlan().scan(configForNative(), range, position);
    }

    /** Exact lookup by the format's documented full-key field order. */
    public java.util.Collection<TableReadEntry<List<Value>>> lookup(List<Value> fullKey)
            throws Exception {
        ensureOpen();
        if (nativeReader != null) {
            TableKeyBuilder builder = nativeReader.keyBuilder();
            for (Value value : Objects.requireNonNull(fullKey, "fullKey")) builder.push(value);
            TableKey key = builder.build();
            List<Value> row = nativeReader.get(key);
            return row == null
                    ? java.util.Collections.<TableReadEntry<List<Value>>>emptyList()
                    : java.util.Collections.singletonList(
                            new TableReadEntry<List<Value>>(
                                    new TableReadPosition(key.bucket(), null, 1), row));
        }
        return session.lookup(Objects.requireNonNull(fullKey, "fullKey"));
    }

    public String name() {
        ensureOpen();
        return nativeReader == null ? snapshot.columnFamily() : nativeReader.name();
    }

    public TableKeyBuilder keyBuilder() {
        requireNative();
        return nativeReader.keyBuilder();
    }

    public boolean refresh() {
        ensureOpen();
        return nativeReader != null && nativeReader.refresh();
    }

    public List<Value> get(TableKey key) {
        requireNative();
        return nativeReader.get(key);
    }

    public DirectTableRow getDirect(TableKey key, ByteBuffer keyBuffer) {
        requireNative();
        return nativeReader.getDirect(key, keyBuffer);
    }

    public List<List<Value>> multiGet(List<TableKey> keys) {
        requireNative();
        return nativeReader.multiGet(keys);
    }

    public TableProjection projectByNames(List<String> fields) {
        requireNative();
        return nativeReader.projectByNames(fields);
    }

    public TableScanCursor scan(int bucket) {
        requireNative();
        return nativeReader.scan(bucket);
    }

    public TableScanCursor scanBounds(int bucket, TableKey start, TableKey end) {
        requireNative();
        return nativeReader.scanBounds(bucket, start, end);
    }

    public TableScanPlan scanPlan() {
        ensureOpen();
        if (nativeReader != null) return nativeReader.scanPlan();
        long dataSizeBytes = 0L;
        for (TableReadSnapshot.ShardDescriptor shard : snapshot.shards()) {
            dataSizeBytes = Math.addExact(dataSizeBytes, shard.snapshot().dataSizeBytes);
        }
        return TableScanPlan.forRead(
                snapshot, session.schema(), session.capabilities(), dataSizeBytes);
    }

    private Config configForNative() {
        return nativeReader.config();
    }

    @Override
    public void close() {
        if (closed) return;
        closed = true;
        if (nativeReader != null) nativeReader.close();
        else session.close();
    }

    private void requireNative() {
        ensureOpen();
        if (nativeReader == null) {
            throw new UnsupportedOperationException(
                    "format '"
                            + snapshot.formatId()
                            + "' does not expose native direct table APIs");
        }
    }

    private void ensureOpen() {
        if (closed) throw new IllegalStateException("table reader is closed");
    }
}
