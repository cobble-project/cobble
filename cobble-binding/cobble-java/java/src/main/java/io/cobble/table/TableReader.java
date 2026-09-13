package io.cobble.table;

import io.cobble.Config;
import io.cobble.DirectColumns;
import io.cobble.DirectScanCursor;
import io.cobble.NativeLoader;
import io.cobble.NativeObject;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Typed reader over one current or fixed global table snapshot.
 *
 * <p>Current readers check for new commits on read access at the configured refresh interval. Fixed
 * readers never advance. Schema access does not perform I/O. Existing projections, cursors, and
 * direct rows remain usable after this reader refreshes or closes and must be closed separately.
 */
public final class TableReader extends NativeObject {
    private final String name;
    private final long refreshIntervalNanos;
    private volatile Table.TableState state;
    private TableReaderView view;
    private long nextRefreshNanos;
    private final DirectColumns.Reader directReader =
            new DirectColumns.Reader() {
                @Override
                public int read(int bucket, ByteBuffer ioBuffer, int keyLength) {
                    return view.getEncodedDirect(bucket, ioBuffer, keyLength, state.readOptions);
                }

                @Override
                public ByteBuffer takeOverflowBuffer() {
                    return view.takeDirectOverflowBuffer();
                }
            };

    private TableReader(long nativeHandle, String name) {
        super(nativeHandle);
        this.name = name;
        long viewHandle = acquireViewNative(nativeHandle, 0L);
        if (viewHandle == 0L)
            throw new IllegalStateException("failed to acquire table reader view");
        TableReaderView initial = new TableReaderView(viewHandle);
        try {
            Table.OpenInfo info =
                    TableJson.openInfoFromJson(describeViewNative(initial.getNativeHandle()));
            state = Table.TableState.createReadOnly(name, info);
            refreshIntervalNanos = refreshIntervalNanosNative(nativeHandle);
            nextRefreshNanos = System.nanoTime() + Math.max(0L, refreshIntervalNanos);
            view = initial;
        } catch (RuntimeException error) {
            initial.close();
            throw error;
        }
    }

    static TableReader fromNativeHandle(long nativeHandle, String name) {
        if (nativeHandle == 0L) throw new IllegalStateException("failed to open table reader");
        try {
            return new TableReader(nativeHandle, name);
        } catch (RuntimeException error) {
            disposeHandleNative(nativeHandle);
            throw error;
        }
    }

    public static TableReader openCurrent(Config config, String tableName) {
        Objects.requireNonNull(config, "config");
        Objects.requireNonNull(tableName, "tableName");
        NativeLoader.load();
        long handle = openCurrentNative(config.toJson(), tableName);
        return fromNativeHandle(handle, tableName);
    }

    public static TableReader open(Config config, String tableName, long snapshotId) {
        Objects.requireNonNull(config, "config");
        Objects.requireNonNull(tableName, "tableName");
        NativeLoader.load();
        long handle = openNative(config.toJson(), tableName, snapshotId);
        return fromNativeHandle(handle, tableName);
    }

    public String name() {
        ensureUsable();
        return name;
    }

    public TableSchema schema() {
        return state().compiled.schema;
    }

    public TableKeyBuilder keyBuilder() {
        return new TableKeyBuilder(state().compiled);
    }

    /** Checks for a newer committed snapshot; returns false for fixed or unchanged readers. */
    public synchronized boolean refresh() {
        ensureUsable();
        refreshNative(nativeHandle);
        boolean changed = installChangedView();
        scheduleNext();
        return changed;
    }

    public synchronized List<Value> get(TableKey key) {
        access();
        TableReaderView current = view;
        Table.TableState currentState = state;
        byte[][] columns =
                current.get(key.bucket(), key.encodedInternal(), currentState.readOptions);
        return columns == null
                ? null
                : Table.assembleRow(currentState.compiled, key.valuesInternal(), columns);
    }

    /**
     * Reads one row through direct I/O and decodes a borrowed typed view.
     *
     * <p>Binary values, including nested binary values, remain valid only until the returned row is
     * closed. The key buffer is overwritten from position zero.
     */
    public synchronized DirectTableRow getDirect(TableKey key, ByteBuffer keyBuffer) {
        access();
        return Table.readDirectRow(state.compiled, key, keyBuffer, directReader);
    }

    public synchronized List<List<Value>> multiGet(List<TableKey> keys) {
        access();
        TableReaderView current = view;
        Table.TableState currentState = state;
        int[] buckets = new int[keys.size()];
        byte[][] encoded = new byte[keys.size()][];
        for (int i = 0; i < keys.size(); i++) {
            TableKey key = Objects.requireNonNull(keys.get(i), "primaryKey");
            buckets[i] = key.bucket();
            encoded[i] = key.encodedInternal();
        }
        byte[][][] columns = current.multiGet(buckets, encoded, currentState.readOptions);
        List<List<Value>> rows = new ArrayList<List<Value>>(columns.length);
        for (int i = 0; i < columns.length; i++)
            rows.add(
                    columns[i] == null
                            ? null
                            : Table.assembleRow(
                                    currentState.compiled,
                                    keys.get(i).valuesInternal(),
                                    columns[i]));
        return Collections.unmodifiableList(rows);
    }

    public synchronized TableProjection projectByNames(List<String> fields) {
        access();
        TableReaderView retained = view.copy();
        Table.TableState currentState = state;
        try {
            return new TableProjection(
                    TableReadBackend.readerView(retained),
                    name,
                    currentState.compiled,
                    currentState.columnFamilyOptionsJson,
                    currentState.physicalColumns,
                    fields,
                    retained);
        } catch (RuntimeException error) {
            retained.close();
            throw error;
        }
    }

    public synchronized TableScanCursor scan(int bucket) {
        return scanBounds(bucket, null, null);
    }

    public synchronized TableScanCursor scanBounds(int bucket, TableKey start, TableKey end) {
        access();
        Table.TableState currentState = state;
        Table.validateBound(bucket, start);
        Table.validateBound(bucket, end);
        TableReaderView retained = view.copy();
        try {
            DirectScanCursor cursor =
                    retained.scan(
                            bucket,
                            start == null ? null : start.encodedInternal(),
                            end == null ? null : end.encodedInternal(),
                            currentState.scanOptions);
            return new TableScanCursor(
                    retained,
                    cursor,
                    entry -> Table.decodeDirectScannedRowOwned(currentState.compiled, entry),
                    retained);
        } catch (RuntimeException error) {
            retained.close();
            throw error;
        }
    }

    /** Builds a portable full-scan plan pinned to this reader's current loaded snapshot. */
    public synchronized TableScanPlan scanPlan() {
        access();
        return view.scanPlan();
    }

    @Override
    public synchronized void close() {
        if (!isDisposed() && view != null) view.close();
        if (!isDisposed() && state != null) state.close();
        super.close();
    }

    @Override
    protected native void disposeInternal(long nativeHandle);

    private void access() {
        ensureUsable();
        if (refreshIntervalNanos >= 0L && System.nanoTime() - nextRefreshNanos >= 0L) {
            refreshNative(nativeHandle);
            installChangedView();
            scheduleNext();
        }
    }

    private boolean installChangedView() {
        long candidateHandle = acquireViewNative(nativeHandle, view.getNativeHandle());
        if (candidateHandle == 0L) return false;
        TableReaderView candidate = new TableReaderView(candidateHandle);
        try {
            Table.TableState candidateState =
                    Table.TableState.createReadOnly(
                            name, TableJson.openInfoFromJson(describeViewNative(candidateHandle)));
            TableReaderView previousView = view;
            Table.TableState previousState = state;
            view = candidate;
            state = candidateState;
            previousView.close();
            previousState.close();
            return true;
        } catch (RuntimeException error) {
            candidate.close();
            throw error;
        }
    }

    private void scheduleNext() {
        nextRefreshNanos = System.nanoTime() + Math.max(0L, refreshIntervalNanos);
    }

    private Table.TableState state() {
        ensureUsable();
        return state;
    }

    private void ensureUsable() {
        if (isDisposed() || nativeHandle == 0L)
            throw new IllegalStateException("table reader is closed");
    }

    static native long cloneViewNative(long viewHandle);

    private static native long openCurrentNative(String configJson, String tableName);

    private static native long openNative(String configJson, String tableName, long snapshotId);

    private static native void disposeHandleNative(long nativeHandle);

    private static native long acquireViewNative(long nativeHandle, long currentViewHandle);

    private static native String describeViewNative(long viewHandle);

    private static native boolean refreshNative(long nativeHandle);

    private static native long refreshIntervalNanosNative(long nativeHandle);
}
