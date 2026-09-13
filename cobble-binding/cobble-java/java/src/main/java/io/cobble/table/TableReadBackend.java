package io.cobble.table;

import io.cobble.DirectScanCursor;
import io.cobble.NativeObject;
import io.cobble.ReadOnlyDb;
import io.cobble.ReadOptions;
import io.cobble.ScanOptions;

final class TableReadBackend {
    private final NativeObject owner;
    private final ReadOnlyDb readOnly;
    private final TableReaderView readerView;
    private final TableReadView tableView;

    private TableReadBackend(
            NativeObject owner,
            ReadOnlyDb readOnly,
            TableReaderView readerView,
            TableReadView tableView) {
        this.owner = owner;
        this.readOnly = readOnly;
        this.readerView = readerView;
        this.tableView = tableView;
    }

    static TableReadBackend readOnly(ReadOnlyDb db) {
        return new TableReadBackend(db, db, null, null);
    }

    static TableReadBackend readerView(TableReaderView view) {
        return new TableReadBackend(view, null, view, null);
    }

    static TableReadBackend tableView(TableReadView view) {
        return new TableReadBackend(view, null, null, view);
    }

    byte[][] get(int bucket, byte[] key, ReadOptions options) {
        if (readOnly != null) return readOnly.getWithOptions(bucket, key, options);
        if (tableView != null) return tableView.get(bucket, key);
        return readerView.get(bucket, key, options);
    }

    byte[][][] multiGet(int[] buckets, byte[][] keys, ReadOptions options) {
        if (readOnly != null) return readOnly.multiGetWithOptions(buckets, keys, options);
        if (tableView != null) return tableView.multiGet(buckets, keys);
        return readerView.multiGet(buckets, keys, options);
    }

    DirectScanCursor scan(
            int bucket, byte[] startInclusive, byte[] endExclusive, ScanOptions options) {
        if (readOnly != null)
            return readOnly.scanDirectWithOptions(bucket, startInclusive, endExclusive, options);
        if (tableView != null) return tableView.scan(bucket, startInclusive, endExclusive);
        return readerView.scan(bucket, startInclusive, endExclusive, options);
    }

    NativeObject owner() {
        return owner;
    }

    TableReaderView copyReaderView() {
        return readerView == null ? null : readerView.copy();
    }

    TableReadView copyTableView() {
        return tableView == null ? null : tableView.copy();
    }

    boolean usesRustTableBinding() {
        return tableView != null;
    }

    void ensureOpen() {
        if (owner.isDisposed()) throw new IllegalStateException("database is closed");
    }
}
