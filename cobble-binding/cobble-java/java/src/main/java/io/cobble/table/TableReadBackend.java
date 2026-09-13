package io.cobble.table;

import io.cobble.Db;
import io.cobble.DirectScanCursor;
import io.cobble.NativeObject;
import io.cobble.ReadOnlyDb;
import io.cobble.ReadOptions;
import io.cobble.ScanOptions;

final class TableReadBackend {
    private final NativeObject owner;
    private final Db writable;
    private final ReadOnlyDb readOnly;
    private final TableReaderView readerView;

    private TableReadBackend(
            NativeObject owner, Db writable, ReadOnlyDb readOnly, TableReaderView readerView) {
        this.owner = owner;
        this.writable = writable;
        this.readOnly = readOnly;
        this.readerView = readerView;
    }

    static TableReadBackend writable(Db db) {
        return new TableReadBackend(db, db, null, null);
    }

    static TableReadBackend readOnly(ReadOnlyDb db) {
        return new TableReadBackend(db, null, db, null);
    }

    static TableReadBackend readerView(TableReaderView view) {
        return new TableReadBackend(view, null, null, view);
    }

    byte[][] get(int bucket, byte[] key, ReadOptions options) {
        if (writable != null) return writable.getWithOptions(bucket, key, options);
        if (readOnly != null) return readOnly.getWithOptions(bucket, key, options);
        return readerView.get(bucket, key, options);
    }

    byte[][][] multiGet(int[] buckets, byte[][] keys, ReadOptions options) {
        if (writable != null) return writable.multiGetWithOptions(buckets, keys, options);
        if (readOnly != null) return readOnly.multiGetWithOptions(buckets, keys, options);
        return readerView.multiGet(buckets, keys, options);
    }

    DirectScanCursor scan(
            int bucket, byte[] startInclusive, byte[] endExclusive, ScanOptions options) {
        if (writable != null)
            return writable.scanDirectWithOptions(bucket, startInclusive, endExclusive, options);
        if (readOnly != null)
            return readOnly.scanDirectWithOptions(bucket, startInclusive, endExclusive, options);
        return readerView.scan(bucket, startInclusive, endExclusive, options);
    }

    NativeObject owner() {
        return owner;
    }

    TableReaderView copyReaderView() {
        return readerView == null ? null : readerView.copy();
    }

    void ensureOpen() {
        if (owner.isDisposed()) throw new IllegalStateException("database is closed");
    }
}
