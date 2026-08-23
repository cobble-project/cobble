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

    private TableReadBackend(NativeObject owner, Db writable, ReadOnlyDb readOnly) {
        this.owner = owner;
        this.writable = writable;
        this.readOnly = readOnly;
    }

    static TableReadBackend writable(Db db) {
        return new TableReadBackend(db, db, null);
    }

    static TableReadBackend readOnly(ReadOnlyDb db) {
        return new TableReadBackend(db, null, db);
    }

    byte[][] get(int bucket, byte[] key, ReadOptions options) {
        return writable != null
                ? writable.getWithOptions(bucket, key, options)
                : readOnly.getWithOptions(bucket, key, options);
    }

    byte[][][] multiGet(int[] buckets, byte[][] keys, ReadOptions options) {
        return writable != null
                ? writable.multiGetWithOptions(buckets, keys, options)
                : readOnly.multiGetWithOptions(buckets, keys, options);
    }

    DirectScanCursor scan(
            int bucket, byte[] startInclusive, byte[] endExclusive, ScanOptions options) {
        return writable != null
                ? writable.scanDirectWithOptions(bucket, startInclusive, endExclusive, options)
                : readOnly.scanDirectWithOptions(bucket, startInclusive, endExclusive, options);
    }

    NativeObject owner() {
        return owner;
    }

    void ensureOpen() {
        if (owner.isDisposed()) throw new IllegalStateException("database is closed");
    }
}
