package io.cobble.table;

import io.cobble.DirectColumns;
import io.cobble.DirectScanCursor;
import io.cobble.NativeObject;
import io.cobble.ReadOnlyDb;
import io.cobble.ReadOptions;
import io.cobble.ScanOptions;

import java.nio.ByteBuffer;

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

    DirectColumns.Reader directReader(ReadOptions options) {
        return new DirectColumns.Reader() {
            @Override
            public int read(int bucket, ByteBuffer io, int keyLength) {
                if (readOnly != null)
                    return ReadOnlyTable.getEncodedDirectNative(
                            readOnly.getNativeHandle(),
                            bucket,
                            io,
                            keyLength,
                            options.getNativeHandle());
                if (tableView != null) return tableView.getEncodedDirect(bucket, io, keyLength);
                return readerView.getEncodedDirect(bucket, io, keyLength, options);
            }

            @Override
            public ByteBuffer takeOverflowBuffer() {
                if (readOnly != null) return ReadOnlyTable.takeDirectOverflowNative();
                if (tableView != null) return tableView.takeDirectOverflowBuffer();
                return readerView.takeDirectOverflowBuffer();
            }
        };
    }

    DirectColumns.BatchReader directBatchReader(ReadOptions options) {
        return new DirectColumns.BatchReader() {
            @Override
            public int read(ByteBuffer io) {
                if (readOnly != null)
                    return ReadOnlyTable.multiGetEncodedDirectNative(
                            readOnly.getNativeHandle(), io, options.getNativeHandle());
                if (tableView != null) return tableView.multiGetEncodedDirect(io);
                return readerView.multiGetEncodedDirect(io, options);
            }

            @Override
            public ByteBuffer takeOverflowBuffer() {
                if (readOnly != null) return ReadOnlyTable.takeDirectOverflowNative();
                if (tableView != null) return tableView.takeDirectOverflowBuffer();
                return readerView.takeDirectOverflowBuffer();
            }
        };
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
