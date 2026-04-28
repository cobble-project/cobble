package io.cobble.structured;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

/** One encoded direct-buffer-backed structured scan row. */
public final class DirectEncodedScanRow {
    private final ByteBuffer key;
    private final DirectEncodedRow row;

    DirectEncodedScanRow(ByteBuffer key, DirectEncodedRow row) {
        this.key = key;
        this.row = row;
    }

    public ByteBuffer getKey() {
        return key.duplicate();
    }

    public int columnCount() {
        return row.columnCount();
    }

    public boolean isNull(int column) {
        return row.isNull(column);
    }

    public <T> T decodeBytesColumn(int column, DirectEncodedRow.ColumnDecoder<T> decoder)
            throws IOException {
        return row.decodeBytesColumn(column, decoder);
    }

    public <T> List<T> decodeListColumn(int column, DirectEncodedRow.ColumnDecoder<T> decoder)
            throws IOException {
        return row.decodeListColumn(column, decoder);
    }
}
