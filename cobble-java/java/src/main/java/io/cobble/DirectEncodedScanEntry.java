package io.cobble;

import java.io.IOException;
import java.nio.ByteBuffer;

/** One encoded raw scan row returned from {@link DirectEncodedScanBatch}. */
public final class DirectEncodedScanEntry {
    private final ByteBuffer key;
    private final DirectEncodedRow row;

    DirectEncodedScanEntry(ByteBuffer key, DirectEncodedRow row) {
        this.key = key;
        this.row = row;
    }

    public ByteBuffer getKey() {
        return key.duplicate();
    }

    public int size() {
        return row.size();
    }

    public boolean isNull(int column) {
        return row.isNull(column);
    }

    public <T> T decodeColumn(int column, DirectEncodedRow.ColumnDecoder<T> decoder)
            throws IOException {
        return row.decodeColumn(column, decoder);
    }
}
