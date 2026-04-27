package io.cobble;

import java.nio.ByteBuffer;

/** One zero-copy raw scan row returned from {@link DirectScanCursor}. */
public final class DirectScanEntry {
    private final ByteBuffer key;
    private final DirectColumns columns;

    DirectScanEntry(ByteBuffer key, DirectColumns columns) {
        this.key = key;
        this.columns = columns;
    }

    public ByteBuffer getKey() {
        return key.duplicate();
    }

    public int size() {
        return columns.size();
    }

    public ByteBuffer getColumn(int column) {
        return columns.get(column);
    }
}
