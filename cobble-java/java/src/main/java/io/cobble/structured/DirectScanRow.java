package io.cobble.structured;

import java.nio.ByteBuffer;
import java.util.List;

/** One direct-buffer-backed row from a structured direct scan cursor. */
public final class DirectScanRow {
    private final ByteBuffer key;
    private final DirectRow row;

    DirectScanRow(ByteBuffer key, DirectRow row) {
        this.key = key;
        this.row = row;
    }

    public ByteBuffer getKey() {
        return key.duplicate();
    }

    public int getColumnCount() {
        return row.size();
    }

    public ByteBuffer getBytes(int column) {
        return row.getBytes(column);
    }

    public List<ByteBuffer> getList(int column) {
        return row.getList(column);
    }
}
