package io.cobble;

import java.io.IOException;
import java.nio.ByteBuffer;

/** One zero-copy raw scan row returned from {@link DirectScanCursor}. */
public final class DirectScanEntry {
    private final ByteBuffer key;
    private final DirectEncodedRow row;

    DirectScanEntry(ByteBuffer key, DirectEncodedRow row) {
        this.key = key;
        this.row = row;
    }

    static DirectScanEntry decode(ByteBuffer encoded, int encodedLength) {
        long address = DirectIoUtils.directAddress(encoded);
        int keyLength =
                DirectIoUtils.readLength(
                        address, encodedLength, 0, "key length", "direct scan row");
        int keyOffset = Integer.BYTES;
        DirectIoUtils.ensureRemaining(encodedLength, keyOffset, keyLength, "direct scan row");
        int rowLengthOffset = keyOffset + keyLength;
        int rowLength =
                DirectIoUtils.readLength(
                        address, encodedLength, rowLengthOffset, "row length", "direct scan row");
        int rowOffset = rowLengthOffset + Integer.BYTES;
        DirectIoUtils.ensureRemaining(encodedLength, rowOffset, rowLength, "direct scan row");
        if (rowOffset + rowLength != encodedLength) {
            throw new IllegalStateException("malformed direct scan row");
        }
        return new DirectScanEntry(
                DirectIoUtils.slice(encoded, keyOffset, keyLength, "direct scan row"),
                new DirectEncodedRow(address + rowOffset, rowLength, null));
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
