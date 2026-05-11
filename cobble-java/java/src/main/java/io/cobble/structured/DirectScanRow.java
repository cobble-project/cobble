package io.cobble.structured;

import io.cobble.DirectIoUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * One encoded direct scan row returned from {@link DirectScanCursor}.
 *
 * <p>The key is exposed as a direct {@link ByteBuffer}. Column payloads stay in Cobble's encoded
 * row format and are decoded on demand through {@link #decodeBytesColumn(int,
 * DirectEncodedRow.ColumnDecoder)} and {@link #decodeListColumn(int,
 * DirectEncodedRow.ColumnDecoder)}.
 */
public final class DirectScanRow {
    private static final String PAYLOAD_NAME = "structured direct scan row";

    private final ByteBuffer key;
    private final DirectEncodedRow row;

    DirectScanRow(ByteBuffer key, DirectEncodedRow row) {
        this.key = key;
        this.row = row;
    }

    static DirectScanRow decode(ByteBuffer encoded, int encodedLength) {
        long address = DirectIoUtils.directAddress(encoded);
        int keyLength =
                DirectIoUtils.readLength(address, encodedLength, 0, "key length", PAYLOAD_NAME);
        int keyOffset = Integer.BYTES;
        DirectIoUtils.ensureRemaining(encodedLength, keyOffset, keyLength, PAYLOAD_NAME);
        int rowLengthOffset = keyOffset + keyLength;
        int rowLength =
                DirectIoUtils.readLength(
                        address, encodedLength, rowLengthOffset, "row length", PAYLOAD_NAME);
        int rowOffset = rowLengthOffset + Integer.BYTES;
        DirectIoUtils.ensureRemaining(encodedLength, rowOffset, rowLength, PAYLOAD_NAME);
        if (rowOffset + rowLength != encodedLength) {
            throw new IllegalStateException("malformed structured direct scan row");
        }
        return new DirectScanRow(
                DirectIoUtils.slice(encoded, keyOffset, keyLength, PAYLOAD_NAME),
                new DirectEncodedRow(address + rowOffset, rowLength, null));
    }

    public ByteBuffer getKey() {
        return key.duplicate();
    }

    /** Returns the number of encoded columns in this row. */
    public int getColumnCount() {
        return row.columnCount();
    }

    public int columnCount() {
        return row.columnCount();
    }

    /** Returns whether the target column is NULL. */
    public boolean isNull(int column) {
        return row.isNull(column);
    }

    byte columnTag(int column) {
        return row.columnTag(column);
    }

    /** Decodes one BYTES column with the provided InputStream-based decoder. */
    public <T> T decodeBytesColumn(int column, DirectEncodedRow.ColumnDecoder<T> decoder)
            throws IOException {
        return row.decodeBytesColumn(column, decoder);
    }

    /** Decodes one LIST column with the provided per-element decoder. */
    public <T> List<T> decodeListColumn(int column, DirectEncodedRow.ColumnDecoder<T> decoder)
            throws IOException {
        return row.decodeListColumn(column, decoder);
    }
}
