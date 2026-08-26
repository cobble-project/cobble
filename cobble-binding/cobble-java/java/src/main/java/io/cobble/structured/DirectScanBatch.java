package io.cobble.structured;

import io.cobble.DirectIoUtils;

import java.nio.ByteBuffer;
import java.util.AbstractList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * A batch of rows returned from {@link DirectScanCursor} in one native call.
 *
 * <p>The rows are borrowed views over the cursor's reusable direct buffer. They remain valid only
 * until the cursor advances again or closes.
 */
public final class DirectScanBatch implements Iterable<DirectScanRow> {
    private static final String PAYLOAD_NAME = "structured direct scan batch";
    private static final DirectScanBatch EMPTY =
            new DirectScanBatch(Collections.<DirectScanRow>emptyList());

    private final List<DirectScanRow> rows;

    private DirectScanBatch(List<DirectScanRow> rows) {
        this.rows = rows;
    }

    static DirectScanBatch empty() {
        return EMPTY;
    }

    static DirectScanBatch decode(ByteBuffer encoded, int encodedLength) {
        long address = DirectIoUtils.directAddress(encoded);
        int rowCount =
                DirectIoUtils.readLength(address, encodedLength, 0, "row count", PAYLOAD_NAME);
        int maximumRowCount = (encodedLength - Integer.BYTES) / Integer.BYTES;
        if (rowCount > maximumRowCount) {
            throw new IllegalStateException("malformed structured direct scan batch");
        }
        int[] offsets = new int[rowCount];
        int[] lengths = new int[rowCount];
        int offset = Integer.BYTES;
        for (int index = 0; index < rowCount; index++) {
            int rowLength =
                    DirectIoUtils.readLength(
                            address, encodedLength, offset, "row length", PAYLOAD_NAME);
            offset += Integer.BYTES;
            DirectIoUtils.ensureRemaining(encodedLength, offset, rowLength, PAYLOAD_NAME);
            offsets[index] = offset;
            lengths[index] = rowLength;
            offset += rowLength;
        }
        if (offset != encodedLength) {
            throw new IllegalStateException("malformed structured direct scan batch");
        }
        return new DirectScanBatch(
                new AbstractList<DirectScanRow>() {
                    @Override
                    public DirectScanRow get(int index) {
                        if (index < 0 || index >= rowCount) {
                            throw new IndexOutOfBoundsException("row index: " + index);
                        }
                        return DirectScanRow.decode(
                                DirectIoUtils.slice(
                                        encoded, offsets[index], lengths[index], PAYLOAD_NAME),
                                lengths[index]);
                    }

                    @Override
                    public int size() {
                        return rowCount;
                    }
                });
    }

    /** Returns the number of rows in this batch. */
    public int size() {
        return rows.size();
    }

    /** Returns whether this batch contains no rows. */
    public boolean isEmpty() {
        return rows.isEmpty();
    }

    /** Returns one borrowed row view. */
    public DirectScanRow get(int index) {
        return rows.get(index);
    }

    @Override
    public Iterator<DirectScanRow> iterator() {
        return rows.iterator();
    }
}
