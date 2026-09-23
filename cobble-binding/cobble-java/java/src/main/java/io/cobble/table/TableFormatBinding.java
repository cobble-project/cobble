package io.cobble.table;

import io.cobble.DirectColumns;
import io.cobble.DirectScanEntry;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * Session-private schema and codec binding for one fixed table-format descriptor.
 *
 * <p>Bindings may retain serializer scratch buffers and are therefore not thread-safe or shared
 * between reader sessions.
 */
public interface TableFormatBinding {
    TableReadSchema schema();

    List<DataField> keyFields();

    TableReadCapabilities capabilities();

    /** Physical column indexes requested for both point reads and direct scans. */
    int[] physicalColumns();

    /** Encodes a complete logical lookup key and routes it to a physical bucket. */
    TablePhysicalKey encodeKey(List<Value> values) throws Exception;

    /** Decodes owned physical columns returned by a point read. */
    List<List<Value>> decode(int bucket, byte[] physicalKey, byte[][] physicalColumns)
            throws Exception;

    /** Decodes transient direct columns from a point read into owned logical values. */
    default List<List<Value>> decodePointDirect(
            int bucket, byte[] physicalKey, DirectColumns physicalColumns) throws Exception {
        byte[][] copied = new byte[physicalColumns.size()][];
        for (int i = 0; i < copied.length; i++) {
            ByteBuffer value = physicalColumns.get(i);
            if (value != null) {
                copied[i] = new byte[value.remaining()];
                value.get(copied[i]);
            }
        }
        return decode(bucket, physicalKey, copied);
    }

    /**
     * Decodes one transient direct physical row. Returned values must not retain direct buffers.
     * The core calls this before advancing the cursor and owns durable positions itself.
     */
    default List<List<Value>> decodeDirect(
            DirectScanEntry entry, int bucket, byte[] ownedPhysicalKey) throws Exception {
        return decode(bucket, ownedPhysicalKey, TableDirectScanCursor.copyColumns(entry));
    }

    /** Whether a physical-reader failure means this format is absent from a selected shard. */
    default boolean isMissingColumnFamily(RuntimeException error) {
        return false;
    }
}
