package io.cobble.table;

import io.cobble.DirectColumns;
import io.cobble.DirectScanCursor;
import io.cobble.DirectScanEntry;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Shared direct physical-scan traversal for format readers that expand one physical entry into zero
 * or more owned logical rows.
 *
 * <p>Decoders receive a transient {@link DirectScanEntry} plus an owned key copy. They must return
 * rows that do not retain direct-buffer views. The cursor owns every direct cursor it opens and
 * closes the active cursor when decoding fails.
 */
public final class TableDirectScanCursor<R> implements TableReadCursor<R> {
    /** Opens one direct cursor for a bucket and its resume-aware bounds. */
    @FunctionalInterface
    public interface BucketCursorOpener {
        DirectScanCursor open(int bucket, byte[] startInclusive, byte[] endExclusive)
                throws Exception;
    }

    /**
     * Decodes one transient physical entry into owned logical rows. The key is a decoder-private
     * heap copy and may be handled as an ordinary byte array, but direct entry views remain valid
     * only for this callback.
     */
    @FunctionalInterface
    public interface EntryDecoder<R> {
        List<R> decode(DirectScanEntry entry, byte[] ownedKey) throws Exception;
    }

    private final List<Integer> buckets;
    private final TableReadPosition resume;
    private final byte[] resumeKey;
    private final byte[] endExclusive;
    private final BucketCursorOpener opener;
    private final EntryDecoder<R> decoder;
    private final boolean fixedCursor;
    private DirectScanCursor cursor;
    private int bucketIndex;
    private int entryBucket;
    private byte[] entryKey;
    private long entryBytes;
    private List<R> rows = Collections.emptyList();
    private int rowOffset;
    private boolean physicalEntryPending;
    private boolean closed;

    private TableDirectScanCursor(
            List<Integer> buckets,
            TableReadPosition resume,
            byte[] endExclusive,
            BucketCursorOpener opener,
            EntryDecoder<R> decoder,
            DirectScanCursor cursor,
            boolean fixedCursor) {
        this.buckets = Collections.unmodifiableList(new ArrayList<Integer>(buckets));
        this.resume = resume;
        this.resumeKey = resume == null ? null : resume.physicalKey();
        this.endExclusive = copy(endExclusive);
        this.opener = opener;
        this.decoder = Objects.requireNonNull(decoder, "decoder");
        this.cursor = cursor;
        this.fixedCursor = fixedCursor;
        if (resume != null && !this.buckets.contains(Integer.valueOf(resume.bucket()))) {
            throw new IllegalArgumentException(
                    "resume position is outside the direct scan buckets");
        }
        while (resume != null
                && bucketIndex < this.buckets.size()
                && this.buckets.get(bucketIndex).intValue() < resume.bucket()) {
            bucketIndex++;
        }
    }

    /** Traverses one already-opened fixed cursor without bucket seeking. */
    public static <R> TableDirectScanCursor<R> fixed(
            DirectScanCursor cursor, EntryDecoder<R> decoder) {
        return new TableDirectScanCursor<R>(
                Collections.<Integer>emptyList(),
                null,
                null,
                null,
                decoder,
                Objects.requireNonNull(cursor, "cursor"),
                true);
    }

    /**
     * Traverses fixed buckets, reopening a direct cursor only when moving to the next bucket.
     * {@code endExclusive} may be null for an unbounded upper scan range.
     */
    public static <R> TableDirectScanCursor<R> buckets(
            List<Integer> buckets,
            TableReadPosition resume,
            byte[] endExclusive,
            BucketCursorOpener opener,
            EntryDecoder<R> decoder) {
        Objects.requireNonNull(buckets, "buckets");
        if (buckets.isEmpty() && resume != null) {
            throw new IllegalArgumentException(
                    "resume position requires an owned direct scan bucket");
        }
        int previous = -1;
        for (Integer bucket : buckets) {
            if (bucket == null || bucket.intValue() < 0 || bucket.intValue() <= previous) {
                throw new IllegalArgumentException(
                        "buckets must be strictly ascending non-negative bucket ids");
            }
            previous = bucket.intValue();
        }
        return new TableDirectScanCursor<R>(
                buckets,
                resume,
                endExclusive,
                Objects.requireNonNull(opener, "opener"),
                decoder,
                null,
                false);
    }

    /** Copies every transient direct column for decoders that require heap byte arrays. */
    public static byte[][] copyColumns(DirectScanEntry entry) {
        Objects.requireNonNull(entry, "entry");
        DirectColumns columns = entry.columnsView();
        byte[][] copy = new byte[columns.size()][];
        for (int index = 0; index < copy.length; index++) {
            ByteBuffer value = columns.get(index);
            copy[index] = value == null ? null : copy(value);
        }
        return copy;
    }

    /** Returns raw key/value payload bytes, matching connector physical-entry accounting. */
    public static long physicalBytes(DirectScanEntry entry, byte[] ownedKey) {
        Objects.requireNonNull(entry, "entry");
        Objects.requireNonNull(ownedKey, "ownedKey");
        long bytes = ownedKey.length;
        DirectColumns columns = entry.columnsView();
        for (int index = 0; index < columns.size(); index++) {
            ByteBuffer value = columns.get(index);
            if (value != null) bytes += value.remaining();
        }
        return bytes;
    }

    @Override
    public TableReadEntry<R> next() throws Exception {
        ensureOpen();
        try {
            while (true) {
                if (rowOffset < rows.size()) {
                    int emittedOffset = ++rowOffset;
                    boolean countPhysicalEntry = physicalEntryPending;
                    physicalEntryPending = false;
                    return new TableReadEntry<R>(
                            TableReadPosition.owned(entryBucket, entryKey, emittedOffset),
                            rows.get(emittedOffset - 1),
                            countPhysicalEntry ? entryBytes : 0L,
                            countPhysicalEntry);
                }
                if (!loadRows()) return null;
            }
        } catch (Exception error) {
            try {
                close();
            } catch (RuntimeException closeError) {
                error.addSuppressed(closeError);
            }
            throw error;
        }
    }

    private boolean loadRows() throws Exception {
        while (true) {
            if (cursor == null) {
                if (fixedCursor || bucketIndex >= buckets.size()) return false;
                int bucket = buckets.get(bucketIndex).intValue();
                byte[] start =
                        resume != null && bucket == resume.bucket() ? resumeKey : new byte[0];
                cursor = opener.open(bucket, start, endExclusive);
                if (cursor == null) {
                    bucketIndex++;
                    continue;
                }
            }
            DirectScanEntry entry = cursor.nextEntry();
            if (entry == null) {
                closeCurrentCursor();
                if (fixedCursor) return false;
                bucketIndex++;
                continue;
            }
            entryBucket = entry.getBucket();
            if (entryBucket < 0 && !fixedCursor) entryBucket = buckets.get(bucketIndex).intValue();
            entryKey = copy(entry.getKey());
            entryBytes = physicalBytes(entry, entryKey);
            // Positions share entryKey across expanded logical rows. Keep decoders isolated from
            // that durable state because their public contract permits ordinary byte[] handling.
            rows = Objects.requireNonNull(decoder.decode(entry, copy(entryKey)), "decoded rows");
            rowOffset = 0;
            if (resume != null
                    && entryBucket == resume.bucket()
                    && Arrays.equals(entryKey, resumeKey)) {
                rowOffset = resume.intraEntryOffset();
                if (rowOffset > rows.size()) {
                    throw new IllegalArgumentException(
                            "resume offset exceeds decoded physical entry rows");
                }
            }
            physicalEntryPending = true;
            if (rowOffset < rows.size()) return true;
        }
    }

    @Override
    public void close() {
        if (closed) return;
        closed = true;
        try {
            closeCurrentCursor();
        } finally {
            rows = Collections.emptyList();
            rowOffset = 0;
            entryKey = null;
            entryBytes = 0L;
            physicalEntryPending = false;
        }
    }

    private void closeCurrentCursor() {
        if (cursor != null) {
            try {
                cursor.close();
            } finally {
                cursor = null;
            }
        }
    }

    private void ensureOpen() {
        if (closed) throw new IllegalStateException("direct table scan cursor is closed");
    }

    private static byte[] copy(ByteBuffer value) {
        ByteBuffer view = value.duplicate();
        byte[] copy = new byte[view.remaining()];
        view.get(copy);
        return copy;
    }

    private static byte[] copy(byte[] value) {
        return value == null ? null : Arrays.copyOf(value, value.length);
    }
}
