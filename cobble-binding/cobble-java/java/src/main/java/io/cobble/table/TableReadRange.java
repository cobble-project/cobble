package io.cobble.table;

/** Inclusive physical bucket range assigned to one scan cursor. */
public final class TableReadRange {
    private final int firstBucket;
    private final int lastBucket;

    public TableReadRange(int firstBucket, int lastBucket) {
        if (firstBucket < 0 || lastBucket < firstBucket) {
            throw new IllegalArgumentException("invalid table read bucket range");
        }
        this.firstBucket = firstBucket;
        this.lastBucket = lastBucket;
    }

    public int firstBucket() {
        return firstBucket;
    }

    public int lastBucket() {
        return lastBucket;
    }
}
